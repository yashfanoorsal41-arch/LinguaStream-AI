import json
import logging
import os
import uuid
import boto3
from ask_sdk_core.skill_builder import SkillBuilder
from ask_sdk_core.dispatch_components import AbstractRequestHandler, AbstractExceptionHandler
from ask_sdk_core.utils import is_request_type, is_intent_name, get_slot_value
from ask_sdk_model.interfaces.audioplayer import (
    PlayDirective, PlayBehavior, AudioItem, Stream
)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

translate_client = boto3.client("translate", region_name="us-east-1")
polly_client = boto3.client("polly", region_name="us-east-1")
s3_client = boto3.client("s3", region_name="us-east-1")
dynamodb_resource = boto3.resource("dynamodb", region_name="us-east-1")

AUDIO_BUCKET = os.environ.get("AUDIO_BUCKET")
DYNAMODB_TABLE_NAME = os.environ.get("DYNAMODB_TABLE", "LinguaStreamStateTable")
db_table = dynamodb_resource.Table(DYNAMODB_TABLE_NAME)

LANGUAGE_CONFIG = {
    "spanish": {"translate_code": "es", "voice_id": "Lucia", "polly_language_code": "es-ES", "char_speed_ms": 65},
    "hindi": {"translate_code": "hi", "voice_id": "Aditi", "polly_language_code": "hi-IN", "char_speed_ms": 80},
    "french": {"translate_code": "fr", "voice_id": "Lea", "polly_language_code": "fr-FR", "char_speed_ms": 70}
}

class LaunchRequestHandler(AbstractRequestHandler):
    def can_handle(self, handler_input):
        return is_request_type("LaunchRequest")(handler_input)

    def handle(self, handler_input):
        logger.info("Ecosystem initialized via user launch request.")
        speech_output = (
            "Welcome to LinguaStream AI Workspace. I can process your timestamped multimedia script timelines, "
            "evaluate overlapping sync risks via secure DynamoDB isolation, and stream output audio. "
            "Which target language track should we build today?"
        )
        return handler_input.response_builder.speak(speech_output).ask("Please request a voice profile language track.").response

class StreamLocalizationIntentHandler(AbstractRequestHandler):
    def can_handle(self, handler_input):
        return is_intent_name("StreamLocalizationIntent")(handler_input)

    def handle(self, handler_input):
        if not AUDIO_BUCKET:
            return handler_input.response_builder.speak("Configuration fault: AUDIO_BUCKET environment parameter missing.").response

        target_lang_input = get_slot_value(handler_input=handler_input, slot_name="language")
        if not target_lang_input:
            return handler_input.response_builder.speak("Please declare your target language parameter.").response

        language_key = target_lang_input.strip().lower()
        config = LANGUAGE_CONFIG.get(language_key)
        if config is None:
            return handler_input.response_builder.speak(f"The language channel {target_lang_input} is unsupported.").response

        input_timeline = {
            "source_language": "en",
            "segments": [
                {"id": "seg-001", "start_ms": 15000, "text": "Welcome back to our coding session."},
                {"id": "seg-002", "start_ms": 18000, "text": "Today we are deploying an automated multi-threaded framework."}
            ]
        }

        system_context = handler_input.request_envelope.context.System
        user_id = getattr(getattr(system_context, "user", None), "user_id", "anonymous_user")
        
        playlist_uuid = str(uuid.uuid4())
        processed_segments = []
        timing_conflicts = []
        successful_audio_queue = []

        for segment in input_timeline["segments"]:
            segment_status = "pending"
            translated_text = None
            audio_key = f"localized-audio/{playlist_uuid}-{segment['id']}-{config['translate_code']}.mp3"
            approx_duration_ms = 0

            try:
                translation_response = translate_client.translate_text(
                    Text=segment["text"], SourceLanguageCode=input_timeline["source_language"], TargetLanguageCode=config['translate_code']
                )
                translated_text = translation_response.get("TranslatedText")
                segment_status = "translated"
            except Exception as e:
                logger.error(f"Translation logic fault on {segment['id']}: {e}")
                segment_status = "translation_failed"

            if segment_status == "translated" and translated_text:
                try:
                    polly_response = polly_client.synthesize_speech(
                        Text=translated_text, OutputFormat="mp3", VoiceId=config["voice_id"], LanguageCode=config["polly_language_code"]
                    )
                    audio_bytes = polly_response["AudioStream"].read()
                    polly_response["AudioStream"].close()

                    if not audio_bytes:
                        segment_status = "audio_pipeline_failed"
                        logger.error(f"Zero bytes layout generated on asset {segment['id']}")
                    else:
                        s3_client.put_object(Bucket=AUDIO_BUCKET, Key=audio_key, Body=audio_bytes, ContentType="audio/mpeg")
                        segment_status = "success"
                        approx_duration_ms = len(translated_text) * config["char_speed_ms"]
                        
                        presigned_url = s3_client.generate_presigned_url(
                            "get_object", Params={"Bucket": AUDIO_BUCKET, "Key": audio_key}, ExpiresIn=3600
                        )
                        successful_audio_queue.append({"id": segment["id"], "url": presigned_url, "s3_key": audio_key})
                except Exception as e:
                    logger.error(f"Media streaming upload orchestration drop: {e}")
                    segment_status = "audio_pipeline_failed"

            processed_segments.append({
                "id": segment["id"], "start_ms": segment["start_ms"],
                "estimated_duration_ms": approx_duration_ms if segment_status == "success" else None,
                "status": segment_status, "localized_text": translated_text
            })

        ordered_segments = sorted(processed_segments, key=lambda item: item["start_ms"])
        for index, segment in enumerate(ordered_segments[:-1]):
            next_segment = ordered_segments[index + 1]
            if segment["status"] == "success" and segment["estimated_duration_ms"]:
                actual_end_ms = segment["start_ms"] + segment["estimated_duration_ms"]
                if actual_end_ms > next_segment["start_ms"]:
                    timing_conflicts.append({
                        "segment_id": segment["id"], "next_segment_id": next_segment["id"],
                        "estimated_overlap_ms": actual_end_ms - next_segment["start_ms"]
                    })

        success_count = len(successful_audio_queue)
        speech_output = (
            f"Orchestration complete. Successfully stabilized {success_count} structural blocks "
            f"for language track {target_lang_input}. Found {len(timing_conflicts)} estimated timeline overlaps."
        )
        response_builder = handler_input.response_builder.speak(speech_output)

        if success_count > 0:
            first_track = successful_audio_queue[0]
            remaining_tracks = successful_audio_queue[1:]
            initial_token = f"play_{playlist_uuid}_{first_track['id']}"

            try:
                db_table.put_item(Item={
                    "playlist_id": initial_token,
                    "user_id": user_id,
                    "playlist_uuid": playlist_uuid,
                    "current_token": first_track["id"],
                    "remaining_queue": remaining_tracks
                })
                logger.info(f"DynamoDB isolated session trace persisted: {initial_token}")
                
                response_builder.add_directive(
                    PlayDirective(
                        play_behavior=PlayBehavior.REPLACE_ALL,
                        audio_item=AudioItem(stream=Stream(token=initial_token, url=first_track["url"], offset_in_milliseconds=0))
                    )
                )
                response_builder.set_should_end_session(True)
            except Exception as e:
                logger.error(f"DynamoDB serialization failure: {e}")
                return handler_input.response_builder.speak("Ecosystem storage state serialization failure. Dropping execution requests.").response

        return response_builder.response

class PlaybackNearlyFinishedHandler(AbstractRequestHandler):
    def can_handle(self, handler_input):
        return is_request_type("AudioPlayer.PlaybackNearlyFinished")(handler_input)

    def handle(self, handler_input):
        logger.info("PlaybackNearlyFinished intercepted. Evaluating composite pipeline data streams.")
        response_builder = handler_input.response_builder
        
        system_context = handler_input.request_envelope.context.System
        user_id = getattr(getattr(system_context, "user", None), "user_id", "anonymous_user")
        current_playlist_token = handler_input.request_envelope.request.token

        try:
            db_response = db_table.get_item(Key={"playlist_id": current_playlist_token})
            playlist_state = db_response.get("Item")
        except Exception as e:
            logger.error(f"DynamoDB dynamic execution lookup dropped on index verification: {e}")
            return response_builder.response

        if playlist_state and playlist_state.get("remaining_queue"):
            remaining = playlist_state["remaining_queue"]
            next_track = remaining
            updated_queue = remaining[1:]
            
            token_parts = current_playlist_token.split("_", 2)
            if len(token_parts) != 3 or token_parts[0] != "play":
                logger.error("Invalid playlist token format intercepted.")
                return response_builder.response
                
Use code with caution.
playlist_uuid = token_parts[1]
next_unique_token = f"play_{playlist_uuid}_{next_track['id']}"
try:
db_table.put_item(
Item={
"playlist_id": next_unique_token,
"user_id": user_id,
"playlist_uuid": playlist_uuid,
"current_token": next_track["id"],
"remaining_queue": updated_queue
},
ConditionExpression="attribute_not_exists(playlist_id)"
)
except Exception as e:
logger.error(f"Atomic block prevented dynamic duplicate write: {e}")
return response_builder.response
next_url = s3_client.generate_presigned_url(
"get_object", Params={"Bucket": AUDIO_BUCKET, "Key": next_track["s3_key"]}, ExpiresIn=3600
)
logger.info(f"Enqueuing sequential track natively: {next_unique_token}")
response_builder.add_directive(
PlayDirective(
play_behavior=PlayBehavior.ENQUEUE,
audio_item=AudioItem(
stream=Stream(
token=next_unique_token, url=next_url,
offset_in_milliseconds=0, expected_previous_token=current_playlist_token
)
)
)
)
return response_builder.response
class SessionEndedRequestHandler(AbstractRequestHandler):
def can_handle(self, handler_input):
return is_request_type("SessionEndedRequest")(handler_input)
def handle(self, handler_input):
return handler_input.response_builder.response
class GlobalExceptionHandler(AbstractExceptionHandler):
def can_handle(self, handler_input, exception):
return True
def handle(self, handler_input, exception):
logger.error(f"Unhandled fault caught: {exception}", exc_info=True)
return handler_input.response_builder.speak("An execution pipeline layer error occurred. Please try again.").response
sb = SkillBuilder()
sb.add_request_handler(LaunchRequestHandler())
sb.add_request_handler(StreamLocalizationIntentHandler())
sb.add_request_handler(PlaybackNearlyFinishedHandler())
sb.add_request_handler(SessionEndedRequestHandler())
sb.add_exception_handler(GlobalExceptionHandler())
lambda_handler = sb.lambda_handler
