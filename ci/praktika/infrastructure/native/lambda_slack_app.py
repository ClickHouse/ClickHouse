import base64
import hashlib
import hmac
import json
import os
import time
import urllib.parse
import urllib.request

import boto3

SLACK_SIGNING_SECRET = os.environ["SIGN_SECRET"]
SLACK_BOT_TOKEN = os.environ.get("SLACK_BOT_TOKEN", "")


def _get_raw_body(event) -> str:
    body = event.get("body", "")
    if event.get("isBase64Encoded"):
        body = base64.b64decode(body).decode("utf-8")
    return body


def verify_slack_request(event) -> None:
    headers = event.get("headers") or {}

    # API Gateway may change header casing
    sig = headers.get("X-Slack-Signature") or headers.get("x-slack-signature")
    ts = headers.get("X-Slack-Request-Timestamp") or headers.get(
        "x-slack-request-timestamp"
    )

    if not sig or not ts:
        raise ValueError("Missing Slack signature headers")

    # Replay protection: reject if older than 5 minutes
    now = int(time.time())
    ts_int = int(ts)
    if abs(now - ts_int) > 60 * 5:
        raise ValueError("Stale Slack request timestamp")

    raw_body = _get_raw_body(event)

    basestring = f"v0:{ts}:{raw_body}".encode("utf-8")
    my_sig = (
        "v0="
        + hmac.new(
            SLACK_SIGNING_SECRET.encode("utf-8"), basestring, hashlib.sha256
        ).hexdigest()
    )

    # Constant-time compare
    if not hmac.compare_digest(my_sig, sig):
        raise ValueError("Invalid Slack signature")


def _json_response(payload: dict, status: int = 200):
    return {
        "statusCode": status,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps(payload),
    }


def post_to_response_url(response_url: str, message: dict) -> None:
    """
    Send a follow-up message using Slack's response_url.
    Can be ephemeral or in_channel depending on response_type.
    """
    data = json.dumps(message).encode("utf-8")
    req = urllib.request.Request(
        response_url,
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=3) as resp:
        resp.read()


def publish_home_view(
    user_id: str,
    username: str,
    github_login: str = "",
    footer: str = "",
) -> None:
    """
    Publish or update the Slack home tab view for a user.
    """
    view_data = {
        "user_id": user_id,
        "view": {
            "type": "home",
            "blocks": [],
        },
    }

    if github_login:
        message_text = f"The feed for user `{github_login}` will appear here."
        view_data["view"]["blocks"].append(
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": message_text,
                },
            }
        )

    # Add footer if provided
    if footer:
        view_data["view"]["blocks"].append({"type": "divider"})
        view_data["view"]["blocks"].append(
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": footer,
                },
            }
        )

    data = json.dumps(view_data).encode("utf-8")
    req = urllib.request.Request(
        "https://slack.com/api/views.publish",
        data=data,
        headers={
            "Authorization": f"Bearer {SLACK_BOT_TOKEN}",
            "Content-Type": "application/json",
        },
        method="POST",
    )

    try:
        with urllib.request.urlopen(req, timeout=3) as resp:
            result = json.loads(resp.read().decode("utf-8"))
            if result.get("ok"):
                print(f"Home view published successfully for user {user_id}")
            else:
                print(f"Error publishing home view: {result.get('error')}")
    except Exception as e:
        print(f"Failed to publish home view: {e}")


def handle_slash_command(form: dict):
    user_id = form.get("user_id", [""])[0]
    command = form.get("command", [""])[0]
    text = form.get("text", [""])[0].strip()
    username = form.get("user_name", [""])[0]
    response_url = form.get("response_url", [""])[0]

    # Parse command arguments
    args = text.split() if text else []

    # Help message (no arguments)
    if not args:
        return _json_response(
            {
                "response_type": "ephemeral",
                "blocks": [
                    {
                        "type": "section",
                        "text": {
                            "type": "mrkdwn",
                            "text": "Use one of the following commands to subscribe or unsubscribe from praktika feed:",
                        },
                    },
                    {
                        "type": "section",
                        "text": {
                            "type": "mrkdwn",
                            "text": (
                                "/praktika subscribe <gh_login>\n"
                                "/praktika unsubscribe"
                            ),
                        },
                    },
                ],
            }
        )

    action = args[0].lower()

    # Subscribe command
    if action == "subscribe":
        if len(args) < 2:
            return _json_response(
                {
                    "response_type": "ephemeral",
                    "text": "❌ Missing GitHub login. Usage: `/praktika subscribe <gh_login>`",
                }
            )

        github_login = args[1]

        # Publish home view immediately with subscription info
        publish_home_view(
            user_id=user_id,
            username=username,
            github_login=github_login,
            footer=f"_{github_login}_",
        )

        # Invoke worker Lambda for subscription processing
        invoke_worker_lambda(
            {
                "action": "subscribe",
                "user_id": user_id,
                "username": username,
                "github_login": github_login,
            }
        )

        return _json_response(
            {
                "response_type": "ephemeral",
                "text": f"✅ Subscribing to praktika feed for GitHub user: `{github_login}`",
            }
        )

    # Unsubscribe command
    elif action == "unsubscribe":
        # Publish home view immediately with subscribe instructions
        publish_home_view(
            user_id=user_id,
            username=username,
            footer="_To subscribe to feed, type:_\n`/praktika subscribe <gh_login>`",
        )

        # Invoke worker Lambda for unsubscription processing
        invoke_worker_lambda(
            {
                "action": "unsubscribe",
                "user_id": user_id,
                "username": username,
            }
        )

        return _json_response(
            {
                "response_type": "ephemeral",
                "text": "✅ Unsubscribing from praktika feed",
            }
        )

    # Unknown command
    else:
        return _json_response(
            {
                "response_type": "ephemeral",
                "text": f"❌ Unknown command: `{action}`. Type `/praktika` for help.",
            }
        )


def invoke_worker_lambda(payload: dict):
    """
    Invoke worker Lambda asynchronously for background processing.
    """
    lambda_client = boto3.client("lambda")
    worker_function_name = "praktika_slack_worker"

    try:
        lambda_client.invoke(
            FunctionName=worker_function_name,
            InvocationType="Event",  # Async invocation
            Payload=json.dumps(payload),
        )
        print(f"[ASYNC] Invoked worker Lambda: {worker_function_name}")
    except Exception as e:
        print(f"[ASYNC] Failed to invoke worker Lambda: {e}")


def handle_interactivity(payload: dict):
    """
    Interactivity requests contain:
      - payload.user.id
      - payload.actions[0].action_id
      - payload.response_url
    We ACK immediately (return 200), then invoke Lambda async for processing.
    """
    user_id = payload.get("user", {}).get("id", "")
    actions = payload.get("actions", [])
    action_id = actions[0].get("action_id") if actions else ""
    value = actions[0].get("value") if actions else ""
    response_url = payload.get("response_url")

    print("Interactivity user_id:", user_id)
    print("Interactivity action_id:", action_id, "value:", value)

    # Handle subscribe button click
    if action_id == "subscribe_button":
        # Extract GitHub login from input field
        view_state = payload.get("view", {}).get("state", {}).get("values", {})
        github_login_block = view_state.get("github_login_input", {})
        github_login_input = github_login_block.get("github_login", {})
        github_login = github_login_input.get("value", "")

        username = payload.get("user", {}).get("username", "")

        print(
            f"User {user_id} ({username}) requested subscribe for GitHub login: {github_login}"
        )

        # Immediately update home view to show processing state
        publish_home_view(user_id, username, github_login, file_size=-1)

        # Invoke worker Lambda for heavy processing
        invoke_worker_lambda(
            {
                "action": "subscribe",
                "user_id": user_id,
                "username": username,
                "github_login": github_login,
            }
        )

    if response_url:
        # Send a follow-up ephemeral message to the user who clicked
        post_to_response_url(
            response_url,
            {
                "response_type": "ephemeral",
                "text": f"👋 Processing your request, <@{user_id}>...",
            },
        )
    else:
        print("No response_url in payload; cannot send follow-up message.")

    # Important: Return immediately to avoid Slack timeout
    return {"statusCode": 200, "body": ""}


def lambda_handler(event, context):
    try:
        verify_slack_request(event)
    except Exception as e:
        print("Slack verification failed:", str(e))
        return {"statusCode": 401, "body": "unauthorized"}

    raw_body = _get_raw_body(event)

    # Slack sends slash commands and interactions as form-urlencoded
    form = urllib.parse.parse_qs(raw_body)

    # Interactivity: 'payload' contains JSON
    if "payload" in form:
        payload_json = form["payload"][0]
        payload = json.loads(payload_json)
        return handle_interactivity(payload)

    # Slash command: has 'command'
    if "command" in form:
        return handle_slash_command(form)

    return {"statusCode": 400, "body": "unknown slack request"}
