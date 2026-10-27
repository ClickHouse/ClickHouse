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


def get_user_email(user_id: str) -> str:
    """Fetch user's email from Slack API."""
    req = urllib.request.Request(
        f"https://slack.com/api/users.info?user={user_id}",
        headers={"Authorization": f"Bearer {SLACK_BOT_TOKEN}"},
        method="GET",
    )
    try:
        with urllib.request.urlopen(req, timeout=3) as resp:
            result = json.loads(resp.read().decode("utf-8"))
            if result.get("ok"):
                return result.get("user", {}).get("profile", {}).get("email", "")
            else:
                print(f"Error fetching user info: {result.get('error')}")
                return ""
    except Exception as e:
        print(f"Failed to fetch user email: {e}")
        return ""


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
                                "/praktika subscribe [email]\n" "/praktika unsubscribe"
                            ),
                        },
                    },
                ],
            }
        )

    action = args[0].lower()

    # Subscribe command
    if action == "subscribe":
        # Use provided email or let worker fetch it from Slack
        if len(args) >= 2:
            user_email = args[1]
        else:
            user_email = get_user_email(user_id)
            if not user_email:
                return _json_response(
                    {
                        "response_type": "ephemeral",
                        "text": "❌ Unable to retrieve your email from Slack. Please provide your email: `/praktika subscribe <email>`",
                    }
                )

        # Invoke worker Lambda for subscription processing
        invoke_worker_lambda(
            {
                "action": "subscribe",
                "user_id": user_id,
                "username": username,
                "github_login": user_email,
                "response_url": response_url,
            }
        )

        return _json_response(
            {
                "response_type": "ephemeral",
                "text": "✅ Subscribing to praktika feed...",
            }
        )

    # Unsubscribe command
    elif action == "unsubscribe":
        # Publish home view immediately with subscribe instructions
        publish_home_view(
            user_id=user_id,
            username=username,
            footer="_To subscribe to feed, type:_\n`/praktika subscribe [email]`",
        )

        # Invoke worker Lambda for unsubscription processing
        invoke_worker_lambda(
            {
                "action": "unsubscribe",
                "user_id": user_id,
                "username": username,
                "response_url": response_url,
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

    username = payload.get("user", {}).get("username", "") or payload.get(
        "user", {}
    ).get("name", "")

    toggle_action_ids = {
        "toggle_hide_merged_prs": "hide_merged_prs",
        "toggle_hide_merges": "hide_merges",
        "toggle_hide_secondary_prs": "hide_secondary_prs",
        "toggle_show_last_7d": "show_last_7d",
        "toggle_notify_on_complete": "notify_on_complete",
        "toggle_notify_on_failure": "notify_on_failure",
    }

    if action_id in toggle_action_ids:
        invoke_worker_lambda(
            {
                "action": "toggle_pref",
                "user_id": user_id,
                "username": username,
                "pref_key": toggle_action_ids[action_id],
                "response_url": response_url,
                "value": value,
            }
        )

        return {"statusCode": 200, "body": ""}

    # Handle subscribe button click
    if action_id == "subscribe_button":
        # Extract GitHub login from input field
        view_state = payload.get("view", {}).get("state", {}).get("values", {})
        github_login_block = view_state.get("github_login_input", {})
        github_login_input = github_login_block.get("github_login", {})
        github_login = github_login_input.get("value", "")

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
                "response_url": response_url,
            }
        )

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
