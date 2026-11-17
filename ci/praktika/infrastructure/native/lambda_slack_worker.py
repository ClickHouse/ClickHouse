import json
import os

from event import EventFeed, FeedSubscription

SLACK_BOT_TOKEN = os.environ.get("SLACK_BOT_TOKEN", "")

# Module-level cache for github_login -> user_ids mapping
# Reused across Lambda invocations within the same container
SUBSCRIPTION_CACHE = {}


def _default_prefs() -> dict:
    return {
        "hide_merged_prs": False,
        "hide_merges": False,
        "hide_secondary_prs": False,
        "show_last_7d": False,
        "notify_on_complete": False,
        "notify_on_failure": False,
        "last_notified_key": {},
    }


def _get_subscription_cached(user_email: str, s3_path: str):
    cached = SUBSCRIPTION_CACHE.get(user_email)
    if isinstance(cached, FeedSubscription):
        return cached

    # Backward compatibility: older cache entries stored just the list of user_ids
    if isinstance(cached, list):
        return FeedSubscription(
            user_ids=cached, user_email=user_email, subscribed_at=""
        )

    if not s3_path:
        return FeedSubscription(user_ids=[], user_email=user_email, subscribed_at="")

    subscription = FeedSubscription.get_subscription(user_email, s3_path=s3_path)
    SUBSCRIPTION_CACHE[user_email] = subscription
    return subscription


def load_user_prefs(user_id: str, user_email: str, s3_path: str) -> dict:
    if not user_id or not user_email or not s3_path:
        return _default_prefs()

    subscription = _get_subscription_cached(user_email, s3_path=s3_path)
    user_prefs = (
        subscription.user_prefs if isinstance(subscription.user_prefs, dict) else {}
    )
    prefs = user_prefs.get(user_id, {}) if isinstance(user_prefs, dict) else {}
    merged = _default_prefs()
    if isinstance(prefs, dict):
        merged.update({k: prefs.get(k, v) for k, v in merged.items()})
    return merged


def save_user_prefs(user_id: str, user_email: str, prefs: dict, s3_path: str) -> None:
    if not user_id or not user_email or not s3_path:
        return

    subscription = _get_subscription_cached(user_email, s3_path=s3_path)
    if not isinstance(subscription.user_prefs, dict):
        subscription.user_prefs = {}
    subscription.user_prefs[user_id] = prefs
    subscription.to_s3(s3_path)
    SUBSCRIPTION_CACHE[user_email] = subscription


def toggle_pref(user_id: str, user_email: str, pref_key: str, s3_path: str) -> dict:
    prefs = load_user_prefs(user_id, user_email, s3_path)
    if pref_key not in prefs:
        return prefs
    if not isinstance(prefs.get(pref_key), bool):
        return prefs
    prefs[pref_key] = not prefs[pref_key]
    save_user_prefs(user_id, user_email, prefs, s3_path)
    return prefs


def get_user_email(user_id: str) -> str:
    """
    Fetch user email from Slack API using user ID.
    """
    import urllib.error
    import urllib.request

    if not SLACK_BOT_TOKEN:
        print("Error: SLACK_BOT_TOKEN not configured")
        return ""

    try:
        req = urllib.request.Request(
            f"https://slack.com/api/users.info?user={user_id}",
            headers={"Authorization": f"Bearer {SLACK_BOT_TOKEN}"},
        )
        with urllib.request.urlopen(req, timeout=10) as response:
            data = json.loads(response.read())
            if data.get("ok"):
                return data.get("user", {}).get("profile", {}).get("email", "")
            else:
                print(f"Slack API error: {data.get('error')}")
                return ""
    except Exception as e:
        print(f"Failed to get user email: {e}")
        return ""


def post_to_response_url(response_url: str, payload: dict) -> None:
    """
    Post a follow-up message to Slack via response_url.
    """
    import urllib.error
    import urllib.request

    if not response_url:
        print("Warning: No response_url provided, skipping follow-up message")
        return

    try:
        data = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(
            response_url,
            data=data,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=10) as response:
            result = response.read()
            print(f"Posted to response_url: {result.decode('utf-8')}")
    except urllib.error.HTTPError as e:
        print(f"Failed to post to response_url: {e.code} - {e.read().decode('utf-8')}")
    except Exception as e:
        print(f"Failed to post to response_url: {e}")


def _open_dm_channel(user_id: str) -> str:
    import urllib.request

    if not SLACK_BOT_TOKEN or not user_id:
        return ""

    body = json.dumps({"users": user_id}).encode("utf-8")
    req = urllib.request.Request(
        "https://slack.com/api/conversations.open",
        data=body,
        headers={
            "Authorization": f"Bearer {SLACK_BOT_TOKEN}",
            "Content-Type": "application/json",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as response:
            data = json.loads(response.read())
            if not data.get("ok"):
                print(f"conversations.open failed: {data}")
                return ""
            return data.get("channel", {}).get("id", "")
    except Exception as e:
        print(f"conversations.open error: {e}")
        return ""


def _post_dm(user_id: str, user_email: str, s3_path: str, text: str) -> None:
    import urllib.request

    prefs = (
        load_user_prefs(user_id, user_email, s3_path) if user_email and s3_path else {}
    )
    channel_id = prefs.get("dm_channel_id") if isinstance(prefs, dict) else ""

    if not channel_id:
        channel_id = _open_dm_channel(user_id)
        if channel_id and isinstance(prefs, dict) and user_email and s3_path:
            prefs["dm_channel_id"] = channel_id
            save_user_prefs(
                user_id=user_id, user_email=user_email, prefs=prefs, s3_path=s3_path
            )

    if not channel_id:
        return

    body = json.dumps({"channel": channel_id, "text": text}).encode("utf-8")
    req = urllib.request.Request(
        "https://slack.com/api/chat.postMessage",
        data=body,
        headers={
            "Authorization": f"Bearer {SLACK_BOT_TOKEN}",
            "Content-Type": "application/json",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as response:
            data = json.loads(response.read())
            if not data.get("ok"):
                print(f"chat.postMessage failed: {data}")
                # If cached channel is invalid, refresh it once
                if isinstance(prefs, dict) and prefs.get("dm_channel_id") == channel_id:
                    new_channel_id = _open_dm_channel(user_id)
                    if (
                        new_channel_id
                        and new_channel_id != channel_id
                        and user_email
                        and s3_path
                    ):
                        prefs["dm_channel_id"] = new_channel_id
                        save_user_prefs(
                            user_id=user_id,
                            user_email=user_email,
                            prefs=prefs,
                            s3_path=s3_path,
                        )
    except Exception as e:
        print(f"chat.postMessage error: {e}")


def format_event_text(event, pr_status, indent="", include_related_prs: bool = True):
    """Format event text with optional indentation for linked events."""
    # PR status emoji
    pr_status_emoji = ":pr_open:" if pr_status == "open" else ":pr_merged:"

    # Get change URL from event.ext
    change_url = event.ext.get("change_url", "")
    is_cancelled = bool(event.ext.get("is_cancelled", False))

    # Check if any job has failed or errored
    has_failures = False
    if hasattr(event, "result") and event.result and event.result.get("results"):
        has_failures = any(
            r.get("status") in ["failure", "error"]
            for r in event.result.get("results", [])
        )

    # CI status emoji based on event.ci_status
    if event.ci_status in ["pending", "running"]:
        ci_running_status_emoji = ":job_running:"
    else:
        if not is_cancelled:
            ci_running_status_emoji = ":checkered_flag:"
        else:
            ci_running_status_emoji = ":job_cancelled:"

    if event.ci_status == "success":
        ci_status_emoji = ":success_sign:"
    elif event.ci_status in ["pending", "running"]:
        ci_status_emoji = ":failure_sign:" if has_failures else ":job_running:"
    else:
        ci_status_emoji = ":failure_sign:"

    # Get report URL from result.ext if available
    report_url = ""
    if hasattr(event, "result") and event.result:
        report_url = event.result.get("ext", {}).get("report_url", "")

    if report_url:
        report_url_text = f"<{report_url}|*report*>"
    else:
        report_url_text = ""

    # Format identifier (sha or PR number) with link if available
    event_pr_number = event.ext.get("pr_number", 0)
    if event_pr_number > 0:
        identifier = f"#{event_pr_number}"
        title = event.ext.get("pr_title", "")
    else:
        identifier = event.sha[:8] if event.sha else "commit"
        title = (
            event.ext.get("commit_message", "").split("\n")[0]
            if event.ext.get("commit_message")
            else event.ext.get("pr_title", "")
        )

    identifier_text = f"<{change_url}|{identifier}>" if change_url else identifier
    workflow_name = event.ext.get("workflow_name", "")
    repo_name = event.ext.get("repo_name", "")
    workflow_suffix = ""
    if workflow_name:
        workflow_suffix = f" [{workflow_name}]"
    if repo_name:
        workflow_suffix += f"[{repo_name}]"

    event_text = f"{indent}{pr_status_emoji} {ci_running_status_emoji} {ci_status_emoji} {identifier_text} *{title}*{workflow_suffix}"

    # Aggregate results by status from workflow result
    if hasattr(event, "result") and event.result and event.result.get("results"):
        fail_cnt = 0
        error_cnt = 0
        dropped_cnt = 0
        success_cnt = 0
        skipped_cnt = 0
        running_cnt = 0
        total_cnt = 0

        for r in event.result.get("results", []):
            status = r.get("status", "")
            total_cnt += 1

            if status == "failure":
                fail_cnt += 1
            elif status == "error":
                error_cnt += 1
            elif status == "dropped":
                dropped_cnt += 1
            elif status == "success":
                success_cnt += 1
            elif status == "skipped":
                skipped_cnt += 1
            elif status in ["pending", "running"]:
                running_cnt += 1

        # Build compact one-line summary
        parts = []
        if fail_cnt or error_cnt:
            parts.append(f"{fail_cnt + error_cnt} failed")
        if dropped_cnt:
            parts.append(f"{dropped_cnt} dropped")
        if running_cnt:
            parts.append(f"{running_cnt} running")
        if success_cnt:
            parts.append(f"{success_cnt} ok")
        if skipped_cnt:
            parts.append(f"{skipped_cnt} skipped")

        if parts:
            summary = " · ".join(parts)
            if report_url_text:
                event_text += f"\n{indent}{summary} · {report_url_text}"
            else:
                event_text += f"\n{indent}{summary}"
        elif report_url_text:
            event_text += f"\n{indent}{report_url_text}"

        # Add related PRs if available (only for parent events)
        if include_related_prs and not indent:
            related_prs = event.ext.get("related_prs", [])
            if related_prs:
                event_text += "\n\n*Related PRs:*"
                for pr_num in related_prs:
                    pr_info = {}
                    if hasattr(event, "result") and event.result:
                        result_ext = event.result.get("ext", {})
                        related_pr_info = result_ext.get("related_pr_info", {})
                        pr_info = related_pr_info.get(pr_num, {})

                    pr_title = pr_info.get("pr_title", "")
                    pr_change_url = pr_info.get("change_url", "")
                    pr_report_url = pr_info.get("report_url", "")

                    if pr_change_url:
                        pr_link_text = f"<{pr_change_url}|#{pr_num}>"
                    else:
                        pr_link_text = f"#{pr_num}"

                    pr_line = f"\n  • {pr_link_text}"
                    if pr_title:
                        pr_line += f" - {pr_title}"
                    if pr_report_url:
                        pr_line += f" (<{pr_report_url}|report>)"

                    event_text += pr_line

    return event_text


def _format_notification_text(event, notify_type: str) -> str:
    note = f"_(notification: {notify_type})_"

    ext = getattr(event, "ext", {}) or {}
    pr_status = (ext.get("pr_status") or "").lower()

    base = format_event_text(event, pr_status, indent="", include_related_prs=False)

    failed_names = []
    result = getattr(event, "result", None)
    if isinstance(result, dict):
        results = result.get("results", []) or []
    elif result is not None and hasattr(result, "results"):
        results = getattr(result, "results") or []
    else:
        results = []

    for r in results:
        if not isinstance(r, dict):
            continue
        status = (r.get("status") or "").lower()
        if status not in ("failure", "error"):
            continue
        name = r.get("name") or ""
        if name:
            failed_names.append(name)

    extra = ""
    if failed_names:
        extra = "\n_Failed jobs:_ " + ", ".join(f"`{n}`" for n in failed_names)

    return f"{base}\n{note}{extra}"


def load_event_timeline(github_login: str):
    """
    Load EventFeed from S3.

    Args:
        github_login: GitHub login to construct S3 key

    Returns:
        EventFeed object
    """
    if not github_login:
        return EventFeed()

    s3_path = os.environ.get("EVENT_FEED_S3_PATH", "")
    assert s3_path

    try:
        # Load EventFeed from S3
        timeline = EventFeed.from_s3(github_login, s3_path=s3_path)
        print(f"Loaded EventFeed with {len(timeline.events)} events")
        return timeline
    except Exception as e:
        print(f"Error loading EventFeed: {e}")
        return EventFeed()


def publish_home_view(
    user_id: str,
    username: str,
    github_login: str = "",
    events: list = None,
) -> None:
    """
    Publish or update the Slack home tab view for a user.
    """
    import urllib.request

    events = events or []

    subscriptions_s3_path = os.environ.get("EVENT_FEED_S3_PATH", "")
    prefs = load_user_prefs(user_id, github_login, subscriptions_s3_path)

    def _is_merge_result(event) -> bool:
        ext = getattr(event, "ext", {}) or {}
        branch = (ext.get("branch") or "").lower()
        pr_number = ext.get("pr_number", 0) or 0
        if pr_number:
            return False
        if branch in ("master", "main"):
            return True
        if branch.startswith("release"):
            return True
        return False

    import time

    merge_result_parent_keys = set()
    for e in events:
        ext = getattr(e, "ext", {}) or {}
        e_pr_number = ext.get("pr_number", 0) or 0
        if e_pr_number != 0:
            continue
        e_parent_pr_number = ext.get("parent_pr_number", 0) or 0
        if not e_parent_pr_number:
            continue
        e_repo_name = ext.get("repo_name", "") or ""
        if not e_repo_name:
            continue
        merge_result_parent_keys.add((e_repo_name, e_parent_pr_number))

    filtered_events = []
    for e in events:
        ext = getattr(e, "ext", {}) or {}
        pr_status = (ext.get("pr_status") or "").lower()
        pr_number = ext.get("pr_number", 0) or 0
        parent_pr_number = ext.get("parent_pr_number", 0) or 0
        repo_name = ext.get("repo_name", "") or ""

        # Workaround (backward compat): older EventFeed entries may miss ext.pr_status.
        # Populate it here so the rest of the function can rely on it.
        if not pr_status:
            if pr_number == 0:
                pr_status = "merged"
            elif (
                pr_number > 0
                and repo_name
                and (repo_name, pr_number) in merge_result_parent_keys
            ):
                pr_status = "merged"
            elif pr_number > 0:
                pr_status = "open"

        if pr_status:
            ext["pr_status"] = pr_status

        if prefs.get("show_last_7d"):
            ts = getattr(e, "timestamp", 0) or 0
            if ts and ts < int(time.time()) - 7 * 24 * 60 * 60:
                continue

        if prefs.get("hide_merged_prs") and pr_number > 0 and pr_status == "merged":
            continue
        if prefs.get("hide_merges") and _is_merge_result(e):
            continue
        if prefs.get("hide_secondary_prs") and parent_pr_number > 0 and pr_number > 0:
            continue

        filtered_events.append(e)

    events = filtered_events

    blocks = []

    def _btn(label: str, action_id: str) -> dict:
        return {
            "type": "button",
            "text": {"type": "plain_text", "text": label, "emoji": True},
            "action_id": action_id,
            "value": "toggle",
        }

    blocks.append(
        {
            "type": "actions",
            "elements": [
                _btn(
                    f"Merged PRs: {'On' if not prefs.get('hide_merged_prs') else 'Off'}",
                    "toggle_hide_merged_prs",
                ),
                _btn(
                    f"Merges: {'On' if not prefs.get('hide_merges') else 'Off'}",
                    "toggle_hide_merges",
                ),
                _btn(
                    f"Auxilary PRs: {'On' if not prefs.get('hide_secondary_prs') else 'Off'}",
                    "toggle_hide_secondary_prs",
                ),
                _btn(
                    f"< 7d: {'On' if prefs.get('show_last_7d') else 'Off'}",
                    "toggle_show_last_7d",
                ),
                _btn(
                    f"Notify complete: {'On' if prefs.get('notify_on_complete') else 'Off'}",
                    "toggle_notify_on_complete",
                ),
                _btn(
                    f"Notify failure: {'On' if prefs.get('notify_on_failure') else 'Off'}",
                    "toggle_notify_on_failure",
                ),
            ],
        }
    )

    # Build nested structure by parent_pr_number
    # 1. Build map of pr_number -> event for quick lookup
    # 2. Find ultimate root parent for each event (flatten hierarchy)
    # 3. Group children by their ultimate root parent
    # 4. Parents are events with no parent (parent_pr_number=0 or not in list)

    # Build lookup map
    pr_to_event = {}
    for event in events:
        pr_number = event.ext.get("pr_number", 0)
        if pr_number > 0:
            pr_to_event[pr_number] = event

    # Helper function to find ultimate root parent for an event
    def find_root_parent(event):
        """Trace back through parent chain to find ultimate root parent."""
        visited = set()
        current = event

        while True:
            pr_number = current.ext.get("pr_number", 0)
            parent_pr_number = current.ext.get("parent_pr_number", 0)

            # Avoid infinite loops
            if pr_number in visited:
                return current
            visited.add(pr_number)

            # If no parent or parent not in list, this is the root
            if parent_pr_number == 0 or parent_pr_number not in pr_to_event:
                return current

            # Move up to parent
            current = pr_to_event[parent_pr_number]

    # Group events by their ultimate root parent
    parent_events = []
    parent_pr_numbers = set()
    children_by_parent = {}  # root_pr_number -> list of child events

    for event in events:
        root_parent = find_root_parent(event)
        root_pr_number = root_parent.ext.get("pr_number", 0)

        if event is root_parent:
            # This event is a root parent itself
            if root_pr_number == 0:
                parent_events.append(event)
            elif root_pr_number > 0 and root_pr_number not in parent_pr_numbers:
                parent_events.append(event)
                parent_pr_numbers.add(root_pr_number)
        else:
            # This event is a child of a root parent
            if root_pr_number not in children_by_parent:
                children_by_parent[root_pr_number] = []
            children_by_parent[root_pr_number].append(event)

    root_events = parent_events

    # Add events list if available (events are already sorted newest first)
    if root_events:
        for root_event in root_events:
            pr_number = root_event.ext.get("pr_number", 0)

            # Determine PR status based on children
            # If any child has pr_number = 0, the PR is merged
            children = children_by_parent.get(pr_number, [])
            ext = getattr(root_event, "ext", {}) or {}
            pr_status = (ext.get("pr_status") or "").lower()
            for child in children:
                if child.ext.get("pr_number", 0) == 0:
                    pr_status = "merged"
                    break

            # Format parent event
            event_text = format_event_text(root_event, pr_status, indent="")

            # Add parent event section
            blocks.append(
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": event_text,
                    },
                }
            )

            # Add child events with indentation (sorted by timestamp, newest first)
            if children:
                children_sorted = sorted(
                    children, key=lambda e: e.timestamp, reverse=True
                )
                for child_event in children_sorted:
                    child_text = format_event_text(child_event, pr_status, indent="> ")
                    blocks.append(
                        {
                            "type": "section",
                            "text": {
                                "type": "mrkdwn",
                                "text": child_text,
                            },
                        }
                    )

            # Add divider after the event group
            blocks.append({"type": "divider"})

    # Add footer with divider
    if github_login:
        blocks.append(
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"_{github_login}_",
                },
            }
        )

    # Slack home views are limited to 100 blocks total.
    # Truncate before the limit, leaving room for a notice block.
    SLACK_BLOCK_LIMIT = 100
    if len(blocks) > SLACK_BLOCK_LIMIT:
        blocks = blocks[: SLACK_BLOCK_LIMIT - 1]
        blocks.append(
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": "_Some events were truncated (view limit reached)._",
                },
            }
        )

    # Slack section text is capped at 3000 characters.
    for block in blocks:
        if block.get("type") == "section":
            txt = block.get("text", {})
            if isinstance(txt, dict) and len(txt.get("text", "")) > 3000:
                txt["text"] = txt["text"][:2997] + "…"

    view_data = {
        "user_id": user_id,
        "view": {
            "type": "home",
            "blocks": blocks,
        },
    }

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


def lambda_handler(event, context):
    """
    Worker Lambda that processes subscribe and update requests:
    - subscribe: Saves subscription details, loads events, publishes home view
    - update: Loads events and publishes home view (no subscription save)

    Note: 'github_login' parameter now contains user email addresses for email-based subscriptions.
    """
    # Clear the per-container cache at the start of every invocation so that
    # preference changes saved to S3 by a previous invocation (e.g. toggle_pref)
    # are always reflected in subsequent invocations running in the same container.
    SUBSCRIPTION_CACHE.clear()

    print(f"Worker Lambda invoked with event: {json.dumps(event)}")

    action = event.get("action", "")
    user_id = event.get("user_id", "")
    username = event.get("username", "")
    user_email = event.get("github_login", "")  # Contains email address
    response_url = event.get("response_url", "")
    pref_key = event.get("pref_key", "")

    if not action:
        print("Error: Missing action parameter")
        return {"statusCode": 400, "body": "Missing action"}

    print(f"Processing {action} for user {user_id} ({username}), Email: {user_email}")

    subscriptions_s3_path = os.environ.get("EVENT_FEED_S3_PATH", "")

    if action == "toggle_pref":
        try:
            if not user_id:
                raise ValueError("Missing user_id")

            if not pref_key:
                raise ValueError("Missing pref_key")

            target_email = FeedSubscription.find_user_subscription(
                user_id, s3_path=subscriptions_s3_path
            )
            if not target_email:
                target_email = user_email

            if not target_email:
                raise ValueError("No subscription found for user")

            prefs = toggle_pref(
                user_id=user_id,
                user_email=target_email,
                pref_key=pref_key,
                s3_path=subscriptions_s3_path,
            )
            print(f"Toggled pref {pref_key} for {user_id}: {prefs.get(pref_key)}")

            if target_email:
                timeline = load_event_timeline(target_email)
                publish_home_view(user_id, target_email, target_email, timeline.events)

            if response_url:
                post_to_response_url(
                    response_url,
                    {
                        "response_type": "ephemeral",
                        "text": "✅ Updated settings",
                    },
                )
        except Exception as e:
            print(f"Error processing toggle_pref: {e}")
            if response_url:
                post_to_response_url(
                    response_url,
                    {
                        "response_type": "ephemeral",
                        "text": "❌ Failed to update settings. Please try again.",
                    },
                )
            return {"statusCode": 500, "body": "Error updating settings"}

        return {"statusCode": 200, "body": "success"}

    if action == "subscribe":
        try:
            if not user_id:
                print("Error: Missing user_id for subscribe action")
                raise ValueError("Missing user_id")

            if not user_email:
                user_email = get_user_email(user_id)
                if not user_email:
                    if response_url:
                        post_to_response_url(
                            response_url,
                            {
                                "response_type": "ephemeral",
                                "text": "❌ Unable to retrieve your email from Slack. Please provide it: `/praktika subscribe <email>`",
                            },
                        )
                    return {"statusCode": 200, "body": "Missing user email"}

            # Load EventFeed from S3 using email
            timeline = load_event_timeline(user_email)

            # Add user_id to subscription list (supports multiple Slack users per email)
            if subscriptions_s3_path:
                subscription = FeedSubscription.add_user_id(
                    user_email=user_email,
                    user_id=user_id,
                    s3_path=subscriptions_s3_path,
                )
                # Update cache with fresh subscription data
                SUBSCRIPTION_CACHE[user_email] = subscription
                print(f"Added subscription for user {user_id} to {user_email}")
            else:
                print(
                    "Warning: EVENT_FEED_S3_PATH not configured, skipping subscription save"
                )

            # Publish updated home view
            publish_home_view(user_id, username, user_email, timeline.events)

            if response_url:
                post_to_response_url(
                    response_url,
                    {
                        "response_type": "ephemeral",
                        "text": f"✅ Subscribed to praktika feed for: `{user_email}`",
                    },
                )
        except Exception as e:
            print(f"Error processing subscribe: {e}")
            if response_url:
                post_to_response_url(
                    response_url,
                    {
                        "response_type": "ephemeral",
                        "text": "❌ Failed to subscribe to praktika feed. Please try again.",
                    },
                )
            return {"statusCode": 500, "body": "Error subscribing"}

    elif action == "unsubscribe":
        try:
            if not user_id:
                print("Error: Missing user_id for unsubscribe action")
                raise ValueError("Missing user_id")

            subscriptions_s3_path = os.environ.get("EVENT_FEED_S3_PATH", "")
            if not subscriptions_s3_path:
                print("Warning: EVENT_FEED_S3_PATH not configured, cannot unsubscribe")
                raise ValueError("Configuration error")

            # If user_email not provided, find it
            target_email = user_email
            if not target_email:
                target_email = FeedSubscription.find_user_subscription(
                    user_id=user_id,
                    s3_path=subscriptions_s3_path,
                )
                if not target_email:
                    print(f"No subscription found for user {user_id}")
                    if response_url:
                        post_to_response_url(
                            response_url,
                            {
                                "response_type": "ephemeral",
                                "text": "ℹ️ No active subscription found.",
                            },
                        )
                    return {"statusCode": 404, "body": "No subscription found"}
                print(f"Found subscription for user {user_id}: {target_email}")

            # Remove user from subscription
            subscription = FeedSubscription.remove_user_id(
                user_email=target_email,
                user_id=user_id,
                s3_path=subscriptions_s3_path,
            )
            # Update cache with new subscription data
            SUBSCRIPTION_CACHE[target_email] = subscription
            print(f"Removed user {user_id} from subscription to {target_email}")

            if response_url:
                post_to_response_url(
                    response_url,
                    {
                        "response_type": "ephemeral",
                        "text": "✅ Unsubscribed from praktika feed",
                    },
                )
        except Exception as e:
            print(f"Error processing unsubscribe: {e}")
            if response_url:
                post_to_response_url(
                    response_url,
                    {
                        "response_type": "ephemeral",
                        "text": "❌ Failed to unsubscribe from praktika feed. Please try again.",
                    },
                )
            return {"statusCode": 500, "body": "Error unsubscribing"}

    elif action == "update":
        # Get list of emails to update (supports both single username and list of emails)
        emails = event.get("emails", [])
        if not emails and username:
            # Backward compatibility: support single username parameter
            emails = [username]

        if not emails:
            print("Error: Missing emails for update action")
            return {"statusCode": 400, "body": "Missing emails"}

        print(f"Processing update for {len(emails)} email(s)")

        # Process each email
        for email in emails:
            print(f"Processing update for {email}")

            # Get subscribed user_ids (use cache if available)
            subscription = _get_subscription_cached(email, subscriptions_s3_path)
            subscribed_user_ids = subscription.user_ids if subscription else []
            if subscribed_user_ids:
                print(f"Using cached subscription for {email}")
            else:
                print(f"No subscriptions found for {email}, skipping notifications")
                continue

            if not subscribed_user_ids:
                print(f"No subscriptions found for {email}, skipping notifications")
                continue

            # Load EventFeed from S3 using email
            timeline = load_event_timeline(email)

            newest_event = timeline.events[0] if timeline.events else None

            newest_has_failures = False
            if (
                newest_event
                and hasattr(newest_event, "result")
                and newest_event.result
                and newest_event.result.get("results")
            ):
                newest_has_failures = any(
                    r.get("status") in ["failure", "error"]
                    for r in newest_event.result.get("results", [])
                )

            # Publish updated home view to all subscribed users
            for subscribed_user_id in subscribed_user_ids:
                try:
                    publish_home_view(subscribed_user_id, email, email, timeline.events)
                    print(f"Published home view for user {subscribed_user_id}")

                    prefs = load_user_prefs(
                        subscribed_user_id, email, subscriptions_s3_path
                    )

                    if newest_event:
                        last_notified = prefs.get("last_notified_key")
                        # Backward compatibility: previously stored as a single string
                        if not isinstance(last_notified, dict):
                            last_notified = {}
                        prefs["last_notified_key"] = last_notified

                        ext = getattr(newest_event, "ext", {}) or {}
                        pr_number = ext.get("pr_number", 0) or 0
                        repo_name = ext.get("repo_name", "")
                        pr_title = ext.get("pr_title", "")
                        sha = getattr(newest_event, "sha", "")
                        ci_status = (
                            getattr(newest_event, "ci_status", "") or ""
                        ).lower()

                        should_notify_complete = getattr(
                            newest_event, "type", ""
                        ) == "completed" and prefs.get("notify_on_complete")

                        should_notify_failure = (
                            not should_notify_complete
                            and newest_has_failures
                            and prefs.get("notify_on_failure")
                        )

                        for notify_type in ("failure", "complete"):
                            if notify_type == "failure" and not should_notify_failure:
                                continue
                            if notify_type == "complete" and not should_notify_complete:
                                continue

                            dedupe_key = f"{notify_type}:{email}:{sha}:{ci_status}:{pr_number}:{repo_name}"
                            if last_notified.get(notify_type) == dedupe_key:
                                continue

                            last_notified[notify_type] = dedupe_key
                            save_user_prefs(
                                user_id=subscribed_user_id,
                                user_email=email,
                                prefs=prefs,
                                s3_path=subscriptions_s3_path,
                            )

                            msg = _format_notification_text(newest_event, notify_type)
                            _post_dm(
                                user_id=subscribed_user_id,
                                user_email=email,
                                s3_path=subscriptions_s3_path,
                                text=msg,
                            )
                except Exception as e:
                    print(f"Error publishing to user {subscribed_user_id}: {e}")

    else:
        print(f"Error: Unknown action '{action}'")
        return {"statusCode": 400, "body": f"Unknown action: {action}"}

    print(f"Completed processing {action} for user {user_id}")

    return {"statusCode": 200, "body": "success"}
