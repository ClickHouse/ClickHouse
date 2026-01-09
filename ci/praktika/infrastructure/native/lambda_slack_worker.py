import json
import os
from datetime import datetime, timezone

from event import EventFeed, FeedSubscription

SLACK_BOT_TOKEN = os.environ.get("SLACK_BOT_TOKEN", "")

# Module-level cache for github_login -> user_ids mapping
# Reused across Lambda invocations within the same container
SUBSCRIPTION_CACHE = {}


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

    s3_path = os.environ.get("EVENTS_S3_PATH", "")
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

    blocks = []

    # Add events list if available
    if events:
        for event in reversed(events[-10:]):  # Show last 10 events, most recent first
            # PR status emoji
            pr_status_emoji = (
                ":pr_open:" if event.pr_status == "open" else ":pr_merged:"
            )

            # Get PR link from event.result.links[0] if available
            try:
                if hasattr(event, "result") and event.result:
                    links = event.result.get("links", [])
                    if links and len(links) > 0:
                        link = links[0]
                        if "pull" in link:
                            pr_link = link
            except Exception:
                pr_link = ""

            # CI status emoji based on event.ci_status
            if event.ci_status in ["pending", "running"]:
                ci_running_status_emoji = ":frog-run:"
            else:
                ci_running_status_emoji = ":checkered_flag:"

            if event.ci_status == "success":
                ci_status_emoji = ":yay-frog:"
            elif event.ci_status in ["pending", "running"]:
                ci_status_emoji = ":frog-run:"
            else:
                ci_status_emoji = ":worry-stop:"

            # Get report URL from result.ext if available
            report_url = ""
            if hasattr(event, "result") and event.result:
                report_url = event.result.get("ext", {}).get("report_url", "")

            if report_url:
                report_url_text = f"<{report_url}|*report*>"
            else:
                report_url_text = f""

            event_text = f"{pr_status_emoji} {ci_running_status_emoji} {ci_status_emoji} <{pr_link}|#{event.pr_number}> *{event.pr_title}*"

            # Aggregate results by status from workflow result
            if (
                hasattr(event, "result")
                and event.result
                and event.result.get("results")
            ):
                failure_jobs = []
                error_jobs = []
                dropped_jobs = []
                success_jobs = []
                skipped_jobs = []
                pending_running_jobs = []

                for r in event.result.get("results", []):
                    status = r.get("status", "")
                    name = r.get("name", "")

                    if status == "failure":
                        failure_jobs.append(name)
                    elif status == "error":
                        error_jobs.append(name)
                    elif status == "dropped":
                        dropped_jobs.append(name)
                    elif status == "success":
                        success_jobs.append(name)
                    elif status == "skipped":
                        skipped_jobs.append(name)
                    elif status in ["pending", "running"]:
                        pending_running_jobs.append(name)

                # Build summary text
                summary_lines = []

                # Combine failures and errors
                failed_or_errored = failure_jobs + error_jobs
                if failed_or_errored:
                    summary_lines.append(
                        f"\n{len(failed_or_errored)} job(s) finished with failure or error. First 5:"
                    )
                    for job_name in failed_or_errored[:5]:
                        summary_lines.append(f"  • {job_name}")

                if dropped_jobs:
                    summary_lines.append(f"{len(dropped_jobs)} job(s) dropped")

                if pending_running_jobs:
                    summary_lines.append(
                        f"{len(pending_running_jobs)} job(s) pending or running"
                    )

                if success_jobs:
                    summary_lines.append(
                        f"{len(success_jobs)} job(s) finished successfully"
                    )

                if skipped_jobs:
                    summary_lines.append(f"{len(skipped_jobs)} job(s) skipped")

                if summary_lines:
                    event_text += "\n" + "\n".join(summary_lines)

                if report_url_text:
                    event_text += "\n" + report_url_text

            # Create individual section block for each event (without image accessory)
            blocks.append(
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": event_text,
                    },
                }
            )

    # Add footer with divider
    if github_login:
        blocks.append({"type": "divider"})
        blocks.append(
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"_{github_login}_",
                },
            }
        )

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
    """
    print(f"Worker Lambda invoked with event: {json.dumps(event)}")

    action = event.get("action", "")
    user_id = event.get("user_id", "")
    username = event.get("username", "")
    github_login = event.get("github_login", "")

    if not action:
        print("Error: Missing action parameter")
        return {"statusCode": 400, "body": "Missing action"}

    print(
        f"Processing {action} for user {user_id} ({username}), GitHub: {github_login}"
    )

    if action == "subscribe":
        if not user_id:
            print("Error: Missing user_id for subscribe action")
            return {"statusCode": 400, "body": "Missing user_id"}

        if not github_login:
            print("Error: Missing github_login for subscribe action")
            return {"statusCode": 400, "body": "Missing github_login"}

        # Load EventFeed from S3
        timeline = load_event_timeline(github_login)

        # Add user_id to subscription list (supports multiple Slack users per GitHub username)
        subscriptions_s3_path = os.environ.get("EVENTS_S3_PATH", "")
        if subscriptions_s3_path:
            try:
                subscription = FeedSubscription.add_user_id(
                    user_name=github_login,
                    user_id=user_id,
                    s3_path=subscriptions_s3_path,
                )
                # Update cache with fresh subscription data
                SUBSCRIPTION_CACHE[github_login] = subscription.user_ids
                print(f"Added subscription for user {user_id} to {github_login}")
            except Exception as e:
                print(f"Error saving subscription: {e}")
        else:
            print("Warning: EVENTS_S3_PATH not configured, skipping subscription save")

        # Publish updated home view
        publish_home_view(user_id, username, github_login, timeline.events)

    elif action == "unsubscribe":
        if not user_id:
            print("Error: Missing user_id for unsubscribe action")
            return {"statusCode": 400, "body": "Missing user_id"}

        subscriptions_s3_path = os.environ.get("EVENTS_S3_PATH", "")
        if not subscriptions_s3_path:
            print("Warning: EVENTS_S3_PATH not configured, cannot unsubscribe")
            return {"statusCode": 400, "body": "Configuration error"}

        # If github_login not provided, find it
        target_github_login = github_login
        if not target_github_login:
            try:
                target_github_login = FeedSubscription.find_user_subscription(
                    user_id=user_id,
                    s3_path=subscriptions_s3_path,
                )
                if not target_github_login:
                    print(f"No subscription found for user {user_id}")
                    return {"statusCode": 404, "body": "No subscription found"}
                print(f"Found subscription for user {user_id}: {target_github_login}")
            except Exception as e:
                print(f"Error finding subscription: {e}")
                return {"statusCode": 500, "body": "Error finding subscription"}

        # Remove user from subscription
        try:
            subscription = FeedSubscription.remove_user_id(
                user_name=target_github_login,
                user_id=user_id,
                s3_path=subscriptions_s3_path,
            )
            # Update cache with new subscription list
            SUBSCRIPTION_CACHE[target_github_login] = subscription.user_ids
            print(f"Removed user {user_id} from subscription to {target_github_login}")
        except Exception as e:
            print(f"Error removing subscription: {e}")
            return {"statusCode": 500, "body": "Error unsubscribing"}

    elif action == "update":
        if not username:
            print("Error: Missing username for update action")
            return {"statusCode": 400, "body": "Missing username"}

        # Get subscribed user_ids (use cache if available)
        subscribed_user_ids = None
        if username in SUBSCRIPTION_CACHE:
            subscribed_user_ids = SUBSCRIPTION_CACHE[username]
            print(f"Using cached subscription for {username}")
        else:
            # Fetch from S3 and populate cache
            subscriptions_s3_path = os.environ.get("EVENTS_S3_PATH", "")
            if subscriptions_s3_path:
                try:
                    subscribed_user_ids = FeedSubscription.get_user_ids(
                        username, s3_path=subscriptions_s3_path
                    )
                    SUBSCRIPTION_CACHE[username] = subscribed_user_ids
                    if subscribed_user_ids:
                        print(f"Cached subscription for {username} from S3")
                    else:
                        print(
                            f"Cached empty subscription for {username} (no subscribers)"
                        )
                except Exception as e:
                    # Cache None to avoid repeated S3 fetches for failed lookups
                    SUBSCRIPTION_CACHE[username] = None
                    print(f"Error fetching subscription: {e}")

        if not subscribed_user_ids:
            print(f"No subscriptions found for {username}, skipping notifications")
            return {"statusCode": 200, "body": "No subscriptions"}

        # Load EventFeed from S3 using username
        timeline = load_event_timeline(username)

        # Publish updated home view to all subscribed users
        for subscribed_user_id in subscribed_user_ids:
            try:
                publish_home_view(
                    subscribed_user_id, username, username, timeline.events
                )
                print(f"Published home view for user {subscribed_user_id}")
            except Exception as e:
                print(f"Error publishing to user {subscribed_user_id}: {e}")

    else:
        print(f"Error: Unknown action '{action}'")
        return {"statusCode": 400, "body": f"Unknown action: {action}"}

    print(f"Completed processing {action} for user {user_id}")

    return {"statusCode": 200, "body": "success"}
