import json
import os

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

    blocks = []

    def format_event_text(event, pr_status, indent=""):
        """Format event text with optional indentation for linked events.

        Args:
            event: Event object
            pr_status: PR status ("open" or "merged") determined by parent analysis
            indent: Indentation string for nested events
        """
        # PR status emoji
        pr_status_emoji = ":pr_open:" if pr_status == "open" else ":pr_merged:"

        # Get change URL from event.ext
        change_url = event.ext.get("change_url", "")

        # Check if any job has failed or errored
        has_failures = False
        if hasattr(event, "result") and event.result and event.result.get("results"):
            has_failures = any(
                r.get("status") in ["failure", "error"]
                for r in event.result.get("results", [])
            )

        # CI status emoji based on event.ci_status
        if event.ci_status in ["pending", "running"]:
            ci_running_status_emoji = ":frog-run:"
        else:
            ci_running_status_emoji = ":checkered_flag:"

        if event.ci_status == "success":
            ci_status_emoji = ":success_sign:"
        elif event.ci_status in ["pending", "running"]:
            ci_status_emoji = ":worry-stop:" if has_failures else ":frog-run:"
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
            success_cnt = 0
            running_cnt = 0
            total_cnt = 0

            for r in event.result.get("results", []):
                status = r.get("status", "")
                total_cnt += 1

                if status == "failure":
                    fail_cnt += 1
                elif status == "error":
                    error_cnt += 1
                elif status == "success":
                    success_cnt += 1
                elif status in ["pending", "running"]:
                    running_cnt += 1

            # Build compact one-line summary
            parts = []
            if fail_cnt or error_cnt:
                parts.append(f"{fail_cnt + error_cnt} failed")
            if running_cnt:
                parts.append(f"{running_cnt} running")
            if success_cnt:
                parts.append(f"{success_cnt} ok")

            if parts:
                summary = " · ".join(parts)
                if report_url_text:
                    event_text += f"\n{indent}{summary} · {report_url_text}"
                else:
                    event_text += f"\n{indent}{summary}"
            elif report_url_text:
                event_text += f"\n{indent}{report_url_text}"

            # Add related PRs if available (only for parent events)
            if not indent:
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
            if root_pr_number > 0 and root_pr_number not in parent_pr_numbers:
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
            pr_status = "open"
            for child in children:
                if child.ext.get("pr_number", 0) == 0:
                    pr_status = "merged"
                    break

            # Format parent event
            event_text = format_event_text(root_event, pr_status, indent="")

            # Add child events with indentation (sorted by timestamp, newest first)
            if children:
                children_sorted = sorted(
                    children, key=lambda e: e.timestamp, reverse=True
                )
                for child_event in children_sorted:
                    child_text = format_event_text(
                        child_event, pr_status, indent="        | "
                    )
                    event_text += "\n" + child_text

            # Create section block for the event (parent + children)
            blocks.append(
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": event_text,
                    },
                }
            )
            blocks.append({"type": "divider"})

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

    Note: 'github_login' parameter now contains user email addresses for email-based subscriptions.
    """
    print(f"Worker Lambda invoked with event: {json.dumps(event)}")

    action = event.get("action", "")
    user_id = event.get("user_id", "")
    username = event.get("username", "")
    user_email = event.get("github_login", "")  # Contains email address

    if not action:
        print("Error: Missing action parameter")
        return {"statusCode": 400, "body": "Missing action"}

    print(f"Processing {action} for user {user_id} ({username}), Email: {user_email}")

    if action == "subscribe":
        if not user_id:
            print("Error: Missing user_id for subscribe action")
            return {"statusCode": 400, "body": "Missing user_id"}

        if not user_email:
            print("Error: Missing user email for subscribe action")
            return {"statusCode": 400, "body": "Missing user email"}

        # Load EventFeed from S3 using email
        timeline = load_event_timeline(user_email)

        # Add user_id to subscription list (supports multiple Slack users per email)
        subscriptions_s3_path = os.environ.get("EVENT_FEED_S3_PATH", "")
        if subscriptions_s3_path:
            try:
                subscription = FeedSubscription.add_user_id(
                    user_email=user_email,
                    user_id=user_id,
                    s3_path=subscriptions_s3_path,
                )
                # Update cache with fresh subscription data
                SUBSCRIPTION_CACHE[user_email] = subscription.user_ids
                print(f"Added subscription for user {user_id} to {user_email}")
            except Exception as e:
                print(f"Error saving subscription: {e}")
        else:
            print(
                "Warning: EVENT_FEED_S3_PATH not configured, skipping subscription save"
            )

        # Publish updated home view
        publish_home_view(user_id, username, user_email, timeline.events)

    elif action == "unsubscribe":
        if not user_id:
            print("Error: Missing user_id for unsubscribe action")
            return {"statusCode": 400, "body": "Missing user_id"}

        subscriptions_s3_path = os.environ.get("EVENT_FEED_S3_PATH", "")
        if not subscriptions_s3_path:
            print("Warning: EVENT_FEED_S3_PATH not configured, cannot unsubscribe")
            return {"statusCode": 400, "body": "Configuration error"}

        # If user_email not provided, find it
        target_email = user_email
        if not target_email:
            try:
                target_email = FeedSubscription.find_user_subscription(
                    user_id=user_id,
                    s3_path=subscriptions_s3_path,
                )
                if not target_email:
                    print(f"No subscription found for user {user_id}")
                    return {"statusCode": 404, "body": "No subscription found"}
                print(f"Found subscription for user {user_id}: {target_email}")
            except Exception as e:
                print(f"Error finding subscription: {e}")
                return {"statusCode": 500, "body": "Error finding subscription"}

        # Remove user from subscription
        try:
            subscription = FeedSubscription.remove_user_id(
                user_email=target_email,
                user_id=user_id,
                s3_path=subscriptions_s3_path,
            )
            # Update cache with new subscription list
            SUBSCRIPTION_CACHE[target_email] = subscription.user_ids
            print(f"Removed user {user_id} from subscription to {target_email}")
        except Exception as e:
            print(f"Error removing subscription: {e}")
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
        subscriptions_s3_path = os.environ.get("EVENT_FEED_S3_PATH", "")

        # Process each email
        for email in emails:
            print(f"Processing update for {email}")

            # Get subscribed user_ids (use cache if available)
            subscribed_user_ids = None
            if email in SUBSCRIPTION_CACHE:
                subscribed_user_ids = SUBSCRIPTION_CACHE[email]
                print(f"Using cached subscription for {email}")
            else:
                # Fetch from S3 and populate cache
                if subscriptions_s3_path:
                    try:
                        subscribed_user_ids = FeedSubscription.get_user_ids(
                            email, s3_path=subscriptions_s3_path
                        )
                        SUBSCRIPTION_CACHE[email] = subscribed_user_ids
                        if subscribed_user_ids:
                            print(f"Cached subscription for {email} from S3")
                        else:
                            print(
                                f"Cached empty subscription for {email} (no subscribers)"
                            )
                    except Exception as e:
                        # Cache None to avoid repeated S3 fetches for failed lookups
                        SUBSCRIPTION_CACHE[email] = None
                        print(f"Error fetching subscription: {e}")

            if not subscribed_user_ids:
                print(f"No subscriptions found for {email}, skipping notifications")
                continue

            # Load EventFeed from S3 using email
            timeline = load_event_timeline(email)

            # Publish updated home view to all subscribed users
            for subscribed_user_id in subscribed_user_ids:
                try:
                    publish_home_view(subscribed_user_id, email, email, timeline.events)
                    print(f"Published home view for user {subscribed_user_id}")
                except Exception as e:
                    print(f"Error publishing to user {subscribed_user_id}: {e}")

    else:
        print(f"Error: Unknown action '{action}'")
        return {"statusCode": 400, "body": f"Unknown action: {action}"}

    print(f"Completed processing {action} for user {user_id}")

    return {"statusCode": 200, "body": "success"}
