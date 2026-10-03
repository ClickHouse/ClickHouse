"""Minimal module for tracking CI events and timelines.

Designed to be used in AWS Lambda functions with minimal dependencies.
Avoids non-standard library imports in core definitions.
"""

import dataclasses
import gzip
import json
import os
import time
from typing import List


@dataclasses.dataclass
class Event:
    """Represents a single CI event for a pull request.

    Tracks the state of a PR's CI run including status, results, and metadata.
    """

    class Type:
        """Event lifecycle states."""

        RUNNING = "running"  # CI is currently executing
        COMPLETED = "completed"  # CI execution finished

    class PRStatus:
        """Pull request states in GitHub."""

        OPEN = "open"  # PR is open and active
        QUEUED = "queued"  # PR is queued for merging
        MERGED = "merged"  # PR has been merged

    type: str  # Event type from Event.Type
    timestamp: int  # Unix timestamp when event occurred
    sha: str  # Git commit SHA
    result: dict  # Top-level workflow result
    ci_status: str  # Overall CI status (success/failure/pending)
    ext: dict  # Additional extensible metadata (includes: branch, pr_number, pr_status, pr_title)
    linked_events: List["Event"] = dataclasses.field(default_factory=list)


# Maximum number of days to retain non-open PRs in the timeline
MAX_TIMELINE_DAYS = 120


def _sanitize_s3_key_name(name: str) -> str:
    """Sanitize username (potentially email) for safe use in S3 key names.

    Replaces special characters that can cause issues with URL encoding.
    """
    return name.replace("@", "_at_").replace("+", "_plus_")


@dataclasses.dataclass
class EventFeed:
    """Time-sorted list of events with only the latest event per PR (deduplicated by pr_number)"""

    events: List[Event] = dataclasses.field(default_factory=list)
    _etag: str = dataclasses.field(
        default=None, init=False, repr=False
    )  # S3 ETag for optimistic locking

    def add(self, event: Event):
        """Add event to the feed.

        Overwrites existing events that match:
        - If pr_number != 0: matches by pr_number and repo_name
        - If pr_number == 0: matches by sha and repo_name

        Nesting by parent_pr_number is done during rendering elsewhere.

        Retention policy:
        - Groups events by parent_pr_number (0 means no parent)
        - Only removes event groups where ALL events (including parent) are older than MAX_TIMELINE_DAYS
        """
        import time

        cutoff_timestamp = int(time.time()) - (MAX_TIMELINE_DAYS * 24 * 60 * 60)
        running_cutoff_timestamp = int(time.time()) - (12 * 60 * 60)

        def sanitize_event(e: Event) -> None:
            if e.timestamp < running_cutoff_timestamp and e.ci_status in (
                "running",
                "pending",
            ):
                e.ci_status = "failure"
                if not isinstance(e.ext, dict):
                    e.ext = {}
                e.ext["is_cancelled"] = True

        # Remove existing events that match the incoming event
        event_pr_number = event.ext.get("pr_number", 0)
        event_repo_name = event.ext.get("repo_name", "")
        event_sha = event.sha

        sanitize_event(event)

        filtered_events = []
        for e in self.events:
            sanitize_event(e)
            e_pr_number = e.ext.get("pr_number", 0)
            e_repo_name = e.ext.get("repo_name", "")
            e_sha = e.sha

            # Determine if this existing event should be replaced
            should_remove = False
            if event_pr_number != 0:
                # Match by pr_number and repo_name
                if e_pr_number == event_pr_number and e_repo_name == event_repo_name:
                    should_remove = True
            else:
                # Match by sha and repo_name
                if e_sha == event_sha and e_repo_name == event_repo_name:
                    should_remove = True

            if not should_remove:
                filtered_events.append(e)

        self.events = filtered_events

        # Ensure pr_status exists for PR events
        if event_pr_number and event_pr_number > 0:
            event.ext["pr_status"] = Event.PRStatus.OPEN

        if event_pr_number == 0:
            event.ext["pr_status"] = Event.PRStatus.MERGED

        # If we received a merge-result event (pr_number == 0) that references a parent PR,
        # mark that parent PR as merged.
        if event_pr_number == 0:
            parent_pr_number = event.ext.get("parent_pr_number", 0)
            linked_pr_number = event.ext.get("linked_pr_number", 0)
            if parent_pr_number and parent_pr_number > 0 and event_repo_name:
                for e in self.events:
                    e_pr_number = e.ext.get("pr_number", 0)
                    e_repo_name = e.ext.get("repo_name", "")
                    if (
                        e_pr_number == parent_pr_number
                        and e_repo_name == event_repo_name
                    ):
                        e.ext["pr_status"] = Event.PRStatus.MERGED
                        break

            if (
                linked_pr_number
                and linked_pr_number > 0
                and linked_pr_number != parent_pr_number
                and event_repo_name
            ):
                for e in self.events:
                    e_pr_number = e.ext.get("pr_number", 0)
                    e_repo_name = e.ext.get("repo_name", "")
                    if (
                        e_pr_number == linked_pr_number
                        and e_repo_name == event_repo_name
                    ):
                        e.ext["pr_status"] = Event.PRStatus.MERGED
                        break

        # Append the new event
        self.events.append(event)

        # Sort events by timestamp (newest first)
        self.events.sort(key=lambda e: e.timestamp, reverse=True)

        for e in self.events:
            sanitize_event(e)

        # Apply retention policy based on parent_pr_number grouping
        # Build a map of parent_pr_number -> list of events
        pr_groups = {}
        for e in self.events:
            pr_number = e.ext.get("pr_number", 0)
            parent_pr_number = e.ext.get("parent_pr_number", 0)

            # Events are grouped by their parent_pr_number
            # If parent_pr_number is 0, the event groups by its own pr_number (it's a root)
            group_key = parent_pr_number if parent_pr_number > 0 else pr_number

            if group_key not in pr_groups:
                pr_groups[group_key] = []
            pr_groups[group_key].append(e)

        # Determine which groups to keep
        # Keep a group if ANY event in the group is recent OR is an open PR
        events_to_keep = []
        for group_key, group_events in pr_groups.items():
            # Check if any event in the group should be retained
            keep_group = False
            for e in group_events:
                if e.timestamp >= cutoff_timestamp:
                    keep_group = True
                    break

            if keep_group:
                events_to_keep.extend(group_events)

        self.events = events_to_keep
        # Re-sort after filtering
        self.events.sort(key=lambda e: e.timestamp, reverse=True)

        return self

    def to_dict(self):
        """Serialize timeline to dictionary format."""
        return {"events": [dataclasses.asdict(e) for e in self.events]}

    @classmethod
    def from_dict(cls, data: dict) -> "EventFeed":
        """Deserialize timeline from dictionary format."""
        events = [Event(**e) for e in data.get("events", [])]
        return cls(events=events)

    @classmethod
    def from_s3(cls, user_name: str, s3_path="") -> "EventFeed":
        """Load timeline from S3 storage.

        Args:
            user_name: User identifier for timeline file naming
            s3_path: S3 bucket/prefix path (format: bucket/prefix), defaults to EVENT_FEED_S3_PATH env var

        Returns:
            EventFeed instance loaded from S3, or empty timeline if not found

        The timeline is stored as gzip-compressed JSON with ETag for optimistic locking.
        """
        import boto3
        from botocore.exceptions import ClientError

        s3_path = s3_path or os.getenv("EVENT_FEED_S3_PATH")
        assert s3_path
        s3_bucket_name = s3_path.split("/")[0]
        s3_prefix = "/".join(s3_path.split("/")[1:])
        if not s3_bucket_name:
            raise ValueError("S3_EVENT_BUCKET environment variable is not set")
        s3_key = f"{s3_prefix}/events/{_sanitize_s3_key_name(user_name)}.json.gz"

        s3_client = boto3.client("s3")
        try:
            response = s3_client.get_object(Bucket=s3_bucket_name, Key=s3_key)
            compressed_data = response["Body"].read()

            json_data = gzip.decompress(compressed_data).decode("utf-8")
            data_dict = json.loads(json_data)

            instance = cls.from_dict(data_dict)
            instance._etag = response.get("ETag")
            return instance
        except ClientError as e:
            if e.response["Error"]["Code"] == "NoSuchKey":
                return cls()
            raise
        except Exception as e:
            print(
                f"WARNING: Failed to load EventFeed from S3 - create new timeline: {e}"
            )
            return cls()

    def to_s3(self, user_name: str, s3_path=""):
        """Save timeline to S3 storage.

        Args:
            user_name: User identifier for timeline file naming
            s3_path: S3 bucket/prefix path (format: bucket/prefix), defaults to EVENT_FEED_S3_PATH env var

        Returns:
            Self for method chaining

        Uses ETag-based conditional writes (IfMatch) to prevent concurrent modification conflicts.
        Stores files under {s3_prefix}/events/ subdirectory.
        """
        import boto3

        s3_path = s3_path or os.getenv("EVENT_FEED_S3_PATH")
        assert s3_path
        s3_bucket_name = s3_path.split("/")[0]
        s3_prefix = "/".join(s3_path.split("/")[1:])
        if not s3_bucket_name:
            raise ValueError("S3_EVENT_BUCKET environment variable is not set")
        s3_key = f"{s3_prefix}/events/{_sanitize_s3_key_name(user_name)}.json.gz"

        data_dict = self.to_dict()
        json_data = json.dumps(data_dict)
        compressed_data = gzip.compress(json_data.encode("utf-8"))

        s3_client = boto3.client("s3")
        put_kwargs = {
            "Bucket": s3_bucket_name,
            "Key": s3_key,
            "Body": compressed_data,
            "ContentType": "application/json",
            "ContentEncoding": "gzip",
        }

        if self._etag:
            put_kwargs["IfMatch"] = self._etag

        response = s3_client.put_object(**put_kwargs)
        self._etag = response.get("ETag")
        return self

    @classmethod
    def update(
        cls,
        user_name: str,
        event: Event,
        s3_path,
        max_retries: int = 3,
    ):
        """Atomically update timeline with new event using optimistic locking.

        Args:
            user_name: User identifier for timeline file naming
            event: Event to add to timeline
            s3_path: S3 bucket/prefix path (format: bucket/prefix)
            max_retries: Maximum retry attempts on conflicts (default: 3)

        Implements exponential backoff retry logic for handling concurrent updates.
        On PreconditionFailed (ETag mismatch), retries with exponentially increasing delays.
        """
        from botocore.exceptions import ClientError

        for attempt in range(max_retries):
            try:
                cls.from_s3(user_name, s3_path=s3_path).add(event).to_s3(
                    user_name, s3_path=s3_path
                )
                break
            except ClientError as e:
                error_code = e.response.get("Error", {}).get("Code")
                if error_code == "PreconditionFailed" and attempt < max_retries - 1:
                    time.sleep(0.1 * (2**attempt))
                    continue
                raise

    @staticmethod
    def notify_slack_users(user_emails: List[str], region_name: str = "us-east-1"):
        """Invoke Lambda to notify Slack users about feed updates.

        Args:
            user_emails: List of user email addresses to notify
            region_name: AWS region for Lambda (default: us-east-1)
        """
        if not user_emails:
            return

        try:
            import boto3

            lambda_client = boto3.client("lambda", region_name=region_name)

            payload = {
                "action": "update",
                "emails": user_emails,
            }

            response = lambda_client.invoke(
                FunctionName="praktika_slack_worker",
                InvocationType="Event",
                Payload=json.dumps(payload),
            )
            print(
                f"Invoked Lambda to update feeds for {len(user_emails)} user(s), StatusCode: {response['StatusCode']}"
            )
        except Exception as e:
            print(f"Warning: Failed to invoke Lambda: {e}")


@dataclasses.dataclass
class FeedSubscription:
    user_ids: List[str]
    user_email: str  # Email address of the user being tracked
    subscribed_at: str
    user_prefs: dict = dataclasses.field(default_factory=dict)

    def to_s3(self, s3_path):
        """Save subscription to S3 storage.

        Stores two files:
        1. {s3_prefix}/subscriptions/email_{sanitized_email}.json - main subscription by email
        2. {s3_prefix}/subscriptions/slack_{user_id}.json - reverse lookup by Slack user ID (for each user)

        Args:
            s3_path: S3 bucket/prefix path (format: bucket/prefix)

        Returns:
            Self for method chaining
        """
        import boto3

        assert s3_path
        s3_bucket_name = s3_path.split("/")[0]
        s3_prefix = "/".join(s3_path.split("/")[1:])
        if not s3_bucket_name:
            raise ValueError("s3_path must contain bucket name")

        s3_client = boto3.client("s3")
        data_dict = dataclasses.asdict(self)
        json_data = json.dumps(data_dict)

        # Save main subscription file by email
        email_key = f"{s3_prefix}/subscriptions/email_{_sanitize_s3_key_name(self.user_email)}.json"
        s3_client.put_object(
            Bucket=s3_bucket_name,
            Key=email_key,
            Body=json_data.encode("utf-8"),
            ContentType="application/json",
        )

        # Save reverse lookup files for each Slack user ID
        for user_id in self.user_ids:
            slack_key = f"{s3_prefix}/subscriptions/slack_{user_id}.json"
            reverse_data = json.dumps(
                {"user_email": self.user_email, "user_id": user_id}
            )
            s3_client.put_object(
                Bucket=s3_bucket_name,
                Key=slack_key,
                Body=reverse_data.encode("utf-8"),
                ContentType="application/json",
            )

        return self

    @classmethod
    def get_subscription(cls, user_email, s3_path):
        """Retrieve full subscription object for a given email from S3.

        Backward compatible with older subscription files that do not include user_prefs.
        """
        import boto3
        from botocore.exceptions import ClientError

        assert s3_path
        s3_bucket_name = s3_path.split("/")[0]
        s3_prefix = "/".join(s3_path.split("/")[1:])
        if not s3_bucket_name:
            raise ValueError("s3_path must contain bucket name")

        s3_key = (
            f"{s3_prefix}/subscriptions/email_{_sanitize_s3_key_name(user_email)}.json"
        )
        s3_client = boto3.client("s3")

        try:
            response = s3_client.get_object(Bucket=s3_bucket_name, Key=s3_key)
            json_data = response["Body"].read().decode("utf-8")
            data_dict = json.loads(json_data)
            if not isinstance(data_dict, dict):
                return cls(
                    user_ids=[], user_email=user_email, subscribed_at="", user_prefs={}
                )

            return cls(
                user_ids=data_dict.get("user_ids", []) or [],
                user_email=data_dict.get("user_email") or user_email,
                subscribed_at=data_dict.get("subscribed_at", "") or "",
                user_prefs=data_dict.get("user_prefs", {}) or {},
            )
        except ClientError as e:
            if e.response["Error"]["Code"] == "NoSuchKey":
                return cls(
                    user_ids=[], user_email=user_email, subscribed_at="", user_prefs={}
                )
            raise

    @classmethod
    def get_user_ids(cls, user_email, s3_path):
        """Retrieve user_ids for a given email from S3.

        Args:
            user_email: Email address for subscription file naming
            s3_path: S3 bucket/prefix path (format: bucket/prefix)

        Returns:
            List of user_id strings if subscription exists, empty list otherwise
        """
        import boto3
        from botocore.exceptions import ClientError

        assert s3_path
        s3_bucket_name = s3_path.split("/")[0]
        s3_prefix = "/".join(s3_path.split("/")[1:])
        if not s3_bucket_name:
            raise ValueError("s3_path must contain bucket name")

        s3_key = (
            f"{s3_prefix}/subscriptions/email_{_sanitize_s3_key_name(user_email)}.json"
        )
        s3_client = boto3.client("s3")
        try:
            response = s3_client.get_object(Bucket=s3_bucket_name, Key=s3_key)
            json_data = response["Body"].read().decode("utf-8")
            data_dict = json.loads(json_data)
            return data_dict.get("user_ids", [])
        except ClientError as e:
            if e.response["Error"]["Code"] == "NoSuchKey":
                return []
            raise

    @classmethod
    def add_user_id(cls, user_email, user_id, s3_path):
        """Add a user_id to the subscription list for a user email.

        Args:
            user_email: Email address
            user_id: Slack user ID to add
            s3_path: S3 bucket/prefix path (format: bucket/prefix)

        Returns:
            FeedSubscription instance
        """
        from datetime import datetime, timezone

        # Load existing subscription or create new (preserve user_prefs)
        existing = cls.get_subscription(user_email, s3_path)
        existing_user_ids = existing.user_ids
        existing_user_prefs = existing.user_prefs

        # Add user_id if not already present
        if user_id not in existing_user_ids:
            existing_user_ids.append(user_id)

        # Create and save subscription
        subscription = cls(
            user_ids=existing_user_ids,
            user_email=user_email,
            subscribed_at=datetime.now(timezone.utc).isoformat(),
            user_prefs=existing_user_prefs,
        )
        subscription.to_s3(s3_path)
        return subscription

    @classmethod
    def remove_user_id(cls, user_email, user_id, s3_path):
        """Remove a user_id from the subscription list for a user email.

        Args:
            user_email: Email address
            user_id: Slack user ID to remove
            s3_path: S3 bucket/prefix path (format: bucket/prefix)

        Returns:
            FeedSubscription instance or None if no subscribers remain
        """
        from datetime import datetime, timezone

        import boto3
        from botocore.exceptions import ClientError

        # Load existing subscription (preserve user_prefs)
        existing = cls.get_subscription(user_email, s3_path)
        existing_user_ids = existing.user_ids
        existing_user_prefs = existing.user_prefs

        # Remove user_id if present
        if user_id in existing_user_ids:
            existing_user_ids.remove(user_id)

        if isinstance(existing_user_prefs, dict) and user_id in existing_user_prefs:
            del existing_user_prefs[user_id]

        # Delete the reverse lookup file for this user
        s3_bucket_name = s3_path.split("/")[0]
        s3_prefix = "/".join(s3_path.split("/")[1:])
        slack_key = f"{s3_prefix}/subscriptions/slack_{user_id}.json"
        s3_client = boto3.client("s3")
        try:
            s3_client.delete_object(Bucket=s3_bucket_name, Key=slack_key)
            print(f"Deleted reverse lookup file: {slack_key}")
        except ClientError as e:
            print(f"Warning: Could not delete reverse lookup file {slack_key}: {e}")

        # Create and save subscription (even if empty)
        subscription = cls(
            user_ids=existing_user_ids,
            user_email=user_email,
            subscribed_at=datetime.now(timezone.utc).isoformat(),
            user_prefs=existing_user_prefs,
        )
        subscription.to_s3(s3_path)
        return subscription

    @classmethod
    def find_user_subscription(cls, user_id, s3_path):
        """Find which email address a Slack user is subscribed to.

        Uses reverse lookup file slack_{user_id}.json for efficient lookup.

        Args:
            user_id: Slack user ID to search for
            s3_path: S3 bucket/prefix path (format: bucket/prefix)

        Returns:
            Email address if found, None otherwise
        """
        import boto3
        from botocore.exceptions import ClientError

        assert s3_path
        s3_bucket_name = s3_path.split("/")[0]
        s3_prefix = "/".join(s3_path.split("/")[1:])
        if not s3_bucket_name:
            raise ValueError("s3_path must contain bucket name")

        # Direct lookup using reverse index file
        slack_key = f"{s3_prefix}/subscriptions/slack_{user_id}.json"
        s3_client = boto3.client("s3")

        try:
            response = s3_client.get_object(Bucket=s3_bucket_name, Key=slack_key)
            json_data = response["Body"].read().decode("utf-8")
            data_dict = json.loads(json_data)
            # Support both old (user_name) and new (user_email) keys for backward compatibility
            return data_dict.get("user_email") or data_dict.get("user_name")
        except ClientError as e:
            if e.response["Error"]["Code"] == "NoSuchKey":
                return None
            print(f"Error reading reverse lookup file: {e}")
            return None
