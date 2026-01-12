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
    branch: str  # Git branch name
    pr_number: int  # GitHub pull request number
    pr_status: str  # PR status from Event.PRStatus
    pr_title: str  # Pull request title
    result: dict  # Top-level workflow result
    ci_status: str  # Overall CI status (success/failure/pending)
    ext: dict  # Additional extensible metadata


# Maximum number of days to retain non-open PRs in the timeline
MAX_TIMELINE_DAYS = 90


@dataclasses.dataclass
class EventFeed:
    """Time-sorted list of events with only the latest event per PR (deduplicated by pr_number)"""

    events: List[Event] = dataclasses.field(default_factory=list)
    _etag: str = dataclasses.field(
        default=None, init=False, repr=False
    )  # S3 ETag for optimistic locking

    def add(self, event: Event):
        """Add event, replacing any existing event for the same PR, maintaining time order.

        Deduplicates by pr_number (keeps only latest event per PR).
        Applies retention policy: removes events older than MAX_TIMELINE_DAYS,
        except for open PRs which are always retained.

        For merge events (pr_number=0), also replaces events for related PRs listed in
        event.ext["related_prs"], preserving their pr_title, change_url and report_url in
        event.ext["related_pr_info"].

        Also replaces events with the same commit SHA.
        """
        import time

        cutoff_timestamp = int(time.time()) - (MAX_TIMELINE_DAYS * 24 * 60 * 60)

        # Get related PRs if this is a merge event
        related_prs = event.ext.get("related_prs", []) if event.pr_number == 0 else []

        # Collect info from events that will be replaced due to related_prs or matching SHA
        related_pr_info = {}
        for e in self.events:
            # Preserve info if event is in related_prs or has matching SHA
            if (related_prs and e.pr_number in related_prs) or (
                e.sha == event.sha and e.pr_number != event.pr_number
            ):
                related_pr_info[e.pr_number] = {
                    "pr_title": e.pr_title,
                    "change_url": e.ext.get("change_url", ""),
                    "report_url": e.ext.get("report_url", ""),
                }

        # Store related PRs info in event.ext if any were found
        if related_pr_info:
            event.ext["related_pr_info"] = related_pr_info

        # Filter events: remove if pr_number matches OR if it's in related_prs (for merge events) OR if commit SHA matches
        # Also apply retention policy (cutoff only to non-open PRs)
        self.events = [
            e
            for e in self.events
            if e.pr_number != event.pr_number
            and (event.pr_number != 0 or e.pr_number not in related_prs)
            and e.sha != event.sha
            and (e.pr_status == Event.PRStatus.OPEN or e.timestamp >= cutoff_timestamp)
        ]
        self.events.append(event)
        self.events.sort(key=lambda e: e.timestamp)
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
            s3_path: S3 bucket/prefix path (format: bucket/prefix), defaults to EVENTS_S3_PATH env var

        Returns:
            EventFeed instance loaded from S3, or empty timeline if not found

        The timeline is stored as gzip-compressed JSON with ETag for optimistic locking.
        """
        import boto3
        from botocore.exceptions import ClientError

        s3_path = s3_path or os.getenv("EVENTS_S3_PATH")
        assert s3_path
        s3_bucket_name = s3_path.split("/")[0]
        s3_prefix = "/".join(s3_path.split("/")[1:])
        if not s3_bucket_name:
            raise ValueError("S3_EVENT_BUCKET environment variable is not set")
        s3_key = f"{s3_prefix}/{user_name}.json.gz"

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
            s3_path: S3 bucket/prefix path (format: bucket/prefix), defaults to EVENTS_S3_PATH env var

        Returns:
            Self for method chaining

        Uses ETag-based conditional writes (IfMatch) to prevent concurrent modification conflicts.
        """
        import boto3

        s3_path = s3_path or os.getenv("EVENTS_S3_PATH")
        assert s3_path
        s3_bucket_name = s3_path.split("/")[0]
        s3_prefix = "/".join(s3_path.split("/")[1:])
        if not s3_bucket_name:
            raise ValueError("S3_EVENT_BUCKET environment variable is not set")
        s3_key = f"{s3_prefix}/{user_name}.json.gz"

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
        notify_slack: bool = False,
    ):
        """Atomically update timeline with new event using optimistic locking.

        Args:
            user_name: User identifier for timeline file naming
            event: Event to add to timeline
            s3_path: S3 bucket/prefix path (format: bucket/prefix)
            max_retries: Maximum retry attempts on conflicts (default: 3)
            notify_slack: If True, invoke Lambda to notify subscribed Slack users (default: False)

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

        # Notify subscribed Slack users if enabled
        # TODO: refactor to use slack_worker_lambda.invoke()
        if notify_slack:
            try:
                import boto3

                lambda_client = boto3.client("lambda", region_name="us-east-1")

                payload = {
                    "action": "update",
                    "username": user_name,
                }

                response = lambda_client.invoke(
                    FunctionName="praktika_slack_worker",
                    InvocationType="Event",
                    Payload=json.dumps(payload),
                )
                print(
                    f"Invoked Lambda to update feed for {user_name}, StatusCode: {response['StatusCode']}"
                )
            except Exception as e:
                print(f"Warning: Failed to invoke Lambda for {user_name}: {e}")


@dataclasses.dataclass
class FeedSubscription:
    user_ids: List[str]
    user_name: str
    subscribed_at: str

    def to_s3(self, s3_path):
        """Save subscription to S3 storage.

        Stores two files:
        1. {s3_prefix}/gh_{user_name}.json - main subscription by GitHub login
        2. {s3_prefix}/slack_{user_id}.json - reverse lookup by Slack user ID (for each user)

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

        # Save main subscription file by GitHub login
        gh_key = f"{s3_prefix}/gh_{self.user_name}.json"
        s3_client.put_object(
            Bucket=s3_bucket_name,
            Key=gh_key,
            Body=json_data.encode("utf-8"),
            ContentType="application/json",
        )

        # Save reverse lookup files for each Slack user ID
        for user_id in self.user_ids:
            slack_key = f"{s3_prefix}/slack_{user_id}.json"
            reverse_data = json.dumps({"user_name": self.user_name, "user_id": user_id})
            s3_client.put_object(
                Bucket=s3_bucket_name,
                Key=slack_key,
                Body=reverse_data.encode("utf-8"),
                ContentType="application/json",
            )

        return self

    @classmethod
    def get_user_ids(cls, user_name, s3_path):
        """Retrieve user_ids for a given user_name from S3.

        Args:
            user_name: User identifier for subscription file naming
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

        s3_key = f"{s3_prefix}/gh_{user_name}.json"
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
    def add_user_id(cls, user_name, user_id, s3_path):
        """Add a user_id to the subscription list for a user_name.

        Args:
            user_name: GitHub username
            user_id: Slack user ID to add
            s3_path: S3 bucket/prefix path (format: bucket/prefix)

        Returns:
            FeedSubscription instance
        """
        from datetime import datetime, timezone

        # Load existing subscription or create new
        existing_user_ids = cls.get_user_ids(user_name, s3_path)

        # Add user_id if not already present
        if user_id not in existing_user_ids:
            existing_user_ids.append(user_id)

        # Create and save subscription
        subscription = cls(
            user_ids=existing_user_ids,
            user_name=user_name,
            subscribed_at=datetime.now(timezone.utc).isoformat(),
        )
        subscription.to_s3(s3_path)
        return subscription

    @classmethod
    def remove_user_id(cls, user_name, user_id, s3_path):
        """Remove a user_id from the subscription list for a user_name.

        Args:
            user_name: GitHub username
            user_id: Slack user ID to remove
            s3_path: S3 bucket/prefix path (format: bucket/prefix)

        Returns:
            FeedSubscription instance or None if no subscribers remain
        """
        from datetime import datetime, timezone

        import boto3
        from botocore.exceptions import ClientError

        # Load existing subscription
        existing_user_ids = cls.get_user_ids(user_name, s3_path)

        # Remove user_id if present
        if user_id in existing_user_ids:
            existing_user_ids.remove(user_id)

        # Delete the reverse lookup file for this user
        s3_bucket_name = s3_path.split("/")[0]
        s3_prefix = "/".join(s3_path.split("/")[1:])
        slack_key = f"{s3_prefix}/slack_{user_id}.json"
        s3_client = boto3.client("s3")
        try:
            s3_client.delete_object(Bucket=s3_bucket_name, Key=slack_key)
            print(f"Deleted reverse lookup file: {slack_key}")
        except ClientError as e:
            print(f"Warning: Could not delete reverse lookup file {slack_key}: {e}")

        # Create and save subscription (even if empty)
        subscription = cls(
            user_ids=existing_user_ids,
            user_name=user_name,
            subscribed_at=datetime.now(timezone.utc).isoformat(),
        )
        subscription.to_s3(s3_path)
        return subscription

    @classmethod
    def find_user_subscription(cls, user_id, s3_path):
        """Find which GitHub user (user_name) a Slack user is subscribed to.

        Uses reverse lookup file slack_{user_id}.json for efficient lookup.

        Args:
            user_id: Slack user ID to search for
            s3_path: S3 bucket/prefix path (format: bucket/prefix)

        Returns:
            GitHub username (user_name) if found, None otherwise
        """
        import boto3
        from botocore.exceptions import ClientError

        assert s3_path
        s3_bucket_name = s3_path.split("/")[0]
        s3_prefix = "/".join(s3_path.split("/")[1:])
        if not s3_bucket_name:
            raise ValueError("s3_path must contain bucket name")

        # Direct lookup using reverse index file
        slack_key = f"{s3_prefix}/slack_{user_id}.json"
        s3_client = boto3.client("s3")

        try:
            response = s3_client.get_object(Bucket=s3_bucket_name, Key=slack_key)
            json_data = response["Body"].read().decode("utf-8")
            data_dict = json.loads(json_data)
            return data_dict.get("user_name")
        except ClientError as e:
            if e.response["Error"]["Code"] == "NoSuchKey":
                return None
            print(f"Error reading reverse lookup file: {e}")
            return None
