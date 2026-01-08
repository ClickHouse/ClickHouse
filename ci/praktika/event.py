# minimal module to be used in lambda function
# avoid non std import

import dataclasses
import gzip
import json
import os
from typing import List


@dataclasses.dataclass
class Event:

    class Type:
        RUNNING = "running"
        COMPLETED = "completed"

    class PRStatus:
        OPEN = "open"
        QUEUED = "queued"
        MERGED = "merged"

    type: str
    timestamp: int
    sha: str
    branch: str
    pr_number: int
    pr_status: str
    pr_title: str
    results: list
    ci_status: str
    ext: dict


MAX_TIMELINE_DAYS = 90


@dataclasses.dataclass
class EventTimeline:
    """Time-sorted list of events with only the latest event per PR (deduplicated by pr_number)"""

    events: List[Event] = dataclasses.field(default_factory=list)
    _etag: str = dataclasses.field(default=None, init=False, repr=False)

    def add(self, event: Event):
        """Add event, replacing any existing event for the same PR, maintaining time order"""
        import time

        cutoff_timestamp = int(time.time()) - (MAX_TIMELINE_DAYS * 24 * 60 * 60)

        # Find existing event for the same PR to potentially transfer title
        existing_event = next(
            (e for e in self.events if e.pr_number == event.pr_number), None
        )

        # Transfer pr_title from existing event if new event's title is empty
        if existing_event and not event.pr_title:
            event.pr_title = existing_event.pr_title

        # Filter events: remove same PR, and apply cutoff only to non-open PRs
        self.events = [
            e
            for e in self.events
            if e.pr_number != event.pr_number
            and (e.pr_status == Event.PRStatus.OPEN or e.timestamp >= cutoff_timestamp)
        ]
        self.events.append(event)
        self.events.sort(key=lambda e: e.timestamp)
        return self

    def to_dict(self):
        return {"events": [dataclasses.asdict(e) for e in self.events]}

    @classmethod
    def from_dict(cls, data: dict) -> "EventsTimeline":
        events = [Event(**e) for e in data.get("events", [])]
        return cls(events=events)

    @classmethod
    def from_s3(cls, user_name: str, s3_path="") -> "EventsTimeline":
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
                f"ERROR: Failed to load EventTimeline from S3 - create new timeline: {e}"
            )
            return cls()

    def to_s3(self, user_name: str, s3_path=""):
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
    def update(cls, user_name: str, event: Event, s3_path, max_retries: int = 3):
        import time

        from botocore.exceptions import ClientError

        for attempt in range(max_retries):
            try:
                cls.from_s3(user_name, s3_path=s3_path).add(event).to_s3(
                    user_name, s3_path=s3_path
                )
                return
            except ClientError as e:
                error_code = e.response.get("Error", {}).get("Code")
                if error_code == "PreconditionFailed" and attempt < max_retries - 1:
                    time.sleep(0.1 * (2**attempt))
                    continue
                raise
