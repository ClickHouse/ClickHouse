from ._utils import aws_client
import json
from dataclasses import dataclass, field
from typing import Any, Dict


class SQSQueue:

    @dataclass
    class Config:
        name: str
        region: str = ""
        # Visibility timeout in seconds (how long a message is hidden after being received)
        visibility_timeout: int = 600
        # Message retention period in seconds (max 14 days = 1209600)
        message_retention: int = 86400
        # Dead letter queue: max receives before sending to DLQ
        max_receive_count: int = 3
        # Create a dead letter queue automatically
        dead_letter_queue: bool = True
        ext: Dict[str, Any] = field(default_factory=dict)

        def fetch(self):
            """Fetch queue configuration from AWS."""

            sqs = aws_client("sqs", self.region, self.name)

            try:
                resp = sqs.get_queue_url(QueueName=self.name)
                self.ext["queue_url"] = resp["QueueUrl"]

                attrs = sqs.get_queue_attributes(
                    QueueUrl=self.ext["queue_url"],
                    AttributeNames=["All"],
                )["Attributes"]
                self.ext["queue_arn"] = attrs.get("QueueArn", "")
                self.ext["attributes"] = attrs
                print(f"Fetched SQS queue: {self.name}")
            except sqs.exceptions.QueueDoesNotExist:
                raise Exception(f"SQS queue '{self.name}' not found")
            return self

        def deploy(self):
            """Create or update the SQS queue."""

            sqs = aws_client("sqs", self.region, self.name)

            dlq_arn = None
            if self.dead_letter_queue:
                dlq_arn = self._ensure_dlq(sqs)

            attributes = {
                "VisibilityTimeout": str(self.visibility_timeout),
                "MessageRetentionPeriod": str(self.message_retention),
            }
            if dlq_arn:
                attributes["RedrivePolicy"] = json.dumps(
                    {
                        "deadLetterTargetArn": dlq_arn,
                        "maxReceiveCount": self.max_receive_count,
                    }
                )

            try:
                resp = sqs.get_queue_url(QueueName=self.name)
                queue_url = resp["QueueUrl"]
                sqs.set_queue_attributes(
                    QueueUrl=queue_url, Attributes=attributes
                )
                print(f"Updated SQS queue: {self.name}")
            except sqs.exceptions.QueueDoesNotExist:
                resp = sqs.create_queue(
                    QueueName=self.name, Attributes=attributes
                )
                queue_url = resp["QueueUrl"]
                print(f"Created SQS queue: {self.name}")

            # Fetch full config after deploy
            attrs = sqs.get_queue_attributes(
                QueueUrl=queue_url, AttributeNames=["All"]
            )["Attributes"]
            self.ext["queue_url"] = queue_url
            self.ext["queue_arn"] = attrs.get("QueueArn", "")
            print(f"Queue URL: {queue_url}")
            print(f"Queue ARN: {self.ext['queue_arn']}")
            return self

        def _ensure_dlq(self, sqs) -> str:
            """Create or get the dead letter queue, return its ARN."""
            dlq_name = f"{self.name}-dlq"
            try:
                resp = sqs.get_queue_url(QueueName=dlq_name)
                dlq_url = resp["QueueUrl"]
                print(f"DLQ already exists: {dlq_name}")
            except sqs.exceptions.QueueDoesNotExist:
                resp = sqs.create_queue(
                    QueueName=dlq_name,
                    Attributes={
                        "MessageRetentionPeriod": str(self.message_retention),
                    },
                )
                dlq_url = resp["QueueUrl"]
                print(f"Created DLQ: {dlq_name}")

            attrs = sqs.get_queue_attributes(
                QueueUrl=dlq_url, AttributeNames=["QueueArn"]
            )["Attributes"]
            return attrs["QueueArn"]

        def shutdown(self, force: bool = True):
            """Delete the queue and its DLQ."""

            sqs = aws_client("sqs", self.region, self.name)

            for queue_name in [self.name, f"{self.name}-dlq"]:
                try:
                    resp = sqs.get_queue_url(QueueName=queue_name)
                    sqs.delete_queue(QueueUrl=resp["QueueUrl"])
                    print(f"Deleted SQS queue: {queue_name}")
                except sqs.exceptions.QueueDoesNotExist:
                    print(f"SQS queue not found: {queue_name}")
