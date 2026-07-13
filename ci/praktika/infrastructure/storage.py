import json
from dataclasses import dataclass, field
from typing import Any, Dict

from botocore.exceptions import ClientError
from ._utils import aws_account_id, aws_client


class Storage:

    @dataclass
    class Config:
        """An S3 bucket with a mandatory retention policy.

        Standard configuration: no versioning, default storage class.
        If public=True, block-public-access is disabled and a public-read
        bucket policy is applied so objects are accessible via HTTPS without
        signing.

        Objects are automatically deleted after `retention_days` days via a
        lifecycle rule. Idempotent: all settings are reconciled on every deploy.
        """

        name: str
        retention_days: int  # required — objects are deleted after this many days
        public: bool = False
        region: str = ""
        ext: Dict[str, Any] = field(default_factory=dict)

        def deploy(self):
            s3 = aws_client("s3", self.region, self.name)

            # Create bucket if missing
            try:
                s3.head_bucket(Bucket=self.name)
                print(f"Bucket '{self.name}' already exists")
            except ClientError as e:
                if e.response["Error"]["Code"] == "404":
                    import time
                    kwargs = {"Bucket": self.name}
                    if self.region and self.region != "us-east-1":
                        kwargs["CreateBucketConfiguration"] = {"LocationConstraint": self.region}
                    for attempt in range(5):
                        try:
                            s3.create_bucket(**kwargs)
                            print(f"Created bucket '{self.name}'")
                            break
                        except ClientError as ce:
                            if ce.response["Error"]["Code"] == "OperationAborted" and attempt < 4:
                                print(f"Bucket operation in progress, retrying in 5s...")
                                time.sleep(5)
                            else:
                                raise
                else:
                    raise

            # Public access
            if self.public:
                account_id = aws_account_id(self.region)
                s3.put_public_access_block(
                    Bucket=self.name,
                    PublicAccessBlockConfiguration={
                        "BlockPublicAcls": False,
                        "IgnorePublicAcls": False,
                        "BlockPublicPolicy": False,
                        "RestrictPublicBuckets": False,
                    },
                )
                s3.put_bucket_policy(
                    Bucket=self.name,
                    Policy=json.dumps({
                        "Version": "2012-10-17",
                        "Statement": [
                            {
                                "Sid": "OwnerFullAccess",
                                "Effect": "Allow",
                                "Principal": {"AWS": f"arn:aws:iam::{account_id}:root"},
                                "Action": "s3:*",
                                "Resource": [
                                    f"arn:aws:s3:::{self.name}",
                                    f"arn:aws:s3:::{self.name}/*",
                                ],
                            },
                            {
                                "Sid": "PublicRead",
                                "Effect": "Allow",
                                "Principal": "*",
                                "Action": "s3:GetObject",
                                "Resource": f"arn:aws:s3:::{self.name}/*",
                            },
                        ],
                    }),
                )
                print(f"Bucket '{self.name}' configured for public read")

            # Lifecycle rule — expire all objects after retention_days
            rule_id = "retention"
            try:
                current = s3.get_bucket_lifecycle_configuration(Bucket=self.name)
                existing = next(
                    (r for r in current.get("Rules", []) if r.get("ID") == rule_id), None
                )
                if existing and existing.get("Expiration", {}).get("Days") == self.retention_days:
                    print(f"Lifecycle retention ({self.retention_days}d) already set for '{self.name}'")
                    self.ext["bucket_arn"] = f"arn:aws:s3:::{self.name}"
                    return self
            except ClientError as e:
                if e.response["Error"]["Code"] != "NoSuchLifecycleConfiguration":
                    raise

            s3.put_bucket_lifecycle_configuration(
                Bucket=self.name,
                LifecycleConfiguration={
                    "Rules": [{
                        "ID": rule_id,
                        "Status": "Enabled",
                        "Filter": {"Prefix": ""},
                        "Expiration": {"Days": self.retention_days},
                    }],
                },
            )
            print(f"Set retention {self.retention_days}d on bucket '{self.name}'")
            self.ext["bucket_arn"] = f"arn:aws:s3:::{self.name}"
            return self

        def delete(self):
            s3 = aws_client("s3", self.region, self.name)
            try:
                paginator = s3.get_paginator("list_objects_v2")
                for page in paginator.paginate(Bucket=self.name):
                    objects = [{"Key": o["Key"]} for o in page.get("Contents") or []]
                    if objects:
                        s3.delete_objects(Bucket=self.name, Delete={"Objects": objects})
                s3.delete_bucket(Bucket=self.name)
                print(f"Deleted bucket '{self.name}'")
            except ClientError as e:
                if e.response["Error"]["Code"] == "NoSuchBucket":
                    print(f"Bucket '{self.name}' does not exist, skipping")
                else:
                    raise
