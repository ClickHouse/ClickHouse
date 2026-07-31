"""
S3.copy_file_to_s3_with_version is praktika's optimistic-locking write. 
update_workflow_results retries the read-merge-write cycle whenever it returns False. 
S3 signals a lost race with PreconditionFailed (theother write committed) or ConditionalRequestConflict (the other writeis in flight).
Treating the latter as fatal kills jobs in pre_run whenever two jobs updated the workflow result concurrently.
"""

import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

pytest.importorskip("boto3")
from botocore.exceptions import ClientError

from ci.praktika.s3 import S3


class _FakeS3Client:
    def __init__(self, put_error_code):
        self.put_error_code = put_error_code

    def head_object(self, Bucket, Key):
        return {"ETag": '"etag"', "Metadata": {"version": "0"}}

    def put_object(self, **kwargs):
        if self.put_error_code:
            raise ClientError({"Error": {"Code": self.put_error_code}}, "PutObject")


def _upload(tmp_path, put_error_code):
    local_file = tmp_path / "result.json"
    local_file.write_text("{}")
    S3._boto3_client = _FakeS3Client(put_error_code)
    try:
        return S3.copy_file_to_s3_with_version("bucket/key", str(local_file), version=1)
    finally:
        S3._boto3_client = None


@pytest.mark.parametrize(
    "code,expected",
    [
        (None, True),
        ("PreconditionFailed", False),
        ("ConditionalRequestConflict", False),
    ],
)
def test_lost_race_returns_false_for_retry(tmp_path, code, expected):
    assert _upload(tmp_path, code) is expected


def test_unexpected_error_raises(tmp_path):
    with pytest.raises(ClientError):
        _upload(tmp_path, "AccessDenied")
