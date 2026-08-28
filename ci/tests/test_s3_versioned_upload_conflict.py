"""
S3.copy_file_to_s3_with_version is praktika's optimistic-locking write.
update_workflow_results retries the read-merge-write cycle whenever it returns False.
Verify S3 signals a lost race with PreconditionFailed (other write committed) or ConditionalRequestConflict (other write in flight).
"""

import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

pytest.importorskip("boto3")
from botocore.exceptions import ClientError

from ci.praktika.s3 import S3

HEAD_OBJECT = {"ETag": '"etag"', "Metadata": {"version": "0"}}


class _FakeS3Client:
    def __init__(self, put_error_code):
        self.put_error_code = put_error_code

    def head_object(self, Bucket, Key):
        return HEAD_OBJECT

    def put_object(self, **kwargs):
        if self.put_error_code:
            raise ClientError({"Error": {"Code": self.put_error_code}}, "PutObject")


def _upload(tmp_path, monkeypatch, code):
    """Run the versioned upload against a fake S3 backend that fails with `code`."""
    local_file = tmp_path / "result.json"
    local_file.write_text("{}")
    monkeypatch.setattr(S3, "_boto3_client", _FakeS3Client(code))
    return S3.copy_file_to_s3_with_version("bucket/key", str(local_file), version=1)


@pytest.mark.parametrize(
    "code,expected",
    [
        (None, True),
        ("PreconditionFailed", False),
        ("ConditionalRequestConflict", False),
    ],
)
def test_lost_race_returns_false_for_retry(tmp_path, monkeypatch, code, expected):
    assert _upload(tmp_path, monkeypatch, code) is expected


def test_unexpected_error_raises(tmp_path, monkeypatch):
    with pytest.raises(ClientError):
        _upload(tmp_path, monkeypatch, "AccessDenied")
