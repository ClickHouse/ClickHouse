"""
S3.copy_file_to_s3_with_version is praktika's optimistic-locking write.
update_workflow_results retries the read-merge-write cycle whenever it returns False.
Verify S3 signals a lost race with PreconditionFailed (other write committed) or ConditionalRequestConflict (other write in flight).
"""

import json
import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

pytest.importorskip("boto3")
from botocore.exceptions import ClientError

from ci.praktika import s3 as s3_module
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


def _upload(tmp_path, monkeypatch, cli, code):
    """Run the versioned upload against a fake S3 backend that fails with `code`."""
    local_file = tmp_path / "result.json"
    local_file.write_text("{}")
    if cli:
        monkeypatch.setattr(s3_module, "BOTO3_AVAILABLE", False)
        monkeypatch.setattr(
            s3_module.Shell, "get_output", lambda *a, **kw: json.dumps(HEAD_OBJECT)
        )
        monkeypatch.setattr(
            s3_module.Shell,
            "get_res_stdout_stderr",
            lambda cmd, **kw: (
                (255, "", f"An error occurred ({code})") if code else (0, "", "")
            ),
        )
    else:
        monkeypatch.setattr(S3, "_boto3_client", _FakeS3Client(code))
    return S3.copy_file_to_s3_with_version("bucket/key", str(local_file), version=1)


@pytest.mark.parametrize("cli", [False, True], ids=["boto3", "cli"])
@pytest.mark.parametrize(
    "code,expected",
    [
        (None, True),
        ("PreconditionFailed", False),
        ("ConditionalRequestConflict", False),
    ],
)
def test_lost_race_returns_false_for_retry(tmp_path, monkeypatch, cli, code, expected):
    assert _upload(tmp_path, monkeypatch, cli, code) is expected


@pytest.mark.parametrize(
    "cli,expected_error",
    [(False, ClientError), (True, RuntimeError)],
    ids=["boto3", "cli"],
)
def test_unexpected_error_raises(tmp_path, monkeypatch, cli, expected_error):
    with pytest.raises(expected_error):
        _upload(tmp_path, monkeypatch, cli, "AccessDenied")
