"""Tests for Praktika's non-versioned S3 upload behavior."""

import os
import sys
from unittest.mock import Mock

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

pytest.importorskip("boto3")
from botocore.exceptions import ClientError

from ci.praktika.s3 import S3
from ci.praktika.usage import StorageUsage


class _FailingS3Client:
    def upload_file(self, *_args, **_kwargs):
        raise ClientError(
            {"Error": {"Code": "AccessDenied", "Message": "denied"}},
            "UploadFile",
        )


def test_no_strict_upload_failure_returns_false_without_recording_usage(
    tmp_path, monkeypatch
):
    local_file = tmp_path / "artifact.txt"
    local_file.write_text("payload")
    add_uploaded = Mock()

    monkeypatch.setattr(S3, "_boto3_client", _FailingS3Client())
    monkeypatch.setattr(StorageUsage, "add_uploaded", add_uploaded)

    result = S3.copy_file_to_s3(
        s3_path="bucket/key", local_path=str(local_file), no_strict=True
    )

    assert result is False
    add_uploaded.assert_not_called()
