"""
Reports render the size of a file next to the link to it, e.g.
`clickhouse-common-static_26.8.1.1332_amd64.deb (1.2 GiB)`.

The size cannot be fetched by the report page itself: the page and the artifact
bucket are different origins and neither bucket allows cross-origin requests.
So it is recorded at upload time - `S3.copy_file_to_s3` remembers the size of
every object it uploads, and `Result` stores it in `ext["link_sizes"]`, keyed by
the link, for `json.html` to render.
"""

import json
import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from ci.praktika.result import Result
from ci.praktika.s3 import S3

DEB = "https://clickhouse-builds.s3.amazonaws.com/REFs/master/abc/build_amd_debug/clickhouse-common-static_26.8.1.1332_amd64.deb"
REPORT = "https://s3.amazonaws.com/clickhouse-test-reports/json.html?REF=master&sha=abc"


@pytest.fixture(autouse=True)
def _clean_registry(monkeypatch):
    """Every test starts with an empty upload registry."""
    monkeypatch.setattr(S3, "_uploaded_object_sizes", {})


def test_set_link_takes_size_from_the_upload():
    S3._uploaded_object_sizes[DEB] = 1288490189
    result = Result("Build (amd, debug)", Result.Status.OK).set_link(DEB)
    assert result.ext["link_sizes"] == {DEB: 1288490189}


def test_set_link_accepts_an_explicit_size():
    result = Result("job", Result.Status.OK).set_link(DEB, size=100)
    assert result.ext["link_sizes"] == {DEB: 100}


def test_link_without_a_known_size_is_not_recorded():
    """A link that is not a file (a link to another report) has no size."""
    result = Result("job", Result.Status.OK).set_link(REPORT)
    assert result.links == [REPORT]
    assert "link_sizes" not in result.ext


def test_create_from_takes_sizes_from_the_uploads():
    S3._uploaded_object_sizes[DEB] = 42
    result = Result.create_from(
        name="job", status=Result.Status.OK, links=[DEB, REPORT]
    )
    assert result.ext["link_sizes"] == {DEB: 42}


def test_sizes_survive_serialization():
    result = Result("job", Result.Status.OK).set_link(DEB, size=1288490189)
    restored = Result.from_dict(json.loads(json.dumps(Result.to_dict(result))))
    assert restored.ext["link_sizes"] == {DEB: 1288490189}


def test_upload_records_the_size_of_the_uploaded_object(tmp_path, monkeypatch):
    pytest.importorskip("boto3")
    from ci.praktika import s3 as s3_module

    class _FakeS3Client:
        def upload_file(self, *args, **kwargs):
            pass

    monkeypatch.setattr(S3, "_boto3_client", _FakeS3Client())
    monkeypatch.setattr(
        s3_module.StorageUsage, "add_uploaded", classmethod(lambda cls, path: None)
    )

    local_file = tmp_path / "clickhouse-server.deb"
    local_file.write_bytes(b"x" * 12345)

    url = S3.copy_file_to_s3("clickhouse-builds/REFs/master/abc", str(local_file))
    assert S3.get_uploaded_size(url) == 12345


def test_failed_upload_does_not_record_a_size(tmp_path, monkeypatch):
    pytest.importorskip("boto3")
    from botocore.exceptions import ClientError

    from ci.praktika import s3 as s3_module

    class _FailingS3Client:
        def upload_file(self, *args, **kwargs):
            raise ClientError({"Error": {"Code": "AccessDenied"}}, "PutObject")

    monkeypatch.setattr(S3, "_boto3_client", _FailingS3Client())
    monkeypatch.setattr(
        s3_module.StorageUsage, "add_uploaded", classmethod(lambda cls, path: None)
    )

    local_file = tmp_path / "clickhouse-server.deb"
    local_file.write_bytes(b"x" * 12345)

    url = S3.copy_file_to_s3(
        "clickhouse-builds/REFs/master/abc", str(local_file), no_strict=True
    )
    assert S3.get_uploaded_size(url) is None
