"""
Tests for `build_download_helper.download_build_with_progress`.

The upgrade check downloads the previous release `.deb` packages during setup.
A transient hiccup that exhausted the (small) retry budget there used to waste
the whole job in the setup phase, recorded only as a generic failure. These
tests pin the resilience and diagnostics behaviour the download path now
guarantees:

  * a genuine 404 (the artifact was never uploaded) fails fast instead of
    burning the entire retry budget;
  * transient errors are retried `retries` times with a capped backoff;
  * a truncated response is treated as a transient failure rather than silently
    producing a corrupt file.

See https://github.com/ClickHouse/ClickHouse/pull/102817#issuecomment-4726000791
"""

import importlib.util
import os
import types

import pytest

# Load the module directly from its file so we do not have to put the whole
# `tests/ci` directory on `sys.path` for the entire pytest session (which would
# risk shadowing equally-named modules in other tests).
_BDH_PATH = os.path.join(
    os.path.dirname(__file__), "..", "..", "tests", "ci", "build_download_helper.py"
)
_spec = importlib.util.spec_from_file_location("build_download_helper", _BDH_PATH)
bdh = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(bdh)


class FakeResponse:
    def __init__(self, body: bytes, content_length=None):
        self.body = body
        length = len(body) if content_length is None else content_length
        self.headers = {"content-length": str(length)}

    @property
    def content(self) -> bytes:
        return self.body

    def iter_content(self, chunk_size=4096):
        for i in range(0, len(self.body), chunk_size):
            yield self.body[i : i + chunk_size]


def _http_error(status_code: int) -> Exception:
    err = Exception(f"HTTP {status_code}")
    err.response = types.SimpleNamespace(status_code=status_code)
    return err


@pytest.fixture
def no_sleep(monkeypatch):
    """Record backoff durations without actually sleeping."""
    sleeps = []
    monkeypatch.setattr(bdh.time, "sleep", sleeps.append)
    return sleeps


def _patch_get(monkeypatch, side_effects):
    """`get_with_retries` returns/raises the next item from `side_effects`."""
    calls = {"count": 0}

    def fake_get_with_retries(*_args, **_kwargs):
        effect = side_effects[calls["count"]]
        calls["count"] += 1
        if isinstance(effect, Exception):
            raise effect
        return effect

    monkeypatch.setattr(bdh, "get_with_retries", fake_get_with_retries)
    return calls


def test_success_writes_file(tmp_path, monkeypatch, no_sleep):
    body = b"hello clickhouse" * 1000
    calls = _patch_get(monkeypatch, [FakeResponse(body)])
    dst = tmp_path / "pkg.deb"

    bdh.download_build_with_progress("http://example/pkg.deb", dst)

    assert dst.read_bytes() == body
    assert calls["count"] == 1
    assert no_sleep == []


def test_404_fails_fast(tmp_path, monkeypatch, no_sleep):
    calls = _patch_get(monkeypatch, [_http_error(404)] * 10)
    dst = tmp_path / "missing.deb"

    with pytest.raises(bdh.DownloadException, match="does not exist"):
        bdh.download_build_with_progress("http://example/missing.deb", dst, retries=10)

    # A 404 must not be retried and must not leave a partial file behind.
    assert calls["count"] == 1
    assert no_sleep == []
    assert not dst.exists()


def test_transient_error_retries_then_fails(tmp_path, monkeypatch, no_sleep):
    retries = 4
    calls = _patch_get(monkeypatch, [ConnectionError("boom")] * retries)
    dst = tmp_path / "pkg.deb"

    with pytest.raises(bdh.DownloadException, match="all 4 retries exceeded"):
        bdh.download_build_with_progress("http://example/pkg.deb", dst, retries=retries)

    assert calls["count"] == retries
    # One sleep between each pair of attempts.
    assert len(no_sleep) == retries - 1


def test_backoff_is_capped(tmp_path, monkeypatch, no_sleep):
    retries = 10
    _patch_get(monkeypatch, [ConnectionError("boom")] * retries)
    dst = tmp_path / "pkg.deb"

    with pytest.raises(bdh.DownloadException):
        bdh.download_build_with_progress("http://example/pkg.deb", dst, retries=retries)

    assert max(no_sleep) == bdh.DOWNLOAD_RETRY_MAX_BACKOFF
    assert all(s <= bdh.DOWNLOAD_RETRY_MAX_BACKOFF for s in no_sleep)


def test_truncated_download_retries_then_succeeds(tmp_path, monkeypatch, no_sleep):
    body = b"x" * 4096
    # First response claims more bytes than it delivers (short read), the retry
    # delivers the full body.
    truncated = FakeResponse(body[:100], content_length=len(body))
    complete = FakeResponse(body)
    calls = _patch_get(monkeypatch, [truncated, complete])
    dst = tmp_path / "pkg.deb"

    bdh.download_build_with_progress("http://example/pkg.deb", dst)

    assert dst.read_bytes() == body
    assert calls["count"] == 2
    assert len(no_sleep) == 1
