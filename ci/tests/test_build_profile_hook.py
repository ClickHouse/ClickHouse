"""Regression tests for the build profile post-hook and its producer.

See ClickHouse/ClickHouse#84159.

Layers covered:

* The producer (``utils/prepare-time-trace/prepare-time-trace.sh``): for a build
  with no readable objects (cross-arch/non-Linux) it must leave
  ``binary_sizes.txt`` empty, not write a junk ``0`` row that no consumer can
  parse.
* The build subset gate (``_should_profile``): telemetry is collected only for
  an explicit subset of builds (amd_release, arm_release) so that ~25 Build
  variants do not upload at once and cross the shared cluster's per-user memory
  limit.
* The hook artifact selection (``_has_data`` / ``_upload_profile_artifacts``):
  no-op for builds without profile data, upload for builds that have it, and
  propagate (not swallow) an upload rejection so lost telemetry stays visible.
* The upload transport (``LogCluster.do_query``): the telemetry INSERT runs
  with parallel parsing disabled so its peak parse memory stays under the
  shared cluster's per-user limit; a transient 5xx rejection (the shared
  cluster's per-user budget is momentarily saturated by unrelated load) is
  retried with backoff and the payload is rewound before each retry, while a
  4xx or a sustained failure is not retried and surfaces loudly.
"""

import os
import subprocess
import sys
from pathlib import Path

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from ci.jobs.scripts.job_hooks.build_profile_hook import (
    _has_data,
    _should_profile,
    _upload_profile_artifacts,
)
from ci.jobs.scripts import log_cluster as log_cluster_mod
from ci.jobs.scripts.log_cluster import LogCluster

_REPO_ROOT = Path(__file__).resolve().parent.parent.parent
_PRODUCER = _REPO_ROOT / "utils" / "prepare-time-trace" / "prepare-time-trace.sh"


def _record_inserts(recorded):
    def insert(build_name, start_time, file):
        recorded.append(str(file))

    return insert


# --- producer: empty build dir must not emit a junk row -------------------


def test_producer_no_objects_leaves_binary_sizes_empty(tmp_path):
    """Cross-arch build: no objects -> binary_sizes.txt empty, not '0\\n'.

    Without ``xargs -r`` GNU xargs runs ``wc -c`` once with no args and writes a
    bare ``0`` row, which fails the binary_sizes FORMAT Regexp and aborts the
    upload. The producer must instead leave the file empty.
    """
    build_dir = tmp_path / "build"
    build_dir.mkdir()
    out_dir = tmp_path / "out"
    out_dir.mkdir()

    subprocess.run(
        ["bash", str(_PRODUCER), str(build_dir), str(out_dir)],
        check=True,
        capture_output=True,
    )

    binary_sizes = out_dir / "binary_sizes.txt"
    assert binary_sizes.exists()
    assert binary_sizes.read_bytes() == b""


# --- build subset gate ----------------------------------------------------


def test_should_profile_only_release_builds():
    """Telemetry is collected only for the explicit release-build subset."""
    assert _should_profile("amd_release")
    assert _should_profile("arm_release")
    assert not _should_profile("amd_debug")
    assert not _should_profile("amd_asan_ubsan")
    assert not _should_profile("arm_tsan")
    assert not _should_profile("amd_darwin")


# --- hook artifact selection ----------------------------------------------


def test_has_data_false_for_missing(tmp_path):
    assert not _has_data(tmp_path / "absent.txt")


def test_has_data_false_for_empty(tmp_path):
    empty = tmp_path / "empty.txt"
    empty.write_text("")
    assert not _has_data(empty)


def test_has_data_true_for_nonempty(tmp_path):
    f = tmp_path / "data.txt"
    f.write_text("x")
    assert _has_data(f)


def test_lto_build_missing_symbols_uploads_rest(tmp_path):
    """LTO build: binary_symbols.txt absent must not abort the upload."""
    profile = tmp_path / "profile.json"
    profile.write_text("[]")
    sizes = tmp_path / "binary_sizes.txt"
    sizes.write_text("1 a.o")
    symbols = tmp_path / "binary_symbols.txt"  # never created for LTO builds

    recorded = []
    insert = _record_inserts(recorded)
    _upload_profile_artifacts(
        "arm_release",
        "2026-06-11 00:00:00",
        [(insert, profile), (insert, sizes), (insert, symbols)],
    )

    assert recorded == [str(profile), str(sizes)]


def test_cross_arch_build_no_data_is_noop(tmp_path):
    """Cross-arch build models the real producer output: empty files, no upload.

    profile.json is absent (the hook never opens what was not produced) and
    binary_sizes.txt / binary_symbols.txt are present-but-empty, exactly what
    ``prepare-time-trace.sh`` (with ``xargs -r``) leaves for a build with no
    readable objects. The hook must no-op without failing.
    """
    sizes = tmp_path / "binary_sizes.txt"
    sizes.write_text("")  # real producer output for no objects, not absent
    symbols = tmp_path / "binary_symbols.txt"
    symbols.write_text("")

    recorded = []
    insert = _record_inserts(recorded)
    _upload_profile_artifacts(
        "arm_release",
        "2026-06-11 00:00:00",
        [
            (insert, tmp_path / "profile.json"),
            (insert, sizes),
            (insert, symbols),
        ],
    )

    assert recorded == []


def test_native_build_uploads_all(tmp_path):
    """Non-LTO native build: all three artifacts present and uploaded."""
    files = []
    for name in ("profile.json", "binary_sizes.txt", "binary_symbols.txt"):
        f = tmp_path / name
        f.write_text("x")
        files.append(f)

    recorded = []
    insert = _record_inserts(recorded)
    _upload_profile_artifacts(
        "amd_release",
        "2026-06-11 00:00:00",
        [(insert, f) for f in files],
    )

    assert recorded == [str(f) for f in files]


def test_upload_rejection_propagates(tmp_path):
    """An upload rejection must NOT be swallowed: it propagates to the caller.

    With the upload restricted to a small build subset the per-user-limit
    contention is gone, so a genuine INSERT rejection now means a real lost
    upload. It must surface (the hook fails loudly) rather than be reported as
    success.
    """
    f = tmp_path / "binary_sizes.txt"
    f.write_text("1 a.o")

    def failing_insert(build_name, start_time, file):
        raise AssertionError("upload rejected")

    with pytest.raises(AssertionError):
        _upload_profile_artifacts(
            "amd_release", "2026-06-11 00:00:00", [(failing_insert, f)]
        )


# --- upload transport: parallel parsing disabled --------------------------


def test_do_query_disables_parallel_parsing():
    """The telemetry INSERT must run with parallel parsing off.

    Parallel parsing buffers many chunks at once; that peak is what crosses the
    shared cluster's per-user memory limit when all Build variants upload at
    once (Code 241 MEMORY_LIMIT_EXCEEDED While executing
    ParallelParsingBlockInputFormat). do_query must send
    input_format_parallel_parsing=0 so the INSERT is accepted and the telemetry
    is kept.
    """

    class _Resp:
        ok = True

    class _Session:
        def __init__(self):
            self.params = None

        def post(self, url, params, data, headers, timeout):
            self.params = params
            return _Resp()

    cluster = LogCluster()
    cluster.is_ready = lambda: True
    cluster.url = "https://example"
    cluster._auth = {}
    cluster._session = _Session()

    assert cluster.do_query("INSERT INTO t FORMAT JSONEachRow", data=b"")
    assert cluster._session.params["input_format_parallel_parsing"] == 0


# --- upload transport: transient 5xx is retried, loud on exhaustion -------


class _Resp:
    def __init__(self, status_code):
        self.status_code = status_code
        self.ok = status_code < 400
        self.text = f"code {status_code}"


class _SequenceSession:
    """A session that returns a preset sequence of responses, one per POST."""

    def __init__(self, statuses):
        self._statuses = list(statuses)
        self.calls = 0

    def post(self, url, params, data, headers, timeout):
        self.calls += 1
        return _Resp(self._statuses.pop(0))


def _make_cluster(session):
    cluster = LogCluster()
    cluster.is_ready = lambda: True
    cluster.url = "https://example"
    cluster._auth = {}
    cluster._session = session
    return cluster


@pytest.fixture
def no_sleep(monkeypatch):
    """Make the backoff instant so retry tests do not actually wait."""
    monkeypatch.setattr(log_cluster_mod.time, "sleep", lambda _s: None)


def test_do_query_retries_transient_5xx_then_succeeds(no_sleep):
    """A transient 5xx (per-user budget momentarily saturated) is retried.

    The real failure is HTTP 500 / Code 241 MEMORY_LIMIT_EXCEEDED on a tiny
    (~5 MiB) parse chunk: the shared "ci"-user budget is transiently full from
    unrelated load, not from this upload. do_query must retry and succeed once a
    window opens, keeping the telemetry.
    """
    session = _SequenceSession([500, 503, 200])
    cluster = _make_cluster(session)

    assert cluster.do_query("INSERT INTO t FORMAT JSONEachRow", data=b"")
    assert session.calls == 3


def test_do_query_fails_loudly_after_exhausting_retries(no_sleep):
    """A sustained 5xx is not swallowed: do_query returns False after retries.

    Riding out a transient window is fine, but a persistently saturated cluster
    must still fail so the check goes red rather than reporting a lost upload as
    success.
    """
    session = _SequenceSession([500] * log_cluster_mod._UPLOAD_RETRIES)
    cluster = _make_cluster(session)

    assert not cluster.do_query("INSERT INTO t FORMAT JSONEachRow", data=b"")
    assert session.calls == log_cluster_mod._UPLOAD_RETRIES


def test_do_query_does_not_retry_4xx(no_sleep):
    """A 4xx (e.g. a malformed query) is a real error, not transient: fail fast."""
    session = _SequenceSession([400, 200])
    cluster = _make_cluster(session)

    assert not cluster.do_query("INSERT INTO t FORMAT JSONEachRow", data=b"")
    assert session.calls == 1  # no retry after a 4xx


def test_do_query_rewinds_file_payload_between_retries(no_sleep):
    """The INSERT payload is a file object: it must be rewound before each retry.

    Without the seek(0), the first attempt consumes the file and a retry uploads
    an empty body, which silently loses the telemetry it was meant to preserve.
    """
    import io

    payload = io.BytesIO(b"row-data")
    bodies_seen = []

    class _RecordingSession:
        def __init__(self):
            self.calls = 0

        def post(self, url, params, data, headers, timeout):
            self.calls += 1
            bodies_seen.append(data.read())
            return _Resp(500 if self.calls == 1 else 200)

    cluster = _make_cluster(_RecordingSession())

    assert cluster.do_query("INSERT INTO t FORMAT JSONEachRow", data=payload)
    # Both attempts saw the full payload, not an empty body on the retry.
    assert bodies_seen == [b"row-data", b"row-data"]
