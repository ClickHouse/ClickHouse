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
  fail-close on a missing artifact the build type is required to produce
  (``_REQUIRED_ARTIFACTS``), no-op for builds legitimately without profile
  data, upload for builds that have it, and propagate (not swallow) an upload
  rejection so lost telemetry stays visible.
* The upload transport (``LogCluster.do_query``): the telemetry INSERT runs
  with parallel parsing disabled so its peak parse memory stays under the
  shared cluster's per-user limit.
"""

import os
import shutil
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
from ci.jobs.scripts.log_cluster import LogCluster, LogClusterBuildProfileQueries

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


@pytest.mark.skipif(not shutil.which("cc"), reason="no C compiler available")
def test_producer_collects_final_binary_symbols_for_lto(tmp_path):
    """LTO build: nm on .o is skipped, but the final binaries still get symbols.

    (Thin)LTO release builds have no per-object symbol data (nm does not work
    on LTO objects), which used to leave binary_symbols.txt absent entirely.
    The producer must still collect the symbol table of the final linked
    binaries - that is what the "Build profile diff" check uses for per-symbol
    size attribution.
    """
    build_dir = tmp_path / "build"
    (build_dir / "programs").mkdir(parents=True)
    (build_dir / "compile_commands.json").write_text('[{"command": "cc -flto x.c"}]')
    src = tmp_path / "main.c"
    src.write_text(
        "int please_find_me(int x) { return x + 1; }\n"
        "int main(void) { return please_find_me(0); }\n"
    )
    subprocess.run(
        ["cc", "-o", str(build_dir / "programs" / "clickhouse"), str(src)],
        check=True,
        capture_output=True,
    )
    out_dir = tmp_path / "out"
    out_dir.mkdir()

    subprocess.run(
        ["bash", str(_PRODUCER), str(build_dir), str(out_dir)],
        check=True,
        capture_output=True,
    )

    symbols_file = out_dir / "binary_symbols.txt"
    assert symbols_file.exists()
    symbols = symbols_file.read_text()
    assert "please_find_me" in symbols
    # Rows are attributed to the binary they come from.
    assert f"{build_dir}/programs/clickhouse " in symbols


# --- build subset gate ----------------------------------------------------


def test_should_profile_only_release_builds():
    """Telemetry is collected only for the explicit release-build subset."""
    assert _should_profile("amd_release")
    assert _should_profile("arm_release")
    # The master warmup build is profiled too: it compiles with the PR flags
    # and is the object-size/compile-time baseline of "Build profile diff".
    assert _should_profile("arm_release_pr_cache_warmup")
    assert not _should_profile("amd_release_pr_cache_warmup")
    assert not _should_profile("amd_debug")
    assert not _should_profile("amd_asan_ubsan")
    assert not _should_profile("arm_tsan")
    assert not _should_profile("amd_darwin")


def test_should_profile_pr_only_arm_release():
    """PR builds are far more frequent than master pushes, so on PRs only the
    aarch64 release build is profiled - the one the "Build profile diff" check
    compares against master."""
    assert _should_profile("arm_release", is_pr=True)
    assert not _should_profile("amd_release", is_pr=True)
    assert not _should_profile("arm_release_pr_cache_warmup", is_pr=True)
    assert not _should_profile("amd_debug", is_pr=True)


def _profile_queries():
    """LogClusterBuildProfileQueries without touching Info() / the environment."""
    queries = LogClusterBuildProfileQueries.__new__(LogClusterBuildProfileQueries)

    class _FakeInfo:
        pr_number = 12345
        sha = "deadbeef"
        instance_type = "t"
        instance_id = "i"

    queries._info = _FakeInfo()
    return queries


def test_profile_query_reduced_filters_events():
    """PR uploads carry only the event kinds the diff check consumes."""
    queries = _profile_queries()
    query = queries._profile_query("arm_release", "2026-06-11 00:00:00", reduced=True)
    assert "WHERE name IN (" in query
    assert "'ExecuteCompiler'" in query
    assert "'OptFunction'" in query
    assert "OR name LIKE 'Total %'" in query
    # The filter must precede the input format clause to apply to input rows.
    assert query.index("WHERE name IN") < query.index("FORMAT JSONCompactEachRow")


def test_profile_query_full_by_default():
    """Master uploads keep the full trace: no event filter."""
    queries = _profile_queries()
    query = queries._profile_query("arm_release", "2026-06-11 00:00:00")
    assert "WHERE name IN" not in query


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


def test_release_build_missing_required_artifact_fails_close(tmp_path):
    """A release build with a missing required artifact must fail loudly.

    The linked release builds always produce all three artifacts (the link
    trace always exists, nm of the linked binary works under LTO). A missing
    or empty one means the producer regressed - prepare-time-trace.sh broke,
    the build layout changed, nm disappeared from the image. Skipping it
    silently would keep `Build (arm_release)` green and let "Build profile
    diff" pass on its "no data" path: a broken producer turned false-green.
    """
    profile = tmp_path / "profile.json"
    profile.write_text("[]")
    sizes = tmp_path / "binary_sizes.txt"
    sizes.write_text("1 a.o")
    symbols = tmp_path / "binary_symbols.txt"  # missing

    recorded = []
    insert = _record_inserts(recorded)
    with pytest.raises(RuntimeError, match="required"):
        _upload_profile_artifacts(
            "arm_release",
            "2026-06-11 00:00:00",
            [(insert, profile), (insert, sizes), (insert, symbols)],
        )

    # Present-but-empty is the same producer failure as absent.
    symbols.write_text("")
    with pytest.raises(RuntimeError, match="required"):
        _upload_profile_artifacts(
            "arm_release",
            "2026-06-11 00:00:00",
            [(insert, profile), (insert, sizes), (insert, symbols)],
        )


def test_warmup_build_uploads_sizes_and_skips_optional(tmp_path):
    """The warmup build links nothing: sizes are required, the rest optional.

    On a master push where every TU is an sccache hit even the time trace is
    legitimately empty, and there is never a linked binary to take symbols
    from - only binary_sizes.txt (the object files) must always be there.
    """
    sizes = tmp_path / "binary_sizes.txt"
    sizes.write_text("1 a.o")
    symbols = tmp_path / "binary_symbols.txt"
    symbols.write_text("")

    recorded = []
    insert = _record_inserts(recorded)
    _upload_profile_artifacts(
        "arm_release_pr_cache_warmup",
        "2026-06-11 00:00:00",
        [
            (insert, tmp_path / "profile.json"),
            (insert, sizes),
            (insert, symbols),
        ],
    )
    assert recorded == [str(sizes)]

    # ... but the object sizes themselves are required: they are the "Build
    # profile diff" object-size baseline.
    sizes.unlink()
    with pytest.raises(RuntimeError, match="required"):
        _upload_profile_artifacts(
            "arm_release_pr_cache_warmup",
            "2026-06-11 00:00:00",
            [(insert, sizes)],
        )


def test_unlisted_build_no_data_is_noop(tmp_path):
    """A build with no required artifacts (cross-arch/non-Linux) no-ops.

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
        "amd_darwin",
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
