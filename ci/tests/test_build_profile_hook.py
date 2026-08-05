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
* The upload order (``_UPLOAD_ORDER``): ``binary_sizes.txt`` is uploaded last,
  because the "Build profile diff" check treats a ``binary_sizes`` row as the
  marker of a complete profile when it picks its master baseline. A build that
  stops halfway must not leave that marker behind.
* The upload transport (``LogCluster.do_query``): the telemetry INSERT runs
  with parallel parsing disabled so its peak parse memory stays under the
  shared cluster's per-user limit.
* The endpoint routing (``LogCluster.READONLY_URL``): the consumer reads
  through the read-only sub-service of the cluster and sends no settings there,
  while the uploads keep the writer endpoint.
"""

import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from ci.jobs.scripts.job_hooks.build_profile_hook import (
    _UPLOAD_ORDER,
    _has_data,
    _should_profile,
    _upload_profile_artifacts,
    _verify_final_binary_coverage,
    _verify_trace_extraction,
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


def test_producer_fails_when_a_trace_file_cannot_be_parsed(tmp_path):
    """A failing ``jq`` child must fail the whole producer, not be swallowed.

    The script used to run without ``set -e`` / ``pipefail``, so a broken trace
    file only made one ``xargs`` child exit non-zero while the script kept going
    and still exited 0. The hook then uploaded a truncated profile and the
    "Build profile diff" check reported the missing translation units as new or
    absent instead of failing loudly.
    """
    build_dir = tmp_path / "build"
    build_dir.mkdir()
    (build_dir / "good.cpp.json").write_text(
        '{"beginningOfTime": 1, "traceEvents": [{"pid": 1, "tid": 1, "ph": "X", "ts": 0, "dur": 1, "cat": "", "name": "ExecuteCompiler", "args": {}}]}'
    )
    (build_dir / "broken.cpp.json").write_text("{ this is not json")
    out_dir = tmp_path / "out"
    out_dir.mkdir()

    result = subprocess.run(
        ["bash", str(_PRODUCER), str(build_dir), str(out_dir)],
        capture_output=True,
    )
    assert result.returncode != 0


@pytest.mark.skipif(not shutil.which("cc"), reason="no C compiler available")
def test_producer_fails_when_nm_cannot_read_an_object(tmp_path):
    """A failing ``nm`` child must fail the producer too.

    An object whose symbol table cannot be read would otherwise silently drop
    out of ``binary_symbols.txt``, and the consumer would report every symbol of
    that translation unit as removed.
    """
    build_dir = tmp_path / "build"
    (build_dir / "programs").mkdir(parents=True)
    # No -flto: the per-object nm pass runs.
    (build_dir / "compile_commands.json").write_text('[{"command": "cc -O2 x.c"}]')
    src = tmp_path / "main.c"
    src.write_text("int main(void) { return 0; }\n")
    subprocess.run(
        ["cc", "-c", "-o", str(build_dir / "good.o"), str(src)],
        check=True,
        capture_output=True,
    )
    (build_dir / "not-an-object.o").write_bytes(b"garbage, definitely not ELF\n")
    out_dir = tmp_path / "out"
    out_dir.mkdir()

    result = subprocess.run(
        ["bash", str(_PRODUCER), str(build_dir), str(out_dir)],
        capture_output=True,
    )
    assert result.returncode != 0


@pytest.mark.skipif(not shutil.which("cc"), reason="no C compiler available")
def test_producer_accepts_an_object_without_reportable_symbols(tmp_path):
    """``grep`` selecting nothing is not an extraction error.

    An object that defines no reportable symbol leaves ``grep -v`` with nothing
    to print, so it exits 1; fail-close must not turn that into a failure of the
    build - only a failing ``nm``/``jq`` may.
    """
    build_dir = tmp_path / "build"
    build_dir.mkdir()
    (build_dir / "compile_commands.json").write_text('[{"command": "cc -O2 x.c"}]')
    src = tmp_path / "empty.c"
    src.write_text("typedef int nothing_defined_here;\n")
    subprocess.run(
        ["cc", "-c", "-o", str(build_dir / "empty.o"), str(src)],
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
    assert (out_dir / "binary_symbols.txt").exists()


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
    # ... and the completion marker must not have been written: with
    # binary_sizes.txt uploaded the broken build would have become the
    # "Build profile diff" master baseline (see _UPLOAD_ORDER).
    assert str(sizes) not in recorded

    # Present-but-empty is the same producer failure as absent.
    symbols.write_text("")
    recorded.clear()
    with pytest.raises(RuntimeError, match="required"):
        _upload_profile_artifacts(
            "arm_release",
            "2026-06-11 00:00:00",
            [(insert, profile), (insert, sizes), (insert, symbols)],
        )
    assert str(sizes) not in recorded


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
    """Non-LTO native build: all three artifacts present and uploaded.

    The order is the hook's, not the caller's: binary_sizes.txt goes last
    (_UPLOAD_ORDER), even though it is passed in the middle here.
    """
    files = {}
    for name in ("profile.json", "binary_sizes.txt", "binary_symbols.txt"):
        f = tmp_path / name
        f.write_text("x")
        files[name] = f

    recorded = []
    insert = _record_inserts(recorded)
    _upload_profile_artifacts(
        "amd_release",
        "2026-06-11 00:00:00",
        [
            (insert, files["profile.json"]),
            (insert, files["binary_sizes.txt"]),
            (insert, files["binary_symbols.txt"]),
        ],
    )

    assert recorded == [str(files[name]) for name in _UPLOAD_ORDER]
    assert recorded[-1] == str(files["binary_sizes.txt"])


def test_upload_order_puts_the_completion_marker_last():
    """binary_sizes.txt is the last upload, and the only completion marker.

    `find_baseline` / `find_warmup_baseline` in
    ci/jobs/build_profile_diff_job.py select the master baseline by the
    presence of a `binary_sizes` row. That only means "the profile of this
    commit is complete" while binary_sizes.txt is written after everything
    else, so this ordering is a contract between the two files, not a detail.
    """
    assert _UPLOAD_ORDER[-1] == "binary_sizes.txt"
    assert set(_UPLOAD_ORDER) == {
        "profile.json",
        "binary_sizes.txt",
        "binary_symbols.txt",
    }


def test_upload_order_rejects_an_unplaced_artifact(tmp_path):
    """An artifact with no place in _UPLOAD_ORDER raises instead of trailing it.

    Appending an unknown artifact after the completion marker would recreate
    exactly the hole the ordering closes, so a new artifact must be placed
    explicitly.
    """
    f = tmp_path / "binary_hashes.txt"
    f.write_text("x")

    recorded = []
    insert = _record_inserts(recorded)
    with pytest.raises(RuntimeError, match="upload order"):
        _upload_profile_artifacts(
            "amd_release", "2026-06-11 00:00:00", [(insert, f)]
        )
    assert recorded == []


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


# --- trace extraction verification ------------------------------------------


def test_trace_extraction_fails_when_raw_traces_yield_nothing(tmp_path):
    """Raw -ftime-trace files present + empty profile.json = broken extractor.

    The warmup build's time trace cannot be unconditionally required: when
    every TU is an sccache hit it is legitimately empty. But the compiler
    leaves one raw trace file per TU it actually ran on, so raw traces with
    nothing extracted means prepare-time-trace.sh regressed - and silently
    uploading would let the "Build profile diff" per-TU compile-time section
    lose its baseline without any signal.
    """
    build_dir = tmp_path / "build"
    (build_dir / "src" / "CMakeFiles" / "dbms.dir").mkdir(parents=True)
    (build_dir / "src" / "CMakeFiles" / "dbms.dir" / "foo.cpp.json").write_text("{}")
    profile = tmp_path / "profile.json"
    profile.write_text("")

    with pytest.raises(RuntimeError, match="extraction"):
        _verify_trace_extraction(build_dir, profile)

    # A link trace counts as a raw trace too.
    for f in build_dir.rglob("*.json"):
        f.unlink()
    (build_dir / "programs").mkdir()
    (build_dir / "programs" / "clickhouse.time-trace").write_text("{}")
    with pytest.raises(RuntimeError, match="extraction"):
        _verify_trace_extraction(build_dir, profile)


def test_trace_extraction_accepts_all_cache_hit_build(tmp_path):
    """No raw traces (every TU an sccache hit) -> an empty profile is fine.

    Non-trace JSON files in the build tree (compile_commands.json, cmake
    artifacts) must not be mistaken for compiler trace output.
    """
    build_dir = tmp_path / "build"
    build_dir.mkdir()
    (build_dir / "compile_commands.json").write_text("[]")
    profile = tmp_path / "profile.json"
    profile.write_text("")

    _verify_trace_extraction(build_dir, profile)


def test_trace_extraction_accepts_extracted_profile(tmp_path):
    """Raw traces present and extraction produced data: nothing to flag."""
    build_dir = tmp_path / "build"
    build_dir.mkdir()
    (build_dir / "foo.cpp.json").write_text("{}")
    profile = tmp_path / "profile.json"
    profile.write_text('["row"]')

    _verify_trace_extraction(build_dir, profile)


# --- final binary coverage verification ------------------------------------


def _coverage_fixture(tmp_path, keeper_symlink=False):
    """A build dir with the final and stripped binaries and covering artifacts."""
    build_dir = tmp_path / "build"
    (build_dir / "programs").mkdir(parents=True)
    (build_dir / "programs" / "clickhouse").write_bytes(b"\x7fELF")
    (build_dir / "programs" / "clickhouse-stripped").write_bytes(b"\x7fELF")
    if keeper_symlink:
        (build_dir / "programs" / "clickhouse-keeper").symlink_to("clickhouse")
    else:
        (build_dir / "programs" / "clickhouse-keeper").write_bytes(b"\x7fELF")
    main = f"{build_dir}/programs/clickhouse"
    keeper = f"{build_dir}/programs/clickhouse-keeper"
    stripped = f"{build_dir}/programs/clickhouse-stripped"
    sizes = tmp_path / "binary_sizes.txt"
    sizes.write_text(f"4 {main}\n4 {keeper}\n4 {stripped}\n")
    symbols = tmp_path / "binary_symbols.txt"
    # The stripped binary legitimately has no symbol rows.
    symbols.write_text(f"{main} 0 16 t main\n{keeper} 0 16 t keeper_main\n")
    return build_dir, sizes, symbols


def test_final_binary_coverage_fails_when_one_binary_lost(tmp_path):
    """A linked binary with no rows in a non-empty artifact fails the hook.

    _REQUIRED_ARTIFACTS only guards whole files: nm silently failing on the
    standalone keeper leaves binary_symbols.txt non-empty (the main binary's
    rows are there) while "Build profile diff" would silently omit the keeper
    from the symbol and ThinLTO sections - a keeper-only regression turned
    false-green. The binary on disk is the ground truth for what must appear.
    """
    build_dir, sizes, symbols = _coverage_fixture(tmp_path)
    symbols.write_text(f"{build_dir}/programs/clickhouse 0 16 t main\n")
    with pytest.raises(RuntimeError, match="clickhouse-keeper"):
        _verify_final_binary_coverage(str(build_dir), sizes, symbols)

    # The size rows are verified the same way.
    build_dir, sizes, symbols = _coverage_fixture(tmp_path / "second")
    sizes.write_text(f"4 {build_dir}/programs/clickhouse-keeper\n")
    with pytest.raises(RuntimeError, match="programs/clickhouse]"):
        _verify_final_binary_coverage(str(build_dir), sizes, symbols)


def test_final_binary_coverage_accepts_complete_artifacts(tmp_path):
    """Both binaries covered by both artifacts: nothing to flag."""
    build_dir, sizes, symbols = _coverage_fixture(tmp_path)
    _verify_final_binary_coverage(str(build_dir), sizes, symbols)


def test_final_binary_coverage_skips_symlinked_keeper(tmp_path):
    """A symlinked keeper is legitimately absent from the artifacts.

    When the keeper is not built standalone the producer skips it (its
    symbols would duplicate the main binary's), so only the real files on
    disk set the expectation.
    """
    build_dir, sizes, symbols = _coverage_fixture(tmp_path, keeper_symlink=True)
    main = f"{build_dir}/programs/clickhouse"
    stripped = f"{build_dir}/programs/clickhouse-stripped"
    sizes.write_text(f"4 {main}\n4 {stripped}\n")
    symbols.write_text(f"{main} 0 16 t main\n")
    _verify_final_binary_coverage(str(build_dir), sizes, symbols)


def test_final_binary_coverage_requires_stripped_size_row(tmp_path):
    """Losing only the stripped binary's size row fails the hook.

    The stripped binary is the headline size comparison's only input
    (HEADLINE_BINARIES in build_profile_diff_job.py), while the run itself is
    resolved via the main binary's rows - so a producer that loses just this
    one row would leave the check running with an empty headline section, a
    size regression turned false-green. It has no symbols by construction,
    so only its size row is demanded.
    """
    build_dir, sizes, symbols = _coverage_fixture(tmp_path)
    main = f"{build_dir}/programs/clickhouse"
    keeper = f"{build_dir}/programs/clickhouse-keeper"
    sizes.write_text(f"4 {main}\n4 {keeper}\n")
    with pytest.raises(RuntimeError, match="clickhouse-stripped"):
        _verify_final_binary_coverage(str(build_dir), sizes, symbols)

    # A build that did not produce the stripped binary at all owes no row.
    build_dir, sizes, symbols = _coverage_fixture(tmp_path / "second")
    (build_dir / "programs" / "clickhouse-stripped").unlink()
    sizes.write_text(f"4 {build_dir}/programs/clickhouse\n4 {build_dir}/programs/clickhouse-keeper\n")
    _verify_final_binary_coverage(str(build_dir), sizes, symbols)


def test_final_binary_coverage_leaves_empty_artifacts_to_required_check(tmp_path):
    """Whole-file absence stays _REQUIRED_ARTIFACTS' report, not this one.

    The warmup build has no binaries at all, and a cross-arch build has empty
    artifacts; both are judged by _REQUIRED_ARTIFACTS with its canonical
    message. This check only catches a *partial* loss inside a produced file.
    """
    build_dir, sizes, symbols = _coverage_fixture(tmp_path)
    sizes.write_text("")
    symbols.unlink()
    _verify_final_binary_coverage(str(build_dir), sizes, symbols)


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


# --- endpoint routing: reads on the read-only sub-service -----------------


def test_reader_uses_the_read_only_endpoint(monkeypatch):
    """The diff consumer must read through the read-only sub-service.

    Its queries scan months of the whole fleet's build telemetry; running them
    on the endpoint that ingests every job's logs and profiles is what makes
    the shared cluster's memory-pressure spikes (Code 241) hit the uploads.
    """
    monkeypatch.delenv("CI_LOGS_HOST", raising=False)
    from ci.jobs.build_profile_diff_job import Db

    cluster = Db()._cluster
    assert cluster.readonly
    assert cluster.url == LogCluster.READONLY_URL


def test_do_query_refuses_the_read_only_endpoint():
    """The telemetry INSERT must never be routed to the read-only endpoint,
    which cannot serve it - the data would be lost instead of uploaded."""
    cluster = LogCluster(readonly=True)
    cluster.is_ready = lambda: True
    with pytest.raises(AssertionError):
        cluster.do_query("INSERT INTO t FORMAT JSONEachRow", data=b"")


def test_select_sends_no_settings_to_the_read_only_endpoint():
    """A read-only profile rejects a query that changes any setting, so the
    read path must not send the `send_logs_level` of the upload path."""

    class _Resp:
        ok = True
        text = "[]"

    class _Session:
        def __init__(self):
            self.params = None

        def post(self, url, params, data, headers, timeout):
            self.params = params
            return _Resp()

    cluster = LogCluster(readonly=True)
    cluster.is_ready = lambda: True
    cluster._auth = {}
    cluster._session = _Session()

    assert cluster.select("SELECT 1 FORMAT JSON") == "[]"
    assert cluster._session.params == {}
