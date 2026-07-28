"""
Regression tests for the post-fuzz memory-stuck classification in
ci/jobs/ast_fuzzer_job.py and the self-kill filter in
ci/jobs/scripts/log_parser.py (ClickHouse/ClickHouse#110074).

A 30/60m AST-fuzzer run can end with the server pinned above its memory cap and
growing while idle; every post-fuzz probe is rejected with the server-global
"(total) memory limit exceeded" tracker error and reclaim never comes. Left
alone the job drifts into the external cancellation ceiling with no artifacts
(status.tsv never written), so the offending query is unattributable. run-fuzzer.sh
now detects that, SIGKILLs the server itself, and writes a marker; a reap/teardown
watchdog records other bounded escalations. These tests pin the Python side:

  - the marker/watchdog helpers build the right Result,
  - ALL sanitized-build leniency (kernel-OOM and the sanitizer-OOM grep) is
    disabled when a marker or watchdog is present (otherwise a marker run would
    be "considered passed" on asan/tsan/msan) -- via the _oom_leniency_granted
    production helper tested here,
  - a benign exit code (137 from a reap-abandon) can never ride the OK branch
    when a watchdog fired,
  - the log parser runs ONLY when the server actually died, so a reap-stage
    watchdog on a healthy, gracefully-stopped server does not scrape the
    normal-termination signal-15 line (_should_parse_logs),
  - the parser's self_killed_server flag filters ONLY the self-inflicted signal
    lines: a genuine earlier finding still wins, and so does a genuine fatal
    signal on the way down (a fatally-signaled server is exactly a server that
    stops answering the probes that set this flag),
  - pre-run hygiene deletes stale classification inputs but keeps the
    pre-container survivors, and fails closed if one cannot be removed.

The ownership-repair skip shares the marker/watchdog predicate tested here.
"""

import inspect
import os
import signal
import sys
import textwrap
import time

import pytest

from ci.praktika.utils import Shell

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

import ci.jobs.ast_fuzzer_job as job
from ci.jobs.scripts.log_parser import FuzzerLogParser
from ci.praktika.result import Result


@pytest.fixture
def workspace(tmp_path, monkeypatch):
    """Point the module's marker/workspace paths at a temp dir."""
    ws = tmp_path / "workspace"
    ws.mkdir()
    monkeypatch.setattr(job, "WORKSPACE_PATH", ws)
    monkeypatch.setattr(job, "MEMORY_STUCK_MARKER", ws / "server_memory_stuck.txt")
    monkeypatch.setattr(job, "HARNESS_WATCHDOG", ws / "harness_watchdog.txt")
    monkeypatch.setattr(
        job,
        "_STALE_RUN_STATE",
        (
            ws / "server_memory_stuck.txt",
            ws / "harness_watchdog.txt",
            ws / "status.tsv",
            ws / "server.log",
            ws / "stderr.log",
            ws / "fuzzer.log",
            ws / "fuzzerout.sql",
            ws / "fatal.log",
            ws / "dmesg.log",
        ),
    )
    return ws


# --------------------------------------------------------------------------- #
# structural-pin helpers (shared by the ordering / early-abort pins below)
# --------------------------------------------------------------------------- #


def assert_statically_reachable(func, stmt, what):
    """Fail if `stmt`'s line was dropped from `func`'s compiled bytecode.

    The pins below are structural: they compare statements inside a block, which
    is only meaningful if the block runs at all. Wrapping a whole
    persist/collect/attach sequence in a dead branch otherwise satisfies every
    relationship while doing none of it.

    Reachability is asked of the CPython compiler rather than re-derived here:
    `code.co_lines` reports the lines the compiler actually emitted, so every
    form of statically dead code it eliminates (`if False:`, the `else` of an
    `if True:`, anything after an unconditional `return`/`raise`) is caught by
    one check, including forms not enumerated when this was written. A
    pattern-matching version can only reject the shapes it lists.

    `run_fuzz_job` cannot be executed here (it drives docker and the real
    runner), which is why this static-but-compiler-backed check is the strongest
    liveness signal available to these pins.
    """
    code = func.__code__
    emitted = {line for _, _, line in code.co_lines() if line}
    body_lines = set(range(stmt.lineno, (stmt.end_lineno or stmt.lineno) + 1))
    # Line numbers in the parsed tree are relative to the `inspect.getsource`
    # snippet, so shift the emitted set into the same frame of reference.
    offset = code.co_firstlineno - 1
    emitted_relative = {line - offset for line in emitted}
    assert body_lines & emitted_relative, (
        f"{what} at line {stmt.lineno} was eliminated from the compiled "
        f"bytecode of {func.__name__}: it is statically unreachable, so the "
        "ordering it pins would hold while nothing executes"
    )


# --------------------------------------------------------------------------- #
# marker / watchdog helpers
# --------------------------------------------------------------------------- #


def test_memory_stuck_result_present(workspace):
    (workspace / "server_memory_stuck.txt").write_text(
        "probes=60 tier=patient MemAvailable_kB=41943040\n"
        "Code: 241. DB::Exception: (total) memory limit exceeded: ...\n",
        encoding="utf-8",
    )
    res = job._memory_stuck_result()
    assert res is not None
    assert res.name == "Server unresponsive: memory limit exceeded"
    assert res.status == Result.Status.FAIL
    assert "tier=patient" in res.info
    assert "(total) memory limit exceeded" in res.info


def test_memory_stuck_result_absent(workspace):
    assert job._memory_stuck_result() is None


def test_harness_watchdog_result_present(workspace):
    (workspace / "harness_watchdog.txt").write_text(
        "stage=teardown reason=graceful_stop_hung waited=180s\n", encoding="utf-8"
    )
    res = job._harness_watchdog_result()
    assert res is not None
    assert res.name == "Fuzzer harness watchdog fired"
    assert res.status == Result.Status.ERROR
    assert "stage=teardown" in res.info


def test_harness_watchdog_result_absent(workspace):
    assert job._harness_watchdog_result() is None


def test_watchdog_stage_teardown_discriminates(workspace):
    wd = workspace / "harness_watchdog.txt"
    wd.write_text("stage=reap reason=client_unreapable waited=390s\n", encoding="utf-8")
    assert job._watchdog_stage_teardown() is False
    wd.write_text(
        "stage=teardown reason=graceful_stop_hung waited=180s\n", encoding="utf-8"
    )
    assert job._watchdog_stage_teardown() is True


def test_watchdog_stage_probes_discriminates(workspace):
    wd = workspace / "harness_watchdog.txt"
    # Absent file -> False.
    assert job._watchdog_stage_probes() is False
    # A teardown-stage line alone is not a probes exhaustion.
    wd.write_text(
        "stage=teardown reason=graceful_stop_hung waited=180s\n", encoding="utf-8"
    )
    assert job._watchdog_stage_probes() is False
    # A probes-stage line -> True.
    wd.write_text(
        "stage=probes reason=zero_answered_probes memory_limit_probes=7\n",
        encoding="utf-8",
    )
    assert job._watchdog_stage_probes() is True
    # A probes line appended AFTER a reap line (both writers append) -> True.
    wd.write_text(
        "stage=reap reason=client_unreapable waited=390s\n"
        "stage=probes reason=zero_answered_probes memory_limit_probes=3\n",
        encoding="utf-8",
    )
    assert job._watchdog_stage_probes() is True
    # The persistent-probe-timeouts exit writes the same stage=probes tag -> True.
    wd.write_text(
        "stage=probes reason=persistent_probe_timeouts timeouts=12\n",
        encoding="utf-8",
    )
    assert job._watchdog_stage_probes() is True


# --------------------------------------------------------------------------- #
# leniency gating (§3g): marker/watchdog present -> no forgiveness
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize("marker_present", [True, False])
@pytest.mark.parametrize("watchdog_present", [True, False])
@pytest.mark.parametrize(
    "sanitizer_oom,kernel_oom_kill",
    [
        ("Child process was terminated by signal 9", False),  # watchdog signal-9 line
        ("AddressSanitizer: failed to allocate 0x... bytes", False),  # benign warning
        ("", True),  # kernel OOM kill (137 + no report)
    ],
)
def test_leniency_only_when_no_marker_and_no_watchdog(
    marker_present, watchdog_present, sanitizer_oom, kernel_oom_kill
):
    # Exercise the PRODUCTION predicate directly so a refactor that drops the
    # marker/watchdog gate is caught by this suite.
    marker = object() if marker_present else None
    watchdog = object() if watchdog_present else None
    granted = job._oom_leniency_granted(
        sanitizer_oom, kernel_oom_kill, marker, watchdog
    )
    if marker_present or watchdog_present:
        assert granted is False  # marker/watchdog run is NEVER forgiven
    else:
        assert granted is True  # today's behavior preserved


# --------------------------------------------------------------------------- #
# parser gate (item 1): scrape logs ONLY when the server actually died
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize("is_failed", [True, False])
@pytest.mark.parametrize("status", [Result.Status.FAIL, Result.Status.ERROR])
@pytest.mark.parametrize("server_died", [True, False])
def test_should_parse_logs_truth_table(is_failed, status, server_died):
    # Parse iff the run failed, is not an ERROR (dmesg-OOM), and the server died.
    expected = is_failed and status != Result.Status.ERROR and server_died
    assert job._should_parse_logs(is_failed, status, server_died) is expected


def test_should_parse_logs_watchdog_only_run_skips_parser():
    # The reap-stage-watchdog-on-a-healthy-server case: _force_fail_for_markers
    # flips OK->FAIL with server_died=0. Gating on server_died keeps the parser
    # from scraping the graceful "Received signal 15" line -> no bogus Signal row.
    assert job._should_parse_logs(True, Result.Status.FAIL, False) is False


# --------------------------------------------------------------------------- #
# parser self_killed_server flag (§3f, r12): filter ONLY the self-inflicted signal
# --------------------------------------------------------------------------- #


def test_self_killed_server_filters_signal_only(tmp_path):
    # ONLY a self-inflicted signal line -> with the flag, the Signal pattern is
    # skipped and the parser falls through to UNKNOWN_ERROR (no bogus signal row).
    server_log = tmp_path / "server.log"
    server_log.write_text(
        "2026.07.25 00:00:00.000000 [ 1 ] {} <Fatal> BaseDaemon: "
        "Child process was terminated by signal 9 (KILL)\n",
        encoding="utf-8",
    )
    filtered = FuzzerLogParser(
        server_log=str(server_log),
        stderr_log="",
        fuzzer_log="",
        self_killed_server=True,
    ).parse_failure()
    assert filtered[0] == FuzzerLogParser.UNKNOWN_ERROR

    # Default flag: the "Signal" pattern matches the "terminated by signal 9"
    # line, so the row name is built from that line (today's behavior).
    default = FuzzerLogParser(
        server_log=str(server_log), stderr_log="", fuzzer_log=""
    ).parse_failure()
    assert "terminated by signal 9" in default[0]
    assert default[0] != FuzzerLogParser.UNKNOWN_ERROR


def test_self_killed_server_keeps_genuine_logical_error(tmp_path):
    # A genuine logical error ABOVE a self-inflicted signal line must still win:
    # the flag filters the signal, not the higher-priority logical error.
    server_log = tmp_path / "server.log"
    server_log.write_text(
        "2026.07.25 00:00:00.000000 [ 1 ] {} <Fatal> : Logical error: "
        "'Bad cast from A to B'.\n"
        "2026.07.25 00:00:05.000000 [ 1 ] {} <Fatal> BaseDaemon: "
        "Child process was terminated by signal 9 (KILL)\n",
        encoding="utf-8",
    )
    name, _info, _files = FuzzerLogParser(
        server_log=str(server_log),
        stderr_log="",
        fuzzer_log="",
        self_killed_server=True,
    ).parse_failure()
    assert name.startswith("Logical error")


# Signals 6/7/8 are used below on purpose: the higher-priority "SegFault" pattern
# matches only the literal "Segmentation fault", so a signal it cannot rescue is
# what proves the Signal pattern itself still reports genuine fatal signals.
_FATAL_BUS = (
    "2026.07.25 01:02:03.000001 [ 123 ] {} <Fatal> BaseDaemon: "
    "(version 26.7.1.1, build id: X, git hash: Y, architecture: x86_64) "
    "(from thread 456) Received signal 7 (Bus error)\n"
    "2026.07.25 01:02:03.000002 [ 123 ] {} <Fatal> BaseDaemon: "
    "Signal description: Bus error\n"
)


def test_self_killed_server_keeps_a_genuine_fatal_signal(tmp_path):
    # A server that dies of a real fatal signal WHILE the probe stage declared it
    # unanswering: the flag is set, but the crash signature must survive (a
    # fatally-signaled server is exactly a server that stops answering probes).
    server_log = tmp_path / "server.log"
    server_log.write_text(_FATAL_BUS, encoding="utf-8")
    name, _info, _files = FuzzerLogParser(
        server_log=str(server_log),
        stderr_log="",
        fuzzer_log="",
        self_killed_server=True,
    ).parse_failure()
    assert "Received signal 7" in name
    assert name != FuzzerLogParser.UNKNOWN_ERROR


def test_self_killed_server_drops_self_line_keeps_later_fatal_signal(tmp_path):
    # Our own graceful-stop line comes FIRST, the genuine fatal signal after it:
    # the self-inflicted line is dropped, the real one is still reported.
    server_log = tmp_path / "server.log"
    server_log.write_text(
        "2026.07.25 01:00:00.000001 [ 123 ] {} <Trace> Application: "
        "Received signal 15\n"
        "2026.07.25 01:02:03.000001 [ 123 ] {} <Fatal> BaseDaemon: "
        "(version 26.7.1.1, build id: X, git hash: Y, architecture: x86_64) "
        "(from thread 456) Received signal 6 (Aborted)\n"
        "2026.07.25 01:02:03.000002 [ 123 ] {} <Fatal> BaseDaemon: "
        "Signal description: Aborted\n",
        encoding="utf-8",
    )
    name, _info, _files = FuzzerLogParser(
        server_log=str(server_log),
        stderr_log="",
        fuzzer_log="",
        self_killed_server=True,
    ).parse_failure()
    assert "Received signal 6" in name
    assert "signal 15" not in name


def test_self_killed_server_graceful_stop_with_trailing_context_is_unknown(tmp_path):
    # A real graceful stop is followed by ordinary shutdown lines. Filtering the
    # self-inflicted lines only AFTER `rg -A 10` expanded context leaves those
    # trailing lines behind, so error_output stays truthy and the FIRST surviving
    # context line becomes the failure name.
    server_log = tmp_path / "server.log"
    server_log.write_text(
        "2026.07.25 00:00:00.000000 [ 1 ] {} <Trace> Application: "
        "Received signal 15\n"
        "2026.07.25 00:00:00.100000 [ 1 ] {} <Information> Application: "
        "Received termination signal (Terminated)\n"
        "2026.07.25 00:00:02.000000 [ 1 ] {} <Information> Application: "
        "Waiting for current connections to close.\n"
        "2026.07.25 00:00:03.000000 [ 1 ] {} <Information> Application: "
        "Closed all listening sockets.\n"
        "2026.07.25 00:00:05.000000 [ 1 ] {} <Information> Application: "
        "Shutting down storages.\n",
        encoding="utf-8",
    )
    name, _info, _files = FuzzerLogParser(
        server_log=str(server_log),
        stderr_log="",
        fuzzer_log="",
        self_killed_server=True,
    ).parse_failure()
    assert name == FuzzerLogParser.UNKNOWN_ERROR, (
        "a shutdown context line became the failure name: the self-kill filter "
        f"must run before -A expands context (got {name!r})"
    )


def test_self_killed_server_sigkill_with_watchdog_context_is_not_a_signal_row(tmp_path):
    # The real memory-stuck shape: the tracker error, our SIGKILL as reported by
    # the watchdog, then the watchdog's own "exited normally" line (BaseDaemon.cpp
    # :727). The stable memory-limit name must win, not the trailing context line.
    server_log = tmp_path / "server.log"
    server_log.write_text(
        "2026.07.25 00:00:00.000000 [ 1 ] {} <Error> MemoryTracker: (total) "
        "memory limit exceeded: would use 55.10 GiB (attempt to allocate chunk "
        "of 4194304 bytes), current RSS: 55.00 GiB, maximum: 46.00 GiB.\n"
        "2026.07.25 00:00:10.000000 [ 1 ] {} <Fatal> BaseDaemon: Child process "
        "was terminated by signal 9 (KILL). If it is not done by 'forcestop' "
        "command or manually, the possible cause is OOM Killer.\n"
        "2026.07.25 00:00:10.100000 [ 1 ] {} <Information> Application: "
        "Child process exited normally with code 0.\n",
        encoding="utf-8",
    )
    name, _info, _files = FuzzerLogParser(
        server_log=str(server_log),
        stderr_log="",
        fuzzer_log="",
        self_killed_server=True,
    ).parse_failure()
    assert name == job.MEMORY_STUCK_NAME, (
        "the SIGKILL watchdog's trailing context line outranked the stable "
        f"memory-limit signature (got {name!r})"
    )
    assert "exited normally" not in name


def test_marker_wins_over_self_inflicted_context_lines(tmp_path):
    # Downstream consequence of the same bug: _select_failure_result treats any
    # name that is neither UNKNOWN_ERROR nor MEMORY_STUCK_NAME as a genuine
    # parser finding, so a context-line name demotes the marker row and destroys
    # the stable CIDB signature the memory-stuck path promises.
    server_log = tmp_path / "server.log"
    server_log.write_text(
        "2026.07.25 00:00:00.000000 [ 1 ] {} <Error> MemoryTracker: (total) "
        "memory limit exceeded: would use 55.10 GiB, maximum: 46.00 GiB.\n"
        "2026.07.25 00:00:10.000000 [ 1 ] {} <Fatal> BaseDaemon: Child process "
        "was terminated by signal 9 (KILL).\n"
        "2026.07.25 00:00:10.100000 [ 1 ] {} <Information> Application: "
        "Child process exited normally with code 0.\n",
        encoding="utf-8",
    )
    parsed_name, parsed_info, files = FuzzerLogParser(
        server_log=str(server_log),
        stderr_log="",
        fuzzer_log="",
        self_killed_server=True,
    ).parse_failure()
    marker_text = "probes=12 tier=fast"
    marker_result = Result(
        name=job.MEMORY_STUCK_NAME,
        status=Result.Status.FAIL,
        info=marker_text,
    )
    selected = job._select_failure_result(
        parsed_name,
        parsed_info,
        files,
        marker_result,
        None,
        marker_text=marker_text,
    )
    assert selected.name == job.MEMORY_STUCK_NAME, (
        "a self-inflicted context line was mistaken for a genuine parser "
        f"finding and demoted the marker row (got {selected.name!r})"
    )


# --------------------------------------------------------------------------- #
# artifact readability (ownership-repair skip)
# --------------------------------------------------------------------------- #


def _pid_from(pidfile, timeout=15):
    """The pid a test child wrote into `pidfile`, or None if it never appeared."""
    give_up_at = time.monotonic() + timeout
    while time.monotonic() < give_up_at:
        try:
            text = pidfile.read_text().strip()
        except OSError:
            text = ""
        if text:
            return int(text)
        time.sleep(0.1)
    return None


def _assert_pid_gone(pid, timeout=15):
    """Fail unless `pid` is dead within `timeout` (killing it so nothing leaks)."""
    give_up_at = time.monotonic() + timeout
    while time.monotonic() < give_up_at:
        try:
            os.kill(pid, 0)
        except OSError:
            return
        time.sleep(0.1)
    try:
        os.kill(pid, 9)
    finally:
        raise AssertionError(f"process {pid} was left running")


def _unreadable(path):
    """chmod a file so the current (non-root) user cannot read it."""
    path.chmod(0o000)
    return not os.access(path, os.R_OK)


@pytest.mark.skipif(
    os.getuid() == 0, reason="root can read any mode, so unreadability cannot be staged"
)
def test_unreadable_artifacts_flags_a_root_owned_report(workspace):
    # The container died before its EXIT trap ran (kernel OOM kill / docker
    # teardown), so the sanitizer report is still 0640 root-owned: the host-side
    # ownership repair must NOT be skipped just because a marker exists.
    san = workspace / "sanitizer.log.4242"
    san.write_text("==4242==ERROR: AddressSanitizer\n", encoding="utf-8")
    ok = workspace / "server.log"
    ok.write_text("readable\n", encoding="utf-8")
    assert _unreadable(san)
    assert job._unreadable_artifacts([san, ok]) == [san]


def test_unreadable_artifacts_empty_when_everything_is_readable(workspace):
    # The normal case: the in-container chmod ran, nothing to repair.
    paths = []
    for name in ("server.log", "sanitizer.log.1", "harness_watchdog.txt"):
        p = workspace / name
        p.write_text("x\n", encoding="utf-8")
        p.chmod(0o644)
        paths.append(p)
    assert job._unreadable_artifacts(paths) == []


def test_unreadable_artifacts_ignores_absent_and_empty_files(workspace):
    # An absent or zero-byte artifact is nothing to rescue: counting it would run
    # the unbounded repair container on every ordinary run.
    missing = workspace / "fatal.log"
    empty = workspace / "dmesg.log"
    empty.write_text("", encoding="utf-8")
    assert job._unreadable_artifacts([missing, empty]) == []


def test_run_fuzz_job_consults_the_readability_check():
    # Pins the CALL SITE: dropping the check would silently restore the
    # marker-implies-readable assumption.
    assert "_unreadable_artifacts" in set(job.run_fuzz_job.__code__.co_names)


def test_collectable_cores_mirrors_the_collector_selection(workspace):
    # collect_cores globs core.*, takes the first 3, and only THEN skips the
    # already-processed .zst/.enc. The readability decision must cover exactly
    # those files -- it `zstd`s them and RAISES on an unreadable one, after
    # classification -- and no more: an unreadable core past the collector's
    # cutoff would run the unbounded repair container for a file never read.
    # The processed entries sit INSIDE the first three sorted names, so filtering
    # before slicing would backfill with core.4/core.5.
    for name in ("core.1", "core.4", "core.5"):
        (workspace / name).write_bytes(b"CORE")
    (workspace / "core.2.zst").write_bytes(b"z")
    (workspace / "core.3.enc").write_bytes(b"e")
    assert sorted(p.name for p in workspace.glob("core.*"))[:3] == [
        "core.1",
        "core.2.zst",
        "core.3.enc",
    ], "fixture must place the processed entries inside the collector's cutoff"
    assert [p.name for p in job._collectable_cores()] == ["core.1"]


def test_collectable_cores_empty_without_cores(workspace):
    # A memory-stuck kill writes no core, and an ordinary run has none: the check
    # must add nothing there, so the skip path stays available.
    (workspace / "server.log").write_text("x\n", encoding="utf-8")
    assert job._collectable_cores() == []


@pytest.mark.skipif(
    os.getuid() == 0, reason="root can read any mode, so unreadability cannot be staged"
)
def test_unreadable_artifacts_flags_a_root_owned_core(workspace):
    # A kernel-written core belongs to the crashing root process, and the
    # in-container chmod cannot have run if the container never reached its trap.
    # ClickHouseService.collect_cores then dies with `zstd: ... Permission denied`,
    # which aborts run_fuzz_job AFTER classification and discards the whole Result.
    core = workspace / "core.31337"
    core.write_bytes(b"FAKECORE")
    assert _unreadable(core)
    assert job._unreadable_artifacts(job._collectable_cores()) == [core]


def test_run_fuzz_job_readability_check_covers_the_cores():
    # The core list is passed to the readability check but must NOT be appended to
    # `paths`: `paths` is the upload list, and a core may only leave the runner
    # compressed+encrypted by collect_cores. Pin both halves structurally -- live
    # execution here would need a real docker daemon.
    import ast
    import inspect

    tree = ast.parse(inspect.getsource(job.run_fuzz_job))
    calls = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "_unreadable_artifacts"
    ]
    assert len(calls) == 1, f"expected one readability check, found {len(calls)}"
    assert "_collectable_cores" in {
        n.func.id
        for n in ast.walk(calls[0])
        if isinstance(n, ast.Call) and isinstance(n.func, ast.Name)
    }, (
        "the readability decision must cover the cores collect_cores will read: "
        f"{ast.dump(calls[0])}"
    )
    # `paths` must not gain the cores (that would upload them unencrypted).
    assigns = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "extend"
        and isinstance(node.func.value, ast.Name)
        and node.func.value.id == "paths"
    ]
    for node in assigns:
        assert "_collectable_cores" not in {
            n.func.id
            for n in ast.walk(node)
            if isinstance(n, ast.Call) and isinstance(n.func, ast.Name)
        }, f"cores must not be added to the upload list: {ast.dump(node)}"


def test_ownership_repair_condition_also_fires_on_unreadable_artifacts():
    # Name-only wiring cannot see the CONDITION: computing `unreadable` and then
    # branching on the marker alone keeps every other test green while a
    # container that died before its EXIT trap silently loses its root-owned
    # 0640 sanitizer reports. Live execution here would need a real docker
    # daemon, so pin the guard structurally (same approach as
    # test_top_level_status_is_called_with_the_attached_results).
    import ast
    import inspect

    tree = ast.parse(inspect.getsource(job.run_fuzz_job))
    guards = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.If)
        and any(
            isinstance(sub, ast.Call)
            and isinstance(sub.func, ast.Attribute)
            and sub.func.attr == "fix_ownership_after_docker"
            for sub in ast.walk(node)
        )
    ]
    assert len(guards) == 1, (
        f"expected one branch guarding fix_ownership_after_docker, found {len(guards)}"
    )
    test = guards[0].test
    assert isinstance(test, ast.BoolOp) and isinstance(test.op, ast.Or), (
        "the repair must run when EITHER the run is healthy OR artifacts are "
        f"unreadable, got {ast.dump(test)}"
    )
    assert "unreadable" in {
        n.id for n in ast.walk(test) if isinstance(n, ast.Name)
    }, (
        "the guard must consult the unreadable-artifacts result, not the "
        f"marker/watchdog alone: {ast.dump(test)}"
    )


# --------------------------------------------------------------------------- #
# pre-run hygiene (§3e)
# --------------------------------------------------------------------------- #


def test_clean_stale_run_state_deletes_inputs_keeps_survivors(workspace):
    # Stale classification inputs from a prior run + a stale sanitizer log.
    stale = [
        "server_memory_stuck.txt",
        "harness_watchdog.txt",
        "status.tsv",
        "server.log",
        "stderr.log",
        "fuzzer.log",
        "fuzzerout.sql",
        "fatal.log",
        "dmesg.log",
        "sanitizer.log.3456",
        # zstd keeps its input, so a raw core outlives the run that produced it,
        # and both the runner tail and collect_cores glob core.* blindly: a later
        # core-less run (a memory-stuck kill writes none) would attach this one as
        # evidence for its own failure.
        "core.31337",
        "core.zst",
    ]
    for name in stale:
        (workspace / name).write_text("stale", encoding="utf-8")
    # Pre-container survivors that must NOT be deleted.
    survivors = ["ci-targeted-queries.txt", "fuzz.json", "ci-changed-files.txt"]
    for name in survivors:
        (workspace / name).write_text("keep", encoding="utf-8")

    job._clean_stale_run_state()

    for name in stale:
        assert not (workspace / name).exists(), f"{name} should have been deleted"
    for name in survivors:
        assert (workspace / name).exists(), f"{name} must survive"


def test_clean_stale_run_state_is_noop_on_clean_workspace(workspace):
    # Nothing to delete -> no error.
    job._clean_stale_run_state()


def test_clean_stale_run_state_keeps_state_seeded_by_the_e2e_fixture(
    workspace, monkeypatch
):
    # ci/tests/test_e2e.py::test_fuzzer seeds status.tsv + the job artifacts and
    # then invokes the real job, so for that fixture they are INPUT. Deleting them
    # made the job take the missing-status.tsv early-abort (which exits before
    # collect_cores), so the report carried no encrypted core and it failed.
    monkeypatch.setenv("PRAKTIKA_FUZZER_KEEP_PRESEEDED_STATE", "1")
    preseeded = [
        "status.tsv",
        "server.log",
        "fuzzer.log",
        "stderr.log",
        "dmesg.log",
        "fatal.log",
        "sanitizer.log.3456",
        # The fixture seeds a core as well, and that is what it asserts gets
        # encrypted and attached, so the opt-in must protect it too.
        "core.test",
        "ci-targeted-queries.txt",
    ]
    for name in preseeded:
        (workspace / name).write_text("seed", encoding="utf-8")

    job._clean_stale_run_state()

    for name in preseeded:
        assert (workspace / name).exists(), (
            f"{name} was seeded as job input and must survive"
        )


@pytest.mark.parametrize("opt_in", [None, "", "0", "true", "yes"])
def test_clean_stale_run_state_cleans_unless_explicitly_opted_in(
    workspace, monkeypatch, opt_in
):
    # An ordinary local rerun must clean too: `ci.praktika run` defaults to local
    # mode, so keying this on the generic local-run flag would let a stale marker
    # from the previous local run misclassify the next healthy one. Only the exact
    # opt-in preserves state, and leftovers of a run predating this cleanup carry
    # no marker of their own, so they are removed as well.
    if opt_in is None:
        monkeypatch.delenv("PRAKTIKA_FUZZER_KEEP_PRESEEDED_STATE", raising=False)
    else:
        monkeypatch.setenv("PRAKTIKA_FUZZER_KEEP_PRESEEDED_STATE", opt_in)
    (workspace / "server_memory_stuck.txt").write_text("from an older run", "utf-8")
    (workspace / "status.tsv").write_text("1\t0\t0\n", encoding="utf-8")
    (workspace / "sanitizer.log.99").write_text("older", encoding="utf-8")

    job._clean_stale_run_state()

    assert not (workspace / "server_memory_stuck.txt").exists()
    assert not (workspace / "status.tsv").exists()
    assert not (workspace / "sanitizer.log.99").exists()


def test_clean_stale_run_state_fails_closed_when_input_survives(workspace, monkeypatch):
    # A stale marker that cannot be removed would misclassify the next run as
    # memory-stuck. If unlink silently fails to remove it, hygiene must raise
    # BEFORE the container launches rather than run against corrupted state.
    (workspace / "server_memory_stuck.txt").write_text("stale", encoding="utf-8")
    monkeypatch.setattr(
        "pathlib.Path.unlink", lambda self: None
    )  # no-op: nothing removed
    with pytest.raises(RuntimeError, match="stale classification inputs"):
        job._clean_stale_run_state()


# --------------------------------------------------------------------------- #
# fail-open closure (§3d/r9): a benign exit code must not report OK under a marker/watchdog
# --------------------------------------------------------------------------- #


def _fail(name, status=Result.Status.FAIL):
    return Result(name=name, status=status, info="")


def test_watchdog_forces_fail_over_benign_exit():
    # The benign branch sets status=OK / is_failed=False for exit codes 0/137/143
    # (the OK status here encodes that that branch was taken); a watchdog (e.g.
    # reap-abandon's 137) must flip it back to a failure.
    watchdog = _fail("Fuzzer harness watchdog fired", Result.Status.ERROR)
    status, is_failed = job._force_fail_for_markers(
        Result.Status.OK, False, None, watchdog
    )
    assert is_failed is True
    assert status == Result.Status.FAIL


def test_marker_forces_fail_over_benign_exit():
    marker = _fail(job.MEMORY_STUCK_NAME)
    status, is_failed = job._force_fail_for_markers(
        Result.Status.OK, False, marker, None
    )
    assert is_failed is True
    assert status == Result.Status.FAIL


def test_no_marker_no_watchdog_leaves_status_untouched():
    status, is_failed = job._force_fail_for_markers(Result.Status.OK, False, None, None)
    assert is_failed is False
    assert status == Result.Status.OK


# --------------------------------------------------------------------------- #
# top-level status: attaching sub-results must not downgrade a decided ERROR
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize(
    "sub",
    [
        pytest.param(_fail("Server unresponsive: memory limit exceeded"), id="marker"),
        pytest.param(
            _fail("Fuzzer harness watchdog fired", Result.Status.ERROR), id="watchdog"
        ),
    ],
)
def test_error_survives_attached_sub_results(sub):
    # The BuzzHouse-227 / client-failure / dmesg-OOM branches decide ERROR before
    # the marker and watchdog rows are attached. Passing None there would let
    # create_from re-derive the status from those rows, and a marker row is a FAIL.
    assert job._top_level_status(Result.Status.ERROR, [sub]) == Result.Status.ERROR
    derived = Result.create_from(
        name="AST fuzzer",
        results=[sub],
        status=job._top_level_status(Result.Status.ERROR, [sub]),
        info="",
    )
    assert derived.status == Result.Status.ERROR, (
        "an ERROR classification was downgraded by attaching a sub-result"
    )
    assert [r.name for r in derived.results] == [sub.name]


def test_watchdog_only_benign_exit_reports_error_end_to_end():
    # The whole transition a reap-abandon takes, not just its first step: the
    # benign exit code 137 sets OK/not-failed, _force_fail_for_markers turns that
    # into FAIL, the watchdog is attached as the only sub-result, and the job must
    # end up ERROR. Asserting only the intermediate FAIL would keep passing while
    # a watchdog-only run reports the wrong final status.
    watchdog = _fail("Fuzzer harness watchdog fired", Result.Status.ERROR)
    status, is_failed = job._force_fail_for_markers(
        Result.Status.OK, False, None, watchdog
    )
    assert (status, is_failed) == (Result.Status.FAIL, True)
    # server_died=0 on this path, so the parser is skipped and the watchdog is
    # surfaced as a sub-result instead.
    assert job._should_parse_logs(is_failed, status, 0) is False
    results = [watchdog]
    final = Result.create_from(
        name="AST fuzzer",
        results=results,
        status=job._top_level_status(status, results),
        info="",
    )
    assert final.status == Result.Status.ERROR, (
        "a watchdog-only run must report ERROR, not the intermediate FAIL"
    )


def test_fail_is_still_derived_from_sub_results():
    # The parser row is what names the failure, so a plain FAIL must still defer.
    parser_row = _fail("Logical error: some assertion")
    assert job._top_level_status(Result.Status.FAIL, [parser_row]) is None


def test_status_is_kept_when_there_are_no_sub_results():
    # Nothing to derive from: create_from would set ERROR for a missing status.
    assert job._top_level_status(Result.Status.OK, []) == Result.Status.OK
    assert job._top_level_status(Result.Status.FAIL, []) == Result.Status.FAIL


# --------------------------------------------------------------------------- #
# early-abort path: status.tsv unreadable/malformed must stay ERROR, and the
# cores of that run must still be collected (this branch exits the job)
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize(
    "exc",
    [
        FileNotFoundError("status.tsv was not produced"),
        FileNotFoundError("status.tsv is empty"),
    ],
    ids=["missing", "empty"],
)
def test_early_abort_defers_to_the_marker_only_when_status_is_absent(exc):
    # A missing/empty status.tsv IS "the runner aborted before reporting", which
    # is exactly what the marker/watchdog classifies -- so the sub-results may
    # set the top-level status there. _read_fuzzer_status raises FileNotFoundError
    # for both shapes.
    marker = _fail(job.MEMORY_STUCK_NAME)
    assert job._early_abort_status(exc, [marker]) is None
    assert (
        Result.create_from(
            name="job", status=job._early_abort_status(exc, [marker]), results=[marker]
        ).status
        == Result.Status.FAIL
    )


@pytest.mark.parametrize(
    "exc",
    [
        ValueError("expected 3 tab-separated fields, got 2"),
        PermissionError("status.tsv is not readable"),
        UnicodeDecodeError("utf-8", b"\xff", 0, 1, "invalid start byte"),
    ],
    ids=["malformed", "unreadable", "undecodable"],
)
def test_early_abort_keeps_error_for_a_faulty_status_file(exc):
    # Anything other than "absent" is a fault of the FILE, which
    # _format_status_error already reports as a harness bug: deriving from the
    # marker (a FAIL) would downgrade a harness/infrastructure ERROR to a plain
    # test failure -- the misattribution this change exists to remove. Measured:
    # Result.create_from(status=None, results=[marker_fail]) yields FAIL.
    marker = _fail(job.MEMORY_STUCK_NAME)
    assert job._early_abort_status(exc, [marker]) == Result.Status.ERROR
    assert (
        Result.create_from(
            name="job", status=job._early_abort_status(exc, [marker]), results=[marker]
        ).status
        == Result.Status.ERROR
    )


def test_early_abort_is_error_without_sub_results():
    # No marker and no watchdog: nothing to derive from, keep master's ERROR.
    assert (
        job._early_abort_status(FileNotFoundError("x"), []) == Result.Status.ERROR
    )


def test_early_abort_status_is_wired_into_the_early_result():
    # Name-only wiring cannot see WHICH exception the decision uses, so pin the
    # call's arguments: passing anything but the caught exception and the
    # sub-results list restores the downgrade.
    import ast
    import inspect

    tree = ast.parse(inspect.getsource(job.run_fuzz_job))
    calls = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "_early_abort_status"
    ]
    assert len(calls) == 1, f"expected one _early_abort_status call, got {len(calls)}"
    assert [type(a) for a in calls[0].args] == [ast.Name, ast.Name], (
        "_early_abort_status must be called with the exception and the sub-results"
    )
    assert [a.id for a in calls[0].args] == ["e", "sub_results"], (
        f"called with {[ast.dump(a) for a in calls[0].args]}: it must get the "
        "caught exception, or a faulty status.tsv is downgraded to FAIL"
    )
    # And that status must be the one the early Result is built from.
    assembly = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "create_from"
        and any(k.arg == "status" and k.value is calls[0] for k in node.keywords)
    ]
    assert len(assembly) == 1, (
        "expected exactly one Result.create_from whose status is _early_abort_status(...)"
    )


def test_collect_cores_or_note_returns_the_encrypted_artifacts(workspace):
    # Drives the REAL ClickHouseService.collect_cores: a readable core is
    # compressed, encrypted and returned alongside the wrapped AES key.
    (workspace / "core.12345").write_bytes(b"FAKECORE" * 64)
    files, note = job._collect_cores_or_note()
    assert note == ""
    assert sorted(os.path.basename(str(f)) for f in files) == [
        "aes.key.rsa",
        "core.12345.zst.enc",
    ]


@pytest.mark.skipif(
    os.getuid() == 0, reason="root can read any mode, so unreadability cannot be staged"
)
def test_collect_cores_or_note_survives_an_unreadable_core(workspace):
    # collect_cores shells out to zstd, which dies with "Permission denied" on a
    # root-owned core. Both call sites run AFTER classification, so an escaping
    # exception discards the whole Result -- marker, watchdog and logs included --
    # which is strictly worse than losing the one core. Report, do not raise.
    core = workspace / "core.31337"
    core.write_bytes(b"FAKECORE" * 64)
    assert _unreadable(core)
    files, note = job._collect_cores_or_note()
    assert files == []
    assert "core collection failed" in note


def test_collect_cores_or_note_gives_up_on_a_stalled_compressor(workspace, monkeypatch):
    # zstd on a multi-GiB core has no bound of its own (Shell.check does not
    # forward its timeout to Shell.run), and this runs AFTER classification, so an
    # unbounded stall recreates the artifact-less external cancellation this whole
    # change removes. The collection runs in a forked child, so it signals through
    # the filesystem rather than through in-process state.
    started = workspace / "collector.started"

    def never_returns(_directory):
        started.write_text("1", encoding="utf-8")
        time.sleep(600)
        raise AssertionError("unreachable")

    (workspace / "core.101").write_bytes(b"CORE" * 64)
    monkeypatch.setattr(job.ClickHouseService, "collect_cores", never_returns)
    t0 = time.monotonic()
    files, note = job._collect_cores_or_note(deadline=1)
    elapsed = time.monotonic() - t0
    assert started.exists(), "the collector was never invoked"
    assert files == []
    assert "did not finish within 1s" in note
    assert elapsed < 30, f"the helper waited {elapsed:.1f}s on a stalled collector"


def test_collect_cores_or_note_kills_the_stalled_compressor(workspace, monkeypatch):
    # Giving up on the wait is not enough: Shell.run spawns each command with
    # start_new_session=True, so it survives interpreter exit (measured: it
    # reparented to PID 1) and keeps burning CPU/disk, or races the next run's
    # workspace cleanup. Use a real grandchild that records its own pid.
    pidfile = workspace / "child.pid"

    def stalling(_directory):
        Shell.check(
            f"bash -c 'echo $$ > {pidfile}; exec sleep 600'", verbose=False, strict=True
        )

    (workspace / "core.102").write_bytes(b"CORE" * 64)
    monkeypatch.setattr(job.ClickHouseService, "collect_cores", stalling)
    files, note = job._collect_cores_or_note(deadline=2)
    assert files == []
    assert "did not finish within 2s" in note
    assert _pid_from(pidfile) is not None, "the stalled child never recorded its pid"
    _assert_pid_gone(_pid_from(pidfile))


def test_collect_cores_or_note_kills_a_later_collection_stage(workspace, monkeypatch):
    # collect_cores walks up to three cores through sequential zstd/openssl
    # commands whose exit codes Utils.encrypt ignores, so killing the ONE process
    # that happens to be running leaves the collector free to start the next stage.
    # Killing the child's whole session must stop every stage: the second command
    # here stands in for that later stage.
    first = workspace / "first.pid"
    second = workspace / "second.pid"

    def two_stages(_directory):
        Shell.check(
            f"bash -c 'echo $$ > {first}; exec sleep 1'", verbose=False, strict=False
        )
        Shell.check(
            f"bash -c 'echo $$ > {second}; exec sleep 600'", verbose=False, strict=False
        )

    (workspace / "core.103").write_bytes(b"CORE" * 64)
    monkeypatch.setattr(job.ClickHouseService, "collect_cores", two_stages)
    files, note = job._collect_cores_or_note(deadline=4)
    assert files == []
    assert "did not finish within 4s" in note
    second_pid = _pid_from(second)
    assert second_pid is not None, "the second stage never started"
    _assert_pid_gone(second_pid)


def test_collect_cores_or_note_kills_a_stage_started_after_the_snapshot(
    workspace, monkeypatch
):
    # The nastiest ordering: the collector finishes one command and launches the
    # NEXT one in a fresh session while cleanup is under way, so the successor is
    # absent from any snapshot taken before it existed. Freezing the collector
    # (SIGSTOP) before snapshotting is what makes the set unable to grow. The first
    # stage here is short, so the successor would appear right around the deadline.
    first = workspace / "first.pid"
    late = workspace / "late.pid"

    def late_stage(_directory):
        Shell.check(
            f"bash -c 'echo $$ > {first}; exec sleep 1'", verbose=False, strict=False
        )
        Shell.check(
            f"bash -c 'echo $$ > {late}; exec sleep 600'", verbose=False, strict=False
        )
        time.sleep(600)

    (workspace / "core.104").write_bytes(b"CORE" * 64)
    monkeypatch.setattr(job.ClickHouseService, "collect_cores", late_stage)
    files, note = job._collect_cores_or_note(deadline=1)
    assert files == []
    assert "did not finish within 1s" in note
    assert _pid_from(first, timeout=10) is not None, "the first stage never started"
    late_pid = _pid_from(late, timeout=8)
    assert late_pid is None, (
        f"a collection stage ({late_pid}) was launched after the collector should "
        "have been frozen"
    )


def test_collect_cores_or_note_survives_a_fork_failure(workspace, monkeypatch):
    # On a memory-saturated host -- exactly the state this change handles -- fork
    # can fail with EAGAIN/ENOMEM right when a core is worth having. The helper's
    # never-raise contract must cover its own setup, or the classified Result is
    # discarded to report a missing core.
    (workspace / "core.11").write_bytes(b"CORE" * 64)

    def no_fork():
        raise BlockingIOError(11, "Resource temporarily unavailable")

    monkeypatch.setattr(os, "fork", no_fork)
    files, note = job._collect_cores_or_note(deadline=5)
    assert files == []
    assert "could not start core collection" in note


def test_collect_cores_or_note_skips_the_fork_without_cores(workspace, monkeypatch):
    # The common case (a memory-stuck kill writes no core) must not fork at all:
    # cheap, and it cannot fail.
    def unexpected_fork():
        raise AssertionError("forked with no core to collect")

    monkeypatch.setattr(os, "fork", unexpected_fork)
    (workspace / "server.log").write_text("x\n", encoding="utf-8")
    assert job._collect_cores_or_note(deadline=5) == ([], "")


def _proc_state(pid):
    try:
        with open(f"/proc/{pid}/stat", "rb") as fh:
            return fh.read().rsplit(b")", 1)[1].split()[0].decode()
    except (OSError, IndexError):
        return ""


def test_await_stopped_observes_the_stop_transition():
    # kill() only QUEUES SIGSTOP: returning from it does NOT mean the process has
    # parked, and a still-runnable collector can start a command that is then
    # missing from the snapshot taken next. The race is probabilistic, so assert
    # over many iterations -- without the waitpid(WUNTRACED) wait some of them
    # observe a still-running process.
    for _ in range(40):
        pid = os.fork()
        if pid == 0:  # pragma: no cover - child
            try:
                while True:
                    os.getpid()  # stay RUNNABLE, not blocked in a syscall
            finally:
                os._exit(0)
        try:
            job._await_stopped(pid)
            state = _proc_state(pid)
            assert state in ("T", "t"), (
                f"collector was not stopped when the snapshot would be taken "
                f"(state {state!r})"
            )
        finally:
            try:
                os.kill(pid, 9)
                os.waitpid(pid, 0)
            except OSError:
                pass


def test_await_stopped_tolerates_an_already_reaped_process(monkeypatch):
    # A collector reaped elsewhere makes waitpid raise ChildProcessError; it can
    # spawn nothing, so this must return rather than propagate (the caller must
    # never raise) or spin until the timeout.
    def reaped(*_args, **_kwargs):
        raise ChildProcessError(10, "No child processes")

    monkeypatch.setattr(os, "kill", lambda *_a: None)
    monkeypatch.setattr(os, "waitpid", reaped)
    started = time.monotonic()
    job._await_stopped(4242, timeout=30)  # must not raise
    assert time.monotonic() - started < 5, "it waited instead of returning at once"


def test_await_stopped_tolerates_an_exited_process():
    # An already-exited (zombie) collector starts nothing either.
    pid = os.fork()
    if pid == 0:  # pragma: no cover - child
        os._exit(0)
    time.sleep(0.2)
    job._await_stopped(pid, timeout=2)  # must not raise or hang
    try:
        os.waitpid(pid, os.WNOHANG)
    except OSError:
        pass


def test_collect_cores_or_note_survives_an_exit_at_the_deadline(workspace, monkeypatch):
    # A collector that finishes right at the deadline can have its exit status
    # consumed by the cleanup path's SIGSTOP wait; a second unguarded waitpid then
    # raises ChildProcessError out of a helper that must never raise, discarding
    # the classified Result. Force the read deadline to expire on a quick collector.
    (workspace / "core.21").write_bytes(b"CORE" * 64)
    monkeypatch.setattr(job.ClickHouseService, "collect_cores", lambda _d: [])
    monkeypatch.setattr(job, "_read_until_eof", lambda fd, _deadline: (os.close(fd), None)[1])
    files, note = job._collect_cores_or_note(deadline=1)  # must not raise
    assert files == []
    assert "did not finish" in note


def test_signal_kill_never_kills_our_own_group():
    # Between fork() and the child's setsid() it still shares OUR process group, so
    # a killpg there SIGKILLs the whole job -- measured: the job dies with 137 and
    # reports nothing at all, which is the exact opposite of this change's purpose.
    pid = os.fork()
    if pid == 0:  # pragma: no cover - child: deliberately does NOT setsid
        try:
            time.sleep(30)
        finally:
            os._exit(0)
    try:
        assert os.getpgid(pid) == os.getpgid(0), "fixture must share our group"
        job._signal_kill(pid)  # must kill the pid, never the group
        assert os.getpgid(0) == os.getpgid(0)  # we are still alive to assert it
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            if _proc_state(pid) in ("", "Z"):
                break
            time.sleep(0.05)
        else:  # pragma: no cover - only on a real regression
            raise AssertionError("the collector pid was not killed")
    finally:
        try:
            os.kill(pid, 9)
            os.waitpid(pid, 0)
        except OSError:
            pass


def test_reap_is_idempotent():
    # Reaping twice must not raise: the SIGSTOP wait may already have consumed the
    # status of a collector that exited at the deadline.
    pid = os.fork()
    if pid == 0:  # pragma: no cover - child
        os._exit(7)
    first = job._reap(pid)
    assert os.WIFEXITED(first) and os.WEXITSTATUS(first) == 7
    assert job._reap(pid) == 0  # already gone, reported as success


def test_unusable_core_artifacts_rejects_an_unwrapped_key(workspace):
    # Utils.encrypt ignores openssl's exit codes and returns the .enc path
    # regardless, and collect_cores appends the .rsa only `if` it exists -- so a
    # failed key wrap looks exactly like success and yields a core nobody can
    # decrypt. Measured with an invalid public key: a .zst.enc, no .rsa, no error.
    enc = workspace / "core.9.zst.enc"
    enc.write_bytes(b"C" * (job.AES_BLOCK_BYTES * 4))
    assert "undecryptable" in job._unusable_core_artifacts([enc])
    rsa = workspace / "aes.key.rsa"
    rsa.write_bytes(b"W" * job.RSA_WRAPPED_KEY_BYTES)
    assert job._unusable_core_artifacts([enc, rsa]) == ""


def test_unusable_core_artifacts_rejects_missing_or_empty(workspace):
    missing = workspace / "core.1.zst.enc"
    assert "missing or empty" in job._unusable_core_artifacts([missing])
    empty = workspace / "core.2.zst.enc"
    empty.write_bytes(b"")
    assert "missing or empty" in job._unusable_core_artifacts([empty])
    assert job._unusable_core_artifacts([]) == ""


def test_collect_cores_or_note_reports_an_unwrapped_key(workspace, monkeypatch):
    # The wrapper must not present an undecryptable core as a clean collection.
    enc = workspace / "core.5.zst.enc"
    enc.write_bytes(b"C" * (job.AES_BLOCK_BYTES * 4))
    (workspace / "core.105").write_bytes(b"CORE" * 64)
    monkeypatch.setattr(job.ClickHouseService, "collect_cores", lambda _d: [str(enc)])
    files, note = job._collect_cores_or_note(deadline=5)
    assert files == []
    assert "core collection incomplete" in note


def test_clean_stale_run_state_removes_leftover_key_material(workspace):
    # collect_cores reuses an existing aes.key and wraps a fresh one only when the
    # .rsa is ABSENT, so leftovers make the next run encrypt with the previous
    # run's key while attaching the previous run's .rsa -- measured to produce an
    # undecryptable core plus a stale artifact shown as this failure's evidence.
    key = workspace / "aes.key"
    wrapped = workspace / "aes.key.rsa"
    key.write_text("OLDKEY\n", encoding="utf-8")
    wrapped.write_bytes(b"OLDRSA")
    job._clean_stale_run_state()
    assert not key.exists(), "a stale AES key would be reused by the next run"
    assert not wrapped.exists(), "a stale wrapped key would be attached to the next run"


@pytest.mark.parametrize("leftover", ["key-only", "rsa-only", "both"])
def test_clean_stale_run_state_removes_partial_key_material(workspace, leftover):
    key = workspace / "aes.key"
    wrapped = workspace / "aes.key.rsa"
    if leftover in ("key-only", "both"):
        key.write_text("k\n", encoding="utf-8")
    if leftover in ("rsa-only", "both"):
        wrapped.write_bytes(b"r")
    job._clean_stale_run_state()
    assert not key.exists() and not wrapped.exists()


def test_core_collection_deadline_is_a_small_slice_of_the_budget():
    # The result is persisted before collection, so this bound decides
    # core-vs-no-core rather than report-vs-no-report. It must still be a small
    # slice of what is left: the stages before it already consume 74.7 min of an
    # observed 78.3-min cancellation, measured from the START of the fuzz budget
    # (configure, server startup and gdb attach are on top of that).
    #
    # The per-stage limits are READ FROM run-fuzzer.sh rather than restated here:
    # hardcoding them lets a stage be widened in the shell while this margin
    # argument keeps quoting the old, smaller sum.
    import os
    import re as _re

    run_fuzzer = os.path.join(
        os.path.dirname(__file__), "..", "jobs", "scripts", "fuzzer", "run-fuzzer.sh"
    )
    shell = open(run_fuzzer, encoding="utf-8").read()
    m = _re.search(
        r"reap_deadline=\$\(\(\s*remaining_seconds\s*\+\s*(\d+)\s*\+\s*(\d+)\s*\)\)",
        shell,
    )
    assert m, "reap deadline slack not found in run-fuzzer.sh"
    reap_slack = int(m.group(1)) + int(m.group(2))
    # Scope the teardown values to the teardown block: `for _ in {1..N}` appears
    # in several unrelated poll loops, so an unscoped search silently picks the
    # wrong one.
    m = _re.search(
        r"# BEGIN: server teardown poll.*?\n(.*?)\n\s*# END: server teardown poll",
        shell,
        _re.DOTALL,
    )
    assert m, "BEGIN/END markers for the server teardown poll not found"
    teardown_block = m.group(1)
    m = _re.search(r"^\s*teardown_deadline=(\d+)$", teardown_block, _re.MULTILINE)
    assert m, "teardown_deadline not found in the teardown poll block"
    teardown = int(m.group(1))
    m = _re.search(r"for _ in \{1\.\.(\d+)\}", teardown_block)
    assert m, "post-SIGKILL grace loop not found in the teardown poll block"
    grace = int(m.group(1))
    # 60 min fuzz budget, then the reap slack, the graceful-stop probe window,
    # the teardown watchdog and its post-SIGKILL grace.
    spent_from_fuzz_start = (60 * 60 + reap_slack) + 300 + teardown + grace
    observed_cancellation = 78.3 * 60
    remaining = observed_cancellation - spent_from_fuzz_start
    assert job.CORE_COLLECTION_DEADLINE <= remaining, (
        f"a {job.CORE_COLLECTION_DEADLINE}s bound exceeds the {remaining:.0f}s that "
        "remain before the observed cancellation, so collection could be the reason "
        "a run is cut off"
    )
    # And not so tight that a healthy collection is cut off: zstd streams a 1 GiB
    # core (the server's default core_dump.size_limit) in seconds.
    assert job.CORE_COLLECTION_DEADLINE >= 60
    # The window above is wide; the PR body publishes the exact bound, so pin it.
    # Widening it towards `remaining` spends the thin margin described above and
    # must be a deliberate edit of both the constant and that description.
    assert job.CORE_COLLECTION_DEADLINE == 180


def test_result_is_persisted_before_cores_are_collected():
    # Structural pin on the ordering that makes the deadline harmless: in BOTH
    # branches the result must be dumped before _collect_cores_or_note, so the worst
    # case is a report without its core rather than no report at all.
    #
    # The pairing is PER BRANCH, not global: a dump in the early-abort handler must
    # not vouch for the normal path (or the reverse), so each collection is matched
    # against a dump of the SAME result object in its OWN statement block.
    import ast
    import inspect

    tree = ast.parse(inspect.getsource(job.run_fuzz_job))

    def attr_call_targets(stmt, attr):
        """Receiver names of `<name>.<attr>(...)` calls anywhere inside one statement."""
        return {
            n.func.value.id
            for n in ast.walk(stmt)
            if isinstance(n, ast.Call)
            and isinstance(n.func, ast.Attribute)
            and n.func.attr == attr
            and isinstance(n.func.value, ast.Name)
        }

    def direct_attr_call_target(stmt, attr):
        """Receiver name iff `stmt` IS the bare statement `<name>.<attr>(...)`.

        A nested match (`if ...: x.dump`) is deliberately rejected: the point of
        the ordering pin is that the persistence really executes on the way to the
        collection, and a conditionally nested -- or unreachable -- call satisfies a
        recursive search while persisting nothing.
        """
        if not isinstance(stmt, ast.Expr) or not isinstance(stmt.value, ast.Call):
            return None
        func = stmt.value.func
        if (
            isinstance(func, ast.Attribute)
            and func.attr == attr
            and isinstance(func.value, ast.Name)
        ):
            return func.value.id
        return None

    # Locate each collection in the INNERMOST statement block that holds it, so the
    # surrounding statements really are its branch siblings. Walking outwards from
    # the call and remembering the last block crossed gives exactly that block.
    parent_of = {}
    block_of = {}  # child stmt -> the list it belongs to
    for node in ast.walk(tree):
        for field in ("body", "orelse", "finalbody"):
            block = getattr(node, field, None)
            if not (isinstance(block, list) and block and isinstance(block[0], ast.stmt)):
                continue
            for child in block:
                block_of[child] = block
        for child in ast.iter_child_nodes(node):
            parent_of[child] = node

    calls = [
        n
        for n in ast.walk(tree)
        if isinstance(n, ast.Call)
        and isinstance(n.func, ast.Name)
        and n.func.id == "_collect_cores_or_note"
    ]
    sites = []  # (block, index of the sibling statement holding the collection)
    for call in calls:
        node = call
        while node is not None and node not in block_of:
            node = parent_of.get(node)
        assert node is not None, "collection call is not inside a statement block"
        block = block_of[node]
        sites.append((block, block.index(node)))
    assert len(sites) == 2, f"expected two collection sites, found {len(sites)}"
    assert len({id(block) for block, _ in sites}) == 2, (
        "the two collection sites must live in two distinct branches"
    )

    for block, index in sites:
        stmt = block[index]
        # A block-local ordering is only meaningful if the block can run.
        assert_statically_reachable(job.run_fuzz_job, stmt, "core collection")
        # The receiver that ends up carrying the cores is the one that must have
        # been persisted first: `<result>.set_files(cores)` in the same block.
        carriers = set()
        for sibling in block[index:]:
            carriers |= attr_call_targets(sibling, "set_files")
        assert carriers, (
            f"collection at line {stmt.lineno} attaches the cores to nothing"
        )
        dumped_before = set()
        for sibling in block[:index]:
            target = direct_attr_call_target(sibling, "dump")
            if target is not None:
                dumped_before.add(target)
        assert carriers & dumped_before, (
            f"core collection at line {stmt.lineno} is not preceded, IN ITS OWN "
            f"branch, by a dump of {sorted(carriers)}: a stalled or cancelled "
            "collection would lose the whole report"
        )
        # And the same object must still be completed after the collection. This
        # one is deliberately not block-scoped to the collection's OWN block: the
        # normal path completes in the enclosing block, one level out from the
        # `if is_failed:` collection. It is scoped to the chain of ENCLOSING blocks
        # instead, which admits that one-level-out shape while still rejecting a
        # completion buried in some unrelated -- or unreachable -- branch.
        enclosing_blocks = []
        node = stmt
        while node is not None:
            if node in block_of:
                enclosing_blocks.append(block_of[node])
            node = parent_of.get(node)
        completed_after = set()
        for enclosing in enclosing_blocks:
            for sibling in enclosing:
                target = direct_attr_call_target(sibling, "complete_job")
                if target is not None and sibling.lineno > stmt.end_lineno:
                    completed_after.add(target)
        assert carriers & completed_after, (
            f"the job must still be completed after the collection at line "
            f"{stmt.lineno}, on the same result object"
        )


def test_cleanup_of_an_unkillable_collector_stays_inside_the_deadline():
    # `deadline` is the bound on the WHOLE helper. A COOPERATIVE stalled collector
    # cannot show this (it stops and reaps at once), so drive the case the bound
    # exists for: a collector that ignores termination and never reports a stop.
    # With the teardown on its own 5 s budgets this returned ~5 s past the
    # deadline, spending that much of the thin cancellation margin.
    # The wait is what dominates: a collector that ignores termination keeps
    # `_reap` blocked for its full timeout, which used to be a flat 5 s spent AFTER
    # the deadline had already expired. Measure that the budget is honoured, since
    # a cooperative stalled collector exits at once and cannot show it.
    pid = os.fork()
    if pid == 0:  # pragma: no cover - child never returns
        try:
            signal.signal(signal.SIGTERM, signal.SIG_IGN)
            while True:
                pass
        finally:
            os._exit(0)
    try:
        started = time.monotonic()
        job._reap(pid, timeout=0.5)
        elapsed = time.monotonic() - started
        assert elapsed < 2, (
            f"reaping an unkillable collector took {elapsed:.1f}s despite a 0.5s "
            "budget, so the teardown is not bounded by the collection deadline"
        )
        # BOTH teardown steps must derive their bound from the remaining deadline,
        # not just one: restoring the fixed 5 s default on either of them overruns
        # the advertised deadline again.
        #
        # Note this cannot be asserted end-to-end by timing the whole helper: the
        # collector it forks is killable, so SIGKILL always lands and the reap
        # returns at once. The 5 s waits are only reachable for a process that
        # survives SIGKILL (uninterruptible I/O), which a unit test cannot create --
        # hence the direct measurement above plus this structural check.
        import ast

        tree = ast.parse(textwrap.dedent(inspect.getsource(job._collect_cores_or_note)))
        for callee in ("_kill_process_group", "_reap"):
            call = next(
                (
                    n
                    for n in ast.walk(tree)
                    if isinstance(n, ast.Call)
                    and isinstance(n.func, ast.Name)
                    and n.func.id == callee
                    and any(kw.arg == "timeout" for kw in n.keywords)
                ),
                None,
            )
            assert call is not None, (
                f"{callee} must be called with an explicit timeout, or it falls back "
                "to a fixed wait on top of the deadline"
            )
            timeout = next(kw.value for kw in call.keywords if kw.arg == "timeout")
            names = {n.id for n in ast.walk(timeout) if isinstance(n, ast.Name)}
            assert "remaining" in names, (
                f"the {callee} timeout must come out of the remaining deadline, "
                f"not a constant: {ast.dump(timeout)}"
            )
    finally:
        # The cleanup above normally already killed and reaped it.
        try:
            os.kill(pid, signal.SIGKILL)
            os.waitpid(pid, 0)
        except (ProcessLookupError, ChildProcessError):
            pass


def test_reap_gives_up_on_an_unkillable_collector(monkeypatch):
    # SIGKILL does not free a process stuck in uninterruptible I/O, so a blocking
    # reap there would recreate the hang this wrapper prevents. Model that with a
    # waitpid that BLOCKS when asked to (no WNOHANG) and reports "still running"
    # when polled, so only the bounded path can return.
    def fake_waitpid(_pid, flags):
        if not flags & os.WNOHANG:
            time.sleep(60)  # a real blocking wait on an unkillable child
            raise AssertionError("unreachable")
        return (0, 0)

    monkeypatch.setattr(os, "waitpid", fake_waitpid)
    started = time.monotonic()
    assert job._reap(4242, timeout=0.3) == 0
    assert time.monotonic() - started < 5, "the bounded reap blocked anyway"


def test_reap_timeout_still_returns_the_status_when_it_arrives():
    pid = os.fork()
    if pid == 0:  # pragma: no cover - child
        os._exit(3)
    status = job._reap(pid, timeout=10)
    assert os.WIFEXITED(status) and os.WEXITSTATUS(status) == 3


def test_timeout_path_reaps_with_a_bound():
    # Pin the ARGUMENT: an unbounded reap on the timeout path is exactly the hang.
    import ast
    import inspect

    tree = ast.parse(inspect.getsource(job._collect_cores_or_note))
    reaps = [
        n
        for n in ast.walk(tree)
        if isinstance(n, ast.Call)
        and isinstance(n.func, ast.Name)
        and n.func.id == "_reap"
    ]
    assert any(
        any(k.arg == "timeout" for k in n.keywords) for n in reaps
    ), "the reap after killing a timed-out collector must be bounded"


def test_unusable_core_artifacts_rejects_a_truncated_cipher(workspace):
    # Utils.encrypt ignores openssl's exit codes, so a truncated write is only
    # visible in the STRUCTURE. Measured under an output size limit: openssl died
    # leaving a non-empty partial file, which any presence-only test accepts.
    enc = workspace / "core.4.zst.enc"
    enc.write_bytes(b"C" * (job.AES_BLOCK_BYTES * 3 + 5))
    rsa = workspace / "aes.key.rsa"
    rsa.write_bytes(b"W" * job.RSA_WRAPPED_KEY_BYTES)
    assert "truncated" in job._unusable_core_artifacts([enc, rsa])


def test_unusable_core_artifacts_rejects_a_block_aligned_partial(workspace):
    # Block alignment alone is not enough: the measured output-limit failure left
    # EXACTLY 1024 bytes, i.e. a perfectly aligned partial cipher. zstd keeps its
    # input, so the .zst the cipher was made from is still on disk and gives the
    # exact expected size.
    plaintext = workspace / "core.6.zst"
    plaintext.write_bytes(b"Z" * 200000)
    enc = workspace / "core.6.zst.enc"
    enc.write_bytes(b"C" * 1024)  # aligned, but far too small for 200000 bytes
    rsa = workspace / "aes.key.rsa"
    rsa.write_bytes(b"W" * job.RSA_WRAPPED_KEY_BYTES)
    assert "truncated" in job._unusable_core_artifacts([enc, rsa])


def test_unusable_core_artifacts_accepts_the_real_cipher_size(workspace):
    # The exact size openssl produces: a 16-byte salt header plus PKCS#7-padded
    # blocks. Verified against the real command over plaintexts 0..4096.
    plaintext = workspace / "core.7.zst"
    plaintext.write_bytes(b"Z" * 1000)
    enc = workspace / "core.7.zst.enc"
    enc.write_bytes(b"C" * 1024)  # 16 + (1000 // 16 + 1) * 16
    rsa = workspace / "aes.key.rsa"
    rsa.write_bytes(b"W" * job.RSA_WRAPPED_KEY_BYTES)
    assert job._unusable_core_artifacts([enc, rsa]) == ""


def test_unusable_core_artifacts_falls_back_without_the_plaintext(workspace):
    # An already-processed core from an earlier run has no .zst beside it; block
    # alignment is then all that can be checked.
    enc = workspace / "core.8.zst.enc"
    enc.write_bytes(b"C" * (job.AES_BLOCK_BYTES * 3 + 7))
    rsa = workspace / "aes.key.rsa"
    rsa.write_bytes(b"W" * job.RSA_WRAPPED_KEY_BYTES)
    assert "truncated" in job._unusable_core_artifacts([enc, rsa])


def test_unusable_core_artifacts_rejects_a_short_wrapped_key(workspace):
    # pkeyutl -encrypt against the 4096-bit key in ci/defs/public.pem emits
    # exactly 512 bytes; anything shorter cannot unwrap.
    enc = workspace / "core.4.zst.enc"
    enc.write_bytes(b"C" * (job.AES_BLOCK_BYTES * 4))
    rsa = workspace / "aes.key.rsa"
    rsa.write_bytes(b"W" * (job.RSA_WRAPPED_KEY_BYTES // 2))
    assert "wrapped AES key" in job._unusable_core_artifacts([enc, rsa])


def test_kill_process_group_survives_a_process_that_already_exited():
    # The collector may finish between the deadline expiring and the kill, so
    # os.getpgid can raise ProcessLookupError -- which would escape
    # _collect_cores_or_note and discard the classified Result.
    gone = 2**22 - 1  # above any plausible live pid on this host
    job._kill_process_group(gone)  # must not raise


def test_early_abort_keeps_a_genuine_parser_finding_over_the_marker(workspace):
    # The parser reads only the logs, so a real sanitizer / logical-error finding
    # survives a missing status.tsv -- and it must still outrank the marker there,
    # or a late `set -e` abort after the marker write relabels a real crash as the
    # generic memory-stuck row. Drives the shared production path.
    server_log = workspace / "server.log"
    server_log.write_text(
        "2026.07.25 00:00:00.000000 [ 1 ] {} <Fatal> : Logical error: "
        "Bad cast from type A to type B.\n"
        "2026.07.25 00:00:01.000000 [ 1 ] {} <Fatal> BaseDaemon: "
        "Child process was terminated by signal 9 (KILL)\n",
        encoding="utf-8",
    )
    marker = _fail(job.MEMORY_STUCK_NAME)
    selected = job._parse_and_select_failure(
        server_log,
        workspace / "stderr.log",
        workspace / "fuzzer.log",
        False,
        marker,
        None,
    )
    assert selected is not None
    assert "Logical error" in selected.name, (
        f"the genuine parser finding was demoted to {selected.name!r}"
    )


def test_early_abort_reports_a_startup_crash_with_no_marker(workspace):
    # The reported gap: a server can log a real finding and die during STARTUP --
    # before the probe loop that writes a marker is reached, and long before
    # status.tsv -- so the early abort had neither marker nor watchdog. Parsing was
    # gated on one of those existing, so this class was reported as a generic
    # harness error with the stable finding name and its reproduction files thrown
    # away. Same shared production path, marker=None and watchdog=None.
    server_log = workspace / "server.log"
    server_log.write_text(
        "2026.07.25 00:00:00.000000 [ 1 ] {} <Fatal> : Logical error: "
        "Bad cast from type A to type B.\n",
        encoding="utf-8",
    )
    selected = job._parse_and_select_failure(
        server_log,
        workspace / "stderr.log",
        workspace / "fuzzer.log",
        False,
        None,
        None,
    )
    assert selected is not None, (
        "a genuine startup-crash finding must survive a missing status.tsv even "
        "with no marker or watchdog to classify the run"
    )
    assert "Logical error" in selected.name, selected.name
    # And it must set the top-level status rather than being reported as a generic
    # harness ERROR: a missing status.tsv with a real finding is a test failure.
    assert job._early_abort_status(FileNotFoundError("x"), [selected]) is None
    # A FAULTY status.tsv is still a harness bug even with a finding attached.
    assert (
        job._early_abort_status(ValueError("malformed"), [selected])
        == Result.Status.ERROR
    )


def test_early_abort_parses_unconditionally():
    # Structural companion: `run_fuzz_job` drives docker and cannot run here, so pin
    # that the parse is NOT gated on a marker/watchdog. A behavioral test on the
    # shared helper cannot see a caller-side `if` reintroducing the gate.
    import ast
    import inspect

    tree = ast.parse(inspect.getsource(job.run_fuzz_job))
    calls = [
        n
        for n in ast.walk(tree)
        if isinstance(n, ast.Call)
        and isinstance(n.func, ast.Name)
        and n.func.id == "_parse_and_select_failure"
    ]
    # One call site here: the early abort. The normal path parses via
    # `_assemble_sub_results`, a separate function (pinned by its own tests).
    assert len(calls) == 1, f"expected the early abort to parse, got {len(calls)}"
    # The early-abort call must not sit under a test of marker_result/watchdog_result.
    for stmt in ast.walk(tree):
        if not isinstance(stmt, ast.If):
            continue
        guard = {n.id for n in ast.walk(stmt.test) if isinstance(n, ast.Name)}
        if not guard & {"marker_result", "watchdog_result"}:
            continue
        for node in ast.walk(stmt):
            if (
                isinstance(node, ast.Call)
                and isinstance(node.func, ast.Name)
                and node.func.id == "_parse_and_select_failure"
            ):
                raise AssertionError(
                    "the early-abort parse is gated on a marker/watchdog again: a "
                    "startup crash that dies before any marker loses its finding"
                )


def test_early_abort_claims_a_harness_stop_only_when_classified():
    # `server_stopped_by_harness=True` tells the parser to ignore the server's
    # signal line as self-inflicted. That is only true for a CLASSIFIED abort
    # (status.tsv is written after the graceful stop). An unclassified startup
    # failure had no stop at all, so hardcoding True there would suppress a genuine
    # server signal -- which may be the only evidence such a crash leaves.
    import ast
    import inspect

    tree = ast.parse(inspect.getsource(job.run_fuzz_job))
    for node in ast.walk(tree):
        if (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Name)
            and node.func.id == "_parse_and_select_failure"
        ):
            for kw in node.keywords:
                if kw.arg != "server_stopped_by_harness":
                    continue
                assert not (
                    isinstance(kw.value, ast.Constant) and kw.value.value is True
                ), (
                    "server_stopped_by_harness must be conditional on the run being "
                    "classified, not hardcoded True"
                )


def test_early_abort_falls_back_to_the_marker_when_the_parser_finds_nothing(workspace):
    # The marker stays the floor: a parser no-match must not erase it.
    (workspace / "server.log").write_text("nothing interesting\n", encoding="utf-8")
    marker = _fail(job.MEMORY_STUCK_NAME)
    selected = job._parse_and_select_failure(
        workspace / "server.log",
        workspace / "stderr.log",
        workspace / "fuzzer.log",
        False,
        marker,
        None,
    )
    assert selected is marker


def test_graceful_stop_paths_keep_a_genuine_sigkill(workspace):
    # A probes-stage watchdog and the early abort only stop the server GRACEFULLY;
    # neither sends SIGKILL. So the ClickHouse watchdog's "terminated by signal 9"
    # record there is the kernel OOM killer or another independent event, and
    # suppressing it would replace a real finding with the generic watchdog row.
    (workspace / "harness_watchdog.txt").write_text(
        "stage=probes reason=zero_answered_probes memory_limit_probes=60\n",
        encoding="utf-8",
    )
    (workspace / "server.log").write_text(
        "2026.07.28 00:00:00.000000 [ 1 ] {} <Trace> Application: Received signal 15\n"
        "2026.07.28 00:00:01.000000 [ 1 ] {} <Fatal> BaseDaemon: "
        "Child process was terminated by signal 9 (KILL)\n",
        encoding="utf-8",
    )
    selected = job._parse_and_select_failure(
        workspace / "server.log",
        workspace / "stderr.log",
        workspace / "fuzzer.log",
        False,
        None,
        job._harness_watchdog_result(),
        server_stopped_by_harness=True,
    )
    assert selected is not None
    assert "signal 9" in selected.name, (
        f"a genuine SIGKILL was suppressed on a graceful-stop path ({selected.name!r})"
    )


def test_marker_run_still_suppresses_our_own_sigkill(workspace):
    # The memory-stuck path DOES SIGKILL the server, so that record is ours and
    # must not become the failure row -- the marker is the classification.
    (workspace / "server_memory_stuck.txt").write_text(
        "probes=60 tier=patient\n", encoding="utf-8"
    )
    (workspace / "server.log").write_text(
        "2026.07.28 00:00:01.000000 [ 1 ] {} <Fatal> BaseDaemon: "
        "Child process was terminated by signal 9 (KILL)\n",
        encoding="utf-8",
    )
    selected = job._parse_and_select_failure(
        workspace / "server.log",
        workspace / "stderr.log",
        workspace / "fuzzer.log",
        False,
        job._memory_stuck_result(),
        None,
    )
    assert selected is not None
    assert selected.name == job.MEMORY_STUCK_NAME, (
        f"our own SIGKILL became the failure row ({selected.name!r})"
    )


def test_early_abort_keeps_a_reap_watchdog_over_our_own_sigterm(workspace):
    # A reap escalation leaves the server HEALTHY, so the stage-based self-kill
    # checks correctly do not fire -- but status.tsv is written after the graceful
    # stop, so reaching the early abort means we stopped the server ourselves.
    # Without the explicit flag the parser turns our own "Received signal 15" into
    # the failure row and buries the watchdog ERROR.
    (workspace / "harness_watchdog.txt").write_text(
        "stage=reap reason=client_unreapable waited=390s\n", encoding="utf-8"
    )
    (workspace / "server.log").write_text(
        "2026.07.28 00:00:00.000000 [ 1 ] {} <Trace> Application: "
        "Received signal 15\n",
        encoding="utf-8",
    )
    watchdog = job._harness_watchdog_result()
    assert watchdog is not None and watchdog.status == Result.Status.ERROR
    selected = job._parse_and_select_failure(
        workspace / "server.log",
        workspace / "stderr.log",
        workspace / "fuzzer.log",
        False,
        None,
        watchdog,
        server_stopped_by_harness=True,
    )
    assert selected is watchdog, (
        f"our own graceful-stop signal replaced the reap watchdog: {selected.name!r}"
    )


def test_early_abort_declares_the_server_already_stopped(workspace):
    # Pin the ARGUMENT, not just the call: the stage checks cannot see a reap-only
    # run, so dropping this keyword silently reintroduces the SIGTERM row.
    #
    # The value is now CONDITIONAL (`classified`) rather than a literal True,
    # because the early abort is also reached by an unclassified startup failure
    # where no stop ever ran -- claiming one there would suppress a genuine server
    # signal. So pin that the keyword is present and that it evaluates True for a
    # classified run: the behavioral half is asserted just above (a reap-only
    # watchdog survives our graceful-stop signal) and in
    # test_early_abort_claims_a_harness_stop_only_when_classified.
    import ast
    import inspect

    tree = ast.parse(inspect.getsource(job.run_fuzz_job))
    handler = next(
        h
        for h in ast.walk(tree)
        if isinstance(h, ast.ExceptHandler)
        and "_parse_and_select_failure"
        in {
            n.func.id
            for n in ast.walk(h)
            if isinstance(n, ast.Call) and isinstance(n.func, ast.Name)
        }
    )
    call = next(
        n
        for n in ast.walk(handler)
        if isinstance(n, ast.Call)
        and isinstance(n.func, ast.Name)
        and n.func.id == "_parse_and_select_failure"
    )
    flag = [k for k in call.keywords if k.arg == "server_stopped_by_harness"]
    assert flag, "the early abort must declare that the harness stopped the server"
    value = flag[0].value
    if isinstance(value, ast.Constant):
        assert value.value is True
    else:
        # A name, and it must be bound to the marker/watchdog presence test in the
        # same function -- not to something unrelated that merely happens to be
        # truthy on the classified path.
        assert isinstance(value, ast.Name), ast.dump(value)
        bound = [
            n
            for n in ast.walk(tree)
            if isinstance(n, ast.Assign)
            and any(
                isinstance(t, ast.Name) and t.id == value.id for t in n.targets
            )
        ]
        assert len(bound) == 1, f"`{value.id}` is not assigned exactly once"
        names = {n.id for n in ast.walk(bound[0].value) if isinstance(n, ast.Name)}
        assert {"marker_result", "watchdog_result"} <= names, (
            f"`{value.id}` must be derived from marker_result/watchdog_result "
            f"presence, got {sorted(names)}"
        )


def test_early_abort_runs_the_parser_before_completing(workspace):
    # Pins the CALL SITE inside the status.tsv handler: without it the promised
    # parser-first precedence silently does not apply on that path.
    import ast
    import inspect

    tree = ast.parse(inspect.getsource(job.run_fuzz_job))
    handlers = [n for n in ast.walk(tree) if isinstance(n, ast.ExceptHandler)]
    calling = [
        h
        for h in handlers
        if "_parse_and_select_failure"
        in {
            n.func.id
            for n in ast.walk(h)
            if isinstance(n, ast.Call) and isinstance(n.func, ast.Name)
        }
    ]
    assert calling, (
        "the early-abort handler must run the parser, or a real crash is relabelled"
    )
    # Presence is not reachability: gating the call on a constant-false test (or
    # any condition that cannot hold) leaves the AST unchanged while the parser
    # never runs. Require every enclosing `if` to have a non-constant test.
    handler = calling[0]
    target = next(
        n
        for n in ast.walk(handler)
        if isinstance(n, ast.Call)
        and isinstance(n.func, ast.Name)
        and n.func.id == "_parse_and_select_failure"
    )
    # The INNERMOST enclosing statement: `ast.walk` yields outer nodes first, so
    # taking the first match would pick the whole function and make the
    # reachability check below vacuous (its line span would cover everything).
    target_stmt = min(
        (
            n
            for n in ast.walk(handler)
            if isinstance(n, ast.stmt) and any(c is target for c in ast.walk(n))
        ),
        key=lambda n: (n.end_lineno or n.lineno) - n.lineno,
    )
    for node in ast.walk(handler):
        if isinstance(node, ast.If) and any(n is target for n in ast.walk(node.test)):
            continue
        if isinstance(node, ast.If) and any(n is target for n in ast.walk(node)):
            assert not isinstance(node.test, ast.Constant), (
                "the early-abort parser call is gated on a constant: it can never run"
            )
            # The gate must consult the markers, which is the only condition that
            # legitimately makes the parser unnecessary here.
            names = {n.id for n in ast.walk(node.test) if isinstance(n, ast.Name)}
            assert names & {"marker_result", "watchdog_result"}, (
                f"unexpected gate on the early-abort parser call: {ast.dump(node.test)}"
            )
    # And the selected row must replace the plain marker/watchdog sub-results, or
    # the parser output is computed and discarded.
    assigns = [
        n
        for n in ast.walk(handler)
        if isinstance(n, ast.Assign)
        and any(
            isinstance(t, ast.Name) and t.id == "sub_results" for t in n.targets
        )
        and any(isinstance(x, ast.Name) and x.id == "selected" for x in ast.walk(n.value))
    ]
    assert assigns, (
        "the parser-selected row must be attached on the early-abort path"
    )
    # ORDERING: `complete_job` exits the job, so a parser selection moved below it
    # is unreachable while every check above still holds. Require the parser call
    # (and the assignment that attaches its row) to precede the completion.
    complete_line = min(
        n.lineno
        for n in ast.walk(handler)
        if isinstance(n, ast.Call)
        and isinstance(n.func, ast.Attribute)
        and n.func.attr == "complete_job"
    )
    assert target.lineno < complete_line, (
        f"the early-abort parser call at line {target.lineno} must run BEFORE "
        f"complete_job at line {complete_line}, which exits the job"
    )
    assert min(a.lineno for a in assigns) < complete_line, (
        "the parser-selected row must be attached before complete_job exits"
    )
    # And it must be reachable at all, for the same reason the collection is.
    assert_statically_reachable(
        job.run_fuzz_job, target_stmt, "early-abort parser call"
    )
    # GUARD: the attachment is conditional, and the condition carries the whole
    # precedence. A marker or watchdog guarantees a non-null selection (that is
    # what _select_failure_result promises), so inverting this to `is None`
    # discards every genuine parser row and silently restores the generic
    # marker/watchdog result -- with the call, the assignment, the ordering and
    # the reachability all still satisfied. Pin the comparison itself.
    # The INNERMOST enclosing `if`: `ast.walk` yields outer nodes first, so the
    # first match is the marker/watchdog gate one level up, not this attachment's
    # own condition.
    enclosing_ifs = [
        n
        for n in ast.walk(handler)
        if isinstance(n, ast.If) and any(a is assigns[0] for a in ast.walk(n))
    ]
    guard = min(
        enclosing_ifs,
        key=lambda n: (n.end_lineno or n.lineno) - n.lineno,
        default=None,
    )
    assert guard is not None, "the parser-row attachment must stay conditional"
    assert isinstance(guard.test, ast.Compare), (
        f"unexpected guard on the parser-row attachment: {ast.dump(guard.test)}"
    )
    assert (
        isinstance(guard.test.left, ast.Name)
        and guard.test.left.id == "selected"
        and len(guard.test.ops) == 1
        and isinstance(guard.test.ops[0], ast.IsNot)
        and isinstance(guard.test.comparators[0], ast.Constant)
        and guard.test.comparators[0].value is None
    ), (
        "the parser row must be attached when `selected is not None`; the "
        f"inverted form discards every parser result: {ast.dump(guard.test)}"
    )


def test_early_abort_collects_cores_before_exiting(workspace):
    # The early-abort branch calls complete_job() (which sys.exits), so the
    # collection at the end of the normal path is unreachable from there: a late
    # `set -e` abort that still left a genuine core would lose it entirely,
    # because `paths` cannot carry a core (it may only leave the runner
    # encrypted). Pin the call site structurally -- live execution needs docker.
    import ast
    import inspect

    tree = ast.parse(inspect.getsource(job.run_fuzz_job))
    handlers = [n for n in ast.walk(tree) if isinstance(n, ast.ExceptHandler)]
    assert handlers, "the status.tsv read must stay wrapped in a try/except"
    collecting = [
        h
        for h in handlers
        if "_collect_cores_or_note"
        in {
            n.func.id
            for n in ast.walk(h)
            if isinstance(n, ast.Call) and isinstance(n.func, ast.Name)
        }
    ]
    assert collecting, (
        "the early-abort handler must collect cores before complete_job(); "
        "otherwise a run that aborted after writing a core uploads no core at all"
    )
    handler = collecting[0]
    # REACHABILITY: presence inside the handler is not enough either -- wrapping
    # the whole persist/collect/attach sequence in `if False:` keeps every ordering
    # relationship below true while collecting nothing.
    collect_stmts = [
        n
        for n in ast.walk(handler)
        if isinstance(n, ast.stmt)
        and any(
            isinstance(c, ast.Call)
            and isinstance(c.func, ast.Name)
            and c.func.id == "_collect_cores_or_note"
            for c in ast.walk(n)
        )
    ]
    for stmt in collect_stmts:
        assert_statically_reachable(job.run_fuzz_job, stmt, "early-abort core collection")
    # ORDERING: presence alone is not enough -- moving the collection after
    # complete_job() (which sys.exits) would leave this passing while the core is
    # never attached. Compare source line numbers within the handler.
    collect_line = min(
        n.lineno
        for n in ast.walk(handler)
        if isinstance(n, ast.Call)
        and isinstance(n.func, ast.Name)
        and n.func.id == "_collect_cores_or_note"
    )
    complete_line = min(
        n.lineno
        for n in ast.walk(handler)
        if isinstance(n, ast.Call)
        and isinstance(n.func, ast.Attribute)
        and n.func.attr == "complete_job"
    )
    assert collect_line < complete_line, (
        "cores must be collected BEFORE complete_job() exits the job "
        f"(collect at {collect_line}, complete_job at {complete_line})"
    )
    # DATA FLOW: the collected list must actually reach set_files, or the encrypted
    # core is computed and thrown away.
    attaching = [
        n
        for n in ast.walk(handler)
        if isinstance(n, ast.Call)
        and isinstance(n.func, ast.Attribute)
        and n.func.attr == "set_files"
        and any(
            isinstance(a, ast.Name) and a.id == "cores" for a in ast.walk(n)
        )
    ]
    assert attaching, (
        "the collected cores must be passed to set_files on the early-abort path"
    )
    assert min(n.lineno for n in attaching) < complete_line, (
        "cores must be attached before complete_job() exits"
    )
    # GUARD: the attachment is conditional on there BEING cores, and that
    # condition is load-bearing. Inverting it to `if not cores` attaches only the
    # empty list, so every successfully encrypted core is dropped while the call,
    # the ordering and the reachability above all still hold. Pin the condition.
    attach_stmt = min(
        (
            n
            for n in ast.walk(handler)
            if isinstance(n, ast.stmt)
            and any(c is attaching[0] for c in ast.walk(n))
        ),
        key=lambda n: (n.end_lineno or n.lineno) - n.lineno,
    )
    # Innermost enclosing `if`, for the same walk-order reason as above.
    guards = [
        n
        for n in ast.walk(handler)
        if isinstance(n, ast.If) and any(c is attach_stmt for c in ast.walk(n))
    ]
    guard = min(
        guards, key=lambda n: (n.end_lineno or n.lineno) - n.lineno, default=None
    )
    assert guard is not None, "the early-abort core attachment must stay conditional"
    assert isinstance(guard.test, ast.Name) and guard.test.id == "cores", (
        "the early-abort cores must be attached when `cores` is truthy; the "
        f"inverted form drops every collected core: {ast.dump(guard.test)}"
    )
    # Nothing in the job may call the raising collector directly any more.
    direct = [
        n
        for n in ast.walk(tree)
        if isinstance(n, ast.Call)
        and isinstance(n.func, ast.Attribute)
        and n.func.attr == "collect_cores"
    ]
    assert direct == [], (
        "collect_cores must be reached through _collect_cores_or_note, or a "
        "collector failure discards the classified Result"
    )


# --------------------------------------------------------------------------- #
# marker-present assembly (§3f): parser-first, marker floor, stable name
# --------------------------------------------------------------------------- #


def test_assembly_genuine_parser_finding_wins_over_marker():
    marker = _fail(job.MEMORY_STUCK_NAME)
    res = job._select_failure_result(
        "Logical error: Bad cast (STID: 1234-abcd)",
        "parser info",
        [],
        marker,
        None,
        marker_text="probes=60 tier=patient",
    )
    assert res.name == "Logical error: Bad cast (STID: 1234-abcd)"
    # Marker text is appended as context to the genuine finding.
    assert "probes=60 tier=patient" in res.info


def test_assembly_unknown_error_falls_back_to_marker():
    marker = _fail(job.MEMORY_STUCK_NAME)
    res = job._select_failure_result(
        FuzzerLogParser.UNKNOWN_ERROR, "Lost connection", [], marker, None
    )
    assert res is marker
    assert res.name == job.MEMORY_STUCK_NAME


def test_assembly_parser_memory_limit_name_falls_back_to_marker():
    # The parser's own memory-limit classification is the same physical state ->
    # one stable signature (the marker row), not two.
    marker = _fail(job.MEMORY_STUCK_NAME)
    res = job._select_failure_result(
        job.MEMORY_STUCK_NAME, "parser memory info", [], marker, None
    )
    assert res is marker


def test_assembly_watchdog_below_marker_above_nomatch():
    watchdog = _fail("Fuzzer harness watchdog fired", Result.Status.ERROR)
    # No marker, parser found nothing attributable -> watchdog row.
    res = job._select_failure_result(
        FuzzerLogParser.UNKNOWN_ERROR, "info", [], None, watchdog
    )
    assert res is watchdog


def test_assembly_marker_absent_parser_row_as_today():
    # No marker, genuine parser finding -> exactly the parser row (today's path).
    res = job._select_failure_result(
        "AddressSanitizer: heap-use-after-free (STID: 1)", "info", [], None, None
    )
    assert res.name == "AddressSanitizer: heap-use-after-free (STID: 1)"
    assert res.status == Result.Status.FAIL


def test_assembly_marker_absent_unknown_error_is_kept():
    # No marker / watchdog and only UNKNOWN_ERROR -> still surfaced (today's row).
    res = job._select_failure_result(
        FuzzerLogParser.UNKNOWN_ERROR, "Lost connection", [], None, None
    )
    assert res.name == FuzzerLogParser.UNKNOWN_ERROR


# wiring: run_fuzz_job must consult the helpers tested above
def test_run_fuzz_job_wires_the_production_helpers():
    # The helpers above are tested directly; this pins their CALL SITES: a
    # refactor that stops run_fuzz_job from consulting any of them must fail
    # the suite, not just the helper tests. co_names lists every global name
    # referenced by the function body (no execution, no mocking).
    referenced = set(job.run_fuzz_job.__code__.co_names)
    for helper in (
        "_clean_stale_run_state",
        "_memory_stuck_result",
        "_harness_watchdog_result",
        "_oom_leniency_granted",
        "_force_fail_for_markers",
        "_top_level_status",
        "_early_abort_status",
        "_collect_cores_or_note",
        # The normal path reaches the parser through _assemble_sub_results (which
        # the behavioral tests above drive directly); the early-abort branch still
        # calls it inline.
        "_assemble_sub_results",
        "_parse_and_select_failure",
    ):
        assert helper in referenced, f"run_fuzz_job no longer calls {helper}"
    assert "_parse_and_select_failure" in set(
        job._assemble_sub_results.__code__.co_names
    ), "_assemble_sub_results no longer consults the parser"
    assert "_should_parse_logs" in set(
        job._assemble_sub_results.__code__.co_names
    ), "_assemble_sub_results no longer gates on _should_parse_logs"
    # The parser wiring moved into _parse_and_select_failure, which is shared by
    # the normal path and the early abort: pin its call sites there instead, so a
    # refactor cannot quietly drop the self-kill discrimination or the selection.
    shared = set(job._parse_and_select_failure.__code__.co_names)
    for helper in (
        "_watchdog_stage_teardown",
        "_watchdog_stage_probes",
        "_select_failure_result",
        "FuzzerLogParser",
    ):
        assert helper in shared, f"_parse_and_select_failure no longer uses {helper}"


def _stub_parse(monkeypatch, selected):
    """Make `_parse_and_select_failure` return `selected` and record its call."""
    calls = []

    def fake(*args, **kwargs):
        calls.append((args, kwargs))
        return selected

    monkeypatch.setattr(job, "_parse_and_select_failure", fake)
    return calls


def _row(name):
    return Result.create_from(name=name, status=Result.Status.ERROR, results=[])


def test_assemble_sub_results_attaches_a_parser_finding(monkeypatch):
    # The parse branch must return the parser's row: dropping the attachment (or
    # inverting its `is not None` guard) loses a genuine finding.
    found = _row("logical error")
    calls = _stub_parse(monkeypatch, found)
    out = job._assemble_sub_results(
        True, Result.Status.FAIL, 1, "s.log", "e.log", "f.log", False, None, None
    )
    assert out == [found], "the parser-selected row must be attached"
    assert len(calls) == 1, "the parser must be consulted exactly once"


def test_assemble_sub_results_drops_a_parser_no_match(monkeypatch):
    # And a no-match must attach nothing rather than a None row, which would make
    # the report carry an empty sub-result.
    _stub_parse(monkeypatch, None)
    out = job._assemble_sub_results(
        True, Result.Status.FAIL, 1, "s.log", "e.log", "f.log", False, None, None
    )
    assert out == [], f"a parser no-match must attach nothing, got {out}"


@pytest.mark.parametrize(
    "marker, watchdog, expected",
    [
        (True, False, ["memory stuck"]),
        (False, True, ["watchdog"]),
        (True, True, ["memory stuck", "watchdog"]),
        (False, False, []),
    ],
)
def test_assemble_sub_results_surfaces_the_markers_when_parsing_is_skipped(
    monkeypatch, marker, watchdog, expected
):
    # The skip path is what makes a reap-only run report ERROR instead of FAIL, so
    # a present marker/watchdog must be surfaced and an absent one must not be.
    calls = _stub_parse(monkeypatch, _row("must not be used"))
    marker_result = _row("memory stuck") if marker else None
    watchdog_result = _row("watchdog") if watchdog else None
    out = job._assemble_sub_results(
        False,
        Result.Status.ERROR,
        0,
        "s.log",
        "e.log",
        "f.log",
        False,
        marker_result,
        watchdog_result,
    )
    assert [r.name for r in out] == expected, (
        f"skip path attached {[r.name for r in out]}, expected {expected}"
    )
    assert calls == [], "the parser must NOT run when _should_parse_logs is False"
    assert all(r is not None for r in out), "no None rows may be attached"


def test_assemble_sub_results_keeps_a_coexisting_watchdog_on_the_parse_path():
    # A reap watchdog is written before the probe loop, and the memory-stuck paths
    # set server_died=1, so both records can exist on the PARSE path.
    # _select_failure_result returns one row (the marker outranks the watchdog), so
    # without this the watchdog is dropped and its ERROR is downgraded to the
    # marker's FAIL -- while the identical state reports ERROR on the skip path.
    marker = Result.create_from(
        name="memory stuck", status=Result.Status.FAIL, results=[]
    )
    watchdog = Result.create_from(name="watchdog", status=Result.Status.ERROR, results=[])
    selected = job._select_failure_result("", "", [], marker, watchdog, "stuck")
    assert selected is marker, "the marker is expected to outrank the watchdog here"
    original = job._parse_and_select_failure
    try:
        job._parse_and_select_failure = lambda *a, **k: selected
        out = job._assemble_sub_results(
            True, Result.Status.FAIL, 1, "s", "e", "f", False, marker, watchdog
        )
    finally:
        job._parse_and_select_failure = original
    assert [r.name for r in out] == ["memory stuck", "watchdog"], (
        f"the coexisting watchdog must be kept, got {[r.name for r in out]}"
    )
    # And the derived status must stay ERROR, which is the whole point.
    final = Result.create_from(
        name="job", status=job._top_level_status(Result.Status.FAIL, out), results=out
    )
    assert final.status == Result.Status.ERROR, (
        f"a watchdog run must report ERROR, got {final.status}"
    )


def test_assemble_sub_results_does_not_duplicate_the_selected_watchdog(monkeypatch):
    # When the watchdog IS the selection (no marker), it must appear once.
    watchdog = Result.create_from(name="watchdog", status=Result.Status.ERROR, results=[])
    selected = job._select_failure_result("", "", [], None, watchdog, "")
    assert selected is watchdog
    _stub_parse(monkeypatch, selected)
    out = job._assemble_sub_results(
        True, Result.Status.FAIL, 1, "s", "e", "f", False, None, watchdog
    )
    assert [r.name for r in out] == ["watchdog"], (
        f"the watchdog must not be duplicated, got {[r.name for r in out]}"
    )


def test_assemble_sub_results_keeps_the_watchdog_beside_a_real_finding(monkeypatch):
    # A genuine parser finding outranks the watchdog for the headline row, but the
    # watchdog record must still be surfaced (and still force ERROR).
    watchdog = Result.create_from(name="watchdog", status=Result.Status.ERROR, results=[])
    found = Result.create_from(
        name="logical error", status=Result.Status.FAIL, results=[]
    )
    _stub_parse(monkeypatch, found)
    out = job._assemble_sub_results(
        True, Result.Status.FAIL, 1, "s", "e", "f", False, None, watchdog
    )
    assert [r.name for r in out] == ["logical error", "watchdog"], (
        f"expected the finding and the watchdog, got {[r.name for r in out]}"
    )


def test_with_watchdog_keeps_a_coexisting_record_for_the_early_abort_path():
    # The early-abort handler reduces its sub-results to the single selection too,
    # so it needs the same merge: a reap watchdog can precede the memory marker and
    # the runner can then abort before completing status.tsv. Without this the
    # watchdog is dropped and the promised ERROR becomes the marker's FAIL.
    marker = Result.create_from(
        name="memory stuck", status=Result.Status.FAIL, results=[]
    )
    watchdog = Result.create_from(name="watchdog", status=Result.Status.ERROR, results=[])
    selected = job._select_failure_result("", "", [], marker, watchdog, "stuck")
    sub_results = job._with_watchdog(selected, watchdog)
    assert [r.name for r in sub_results] == ["memory stuck", "watchdog"], (
        f"the coexisting watchdog must survive, got {[r.name for r in sub_results]}"
    )
    final = Result.create_from(
        name="job",
        status=job._early_abort_status(FileNotFoundError("status.tsv"), sub_results),
        results=sub_results,
    )
    assert final.status == Result.Status.ERROR, (
        f"a watchdog run aborting on a missing status.tsv must report ERROR, got "
        f"{final.status}"
    )


def test_with_watchdog_does_not_duplicate_or_invent_rows():
    watchdog = Result.create_from(name="watchdog", status=Result.Status.ERROR, results=[])
    # The watchdog IS the selection -> exactly one row.
    assert [r.name for r in job._with_watchdog(watchdog, watchdog)] == ["watchdog"]
    # No selection and no watchdog -> nothing.
    assert job._with_watchdog(None, None) == []


def test_early_abort_handler_merges_the_watchdog():
    # And the handler must actually use the merge rather than reducing to [selected].
    import ast
    import inspect

    tree = ast.parse(inspect.getsource(job.run_fuzz_job))
    handler = next(
        h
        for h in ast.walk(tree)
        if isinstance(h, ast.ExceptHandler)
        and any(
            isinstance(n, ast.Call)
            and isinstance(n.func, ast.Name)
            and n.func.id == "_parse_and_select_failure"
            for n in ast.walk(h)
        )
    )
    merges = [
        n
        for n in ast.walk(handler)
        if isinstance(n, ast.Call)
        and isinstance(n.func, ast.Name)
        and n.func.id == "_with_watchdog"
    ]
    assert merges, (
        "the early-abort handler must build its sub-results through _with_watchdog, "
        "or a coexisting watchdog is dropped"
    )


def test_the_production_call_sites_forward_the_marker_and_watchdog():
    # The helpers are covered behaviorally above, but they only protect the report
    # if the job HANDS THEM the real records. Passing None for `watchdog_result` at
    # either call site silently downgrades a watchdog run to FAIL, and dropping
    # `marker_result` from the early parser call loses the stable memory-stuck row,
    # with every other assertion still satisfied. Pin the forwarded names.
    import ast
    import inspect

    # Both the job and the assembly helper make these calls, and the helper holds
    # the normal path's parser call, so check both function bodies.
    tree = ast.parse(
        inspect.getsource(job.run_fuzz_job)
        + "\n"
        + textwrap.dedent(inspect.getsource(job._assemble_sub_results))
    )

    # The parameter POSITION matters: the two records have different precedence
    # (the marker outranks the watchdog) and different signal-filter semantics, so
    # a swap makes a memory marker behave as a watchdog and lets the harness's own
    # SIGKILL row replace the stable memory-stuck classification. Resolve each
    # name to the callee's own signature rather than hardcoding indices.
    required = {
        # callee: the parameter names whose forwarding the report depends on
        job._with_watchdog: ("watchdog_result",),
        job._assemble_sub_results: ("marker_result", "watchdog_result"),
        job._parse_and_select_failure: ("marker_result", "watchdog_result"),
    }
    by_name = {fn.__name__: (fn, params) for fn, params in required.items()}

    def forwarded_name(call, fn, param):
        """The name this call passes for `param`, by keyword or by position."""
        for kw in call.keywords:
            if kw.arg == param:
                return kw.value.id if isinstance(kw.value, ast.Name) else None
        names = list(inspect.signature(fn).parameters)
        index = names.index(param)
        if index < len(call.args):
            arg = call.args[index]
            return arg.id if isinstance(arg, ast.Name) else None
        return None

    seen = set()
    for node in ast.walk(tree):
        if not (isinstance(node, ast.Call) and isinstance(node.func, ast.Name)):
            continue
        entry = by_name.get(node.func.id)
        if entry is None:
            continue
        fn, params = entry
        seen.add(node.func.id)
        for param in params:
            assert forwarded_name(node, fn, param) == param, (
                f"the {node.func.id} call at line {node.lineno} passes "
                f"{forwarded_name(node, fn, param)!r} as {param!r}: the "
                "marker/watchdog records must reach their own parameters, or a "
                "watchdog run reports FAIL and the memory-stuck row is replaced"
            )
    assert seen == set(by_name), (
        f"expected calls to {sorted(by_name)}, found {sorted(seen)}"
    )


def test_the_assembly_result_reaches_the_reported_results():
    # Calling the helper and throwing its return value away loses every parser,
    # marker and watchdog row while the wiring assertions stay satisfied.
    import ast
    import inspect

    tree = ast.parse(inspect.getsource(job.run_fuzz_job))
    assigns = [
        n
        for n in ast.walk(tree)
        if isinstance(n, ast.Assign)
        and any(isinstance(t, ast.Name) and t.id == "results" for t in n.targets)
        and isinstance(n.value, ast.Call)
        and isinstance(n.value.func, ast.Name)
        and n.value.func.id == "_assemble_sub_results"
    ]
    assert len(assigns) == 1, (
        "`results` must be assigned from _assemble_sub_results exactly once, so the "
        f"assembled rows are the ones reported (found {len(assigns)})"
    )
    # And nothing may overwrite it afterwards with something else.
    later = [
        n
        for n in ast.walk(tree)
        if isinstance(n, ast.Assign)
        and any(isinstance(t, ast.Name) and t.id == "results" for t in n.targets)
        and n is not assigns[0]
    ]
    assert later == [], (
        "`results` is reassigned after the assembly, which discards the assembled "
        f"rows (line {later[0].lineno if later else '-'})"
    )
    # And the assignment must be reachable: under `if False` the AST is unchanged
    # while production reaches Result.create_from with `results` unbound.
    assert_statically_reachable(
        job.run_fuzz_job, assigns[0], "the sub-result assembly"
    )


def test_force_fail_for_markers_receives_the_real_records():
    # With None passed instead, a watchdog run keeps is_failed False, so the marker,
    # the logs, the pre-core persisted result and the cores are all skipped.
    import ast
    import inspect

    tree = ast.parse(inspect.getsource(job.run_fuzz_job))
    calls = [
        n
        for n in ast.walk(tree)
        if isinstance(n, ast.Call)
        and isinstance(n.func, ast.Name)
        and n.func.id == "_force_fail_for_markers"
    ]
    assert len(calls) == 1, f"expected one _force_fail_for_markers call, got {len(calls)}"
    # Both outputs must be kept: dropping `is_failed` (`status, _ = ...`) still
    # classifies the failure but skips the artifact, persistence and core block.
    assignment = next(
        (
            n
            for n in ast.walk(tree)
            if isinstance(n, ast.Assign) and any(c is calls[0] for c in ast.walk(n))
        ),
        None,
    )
    assert assignment is not None, "_force_fail_for_markers result must be assigned"
    assert len(assignment.targets) == 1 and isinstance(
        assignment.targets[0], ast.Tuple
    ), "_force_fail_for_markers must assign both of its outputs"
    assert [
        e.id if isinstance(e, ast.Name) else None
        for e in assignment.targets[0].elts
    ] == ["status", "is_failed"], (
        "both `status` and `is_failed` must be kept, or a marked run skips its "
        "logs, marker and cores"
    )
    assert_statically_reachable(
        job.run_fuzz_job, assignment, "the marker force-fail"
    )
    names = list(inspect.signature(job._force_fail_for_markers).parameters)
    for param in ("marker_result", "watchdog_result"):
        index = names.index(param)
        arg = calls[0].args[index] if index < len(calls[0].args) else None
        got = arg.id if isinstance(arg, ast.Name) else None
        assert got == param, (
            f"_force_fail_for_markers receives {got!r} as {param!r}: a marked run "
            "would keep is_failed False and skip its logs, marker and cores"
        )


def test_ownership_repair_skip_requires_BOTH_records_absent():
    # `and` -> `or` restores the unbounded repair on every ordinary marked run
    # (which normally carries exactly one of the two records).
    import ast
    import inspect

    tree = ast.parse(inspect.getsource(job.run_fuzz_job))
    # Several `if`s mention `unreadable`; the gate is the one that also weighs the
    # marker/watchdog records.
    tests = [
        n.test
        for n in ast.walk(tree)
        if isinstance(n, ast.If)
        and {
            c.id for c in ast.walk(n.test) if isinstance(c, ast.Name)
        }
        >= {"unreadable", "marker_result", "watchdog_result"}
    ]
    assert len(tests) == 1, f"expected one ownership-repair gate, got {len(tests)}"
    marker_clause = next(
        (
            n
            for n in ast.walk(tests[0])
            if isinstance(n, ast.BoolOp)
            and isinstance(n.op, ast.And)
            and {
                c.id
                for c in ast.walk(n)
                if isinstance(c, ast.Name)
            }
            >= {"marker_result", "watchdog_result"}
        ),
        None,
    )
    assert marker_clause is not None, (
        "the ownership-repair gate must skip only when BOTH the marker and the "
        "watchdog are absent (an `or` fires the unbounded repair on marked runs)"
    )
    # Polarity too: `marker_result is not None and watchdog_result is None` keeps
    # the `And` and both names while running the unbounded repair on ordinary
    # marker-only failures.
    compared = {}
    for cmp_node in marker_clause.values:
        assert isinstance(cmp_node, ast.Compare), (
            f"unexpected clause in the ownership gate: {ast.dump(cmp_node)}"
        )
        assert isinstance(cmp_node.left, ast.Name), ast.dump(cmp_node)
        assert len(cmp_node.ops) == 1 and isinstance(cmp_node.ops[0], ast.Is), (
            f"{cmp_node.left.id} must be compared with `is None`, not "
            f"{type(cmp_node.ops[0]).__name__}"
        )
        assert (
            isinstance(cmp_node.comparators[0], ast.Constant)
            and cmp_node.comparators[0].value is None
        ), ast.dump(cmp_node)
        compared[cmp_node.left.id] = True
    assert set(compared) == {"marker_result", "watchdog_result"}, (
        f"the gate must test exactly the two records, got {sorted(compared)}"
    )
    # `unreadable` must be a POSITIVE operand: `or not unreadable` runs the
    # possibly-hanging repair on readable marked runs and skips it precisely when
    # the marked artifacts really are unreadable.
    assert any(
        isinstance(v, ast.Name) and v.id == "unreadable" for v in tests[0].values
    ), (
        "the ownership gate must repair when `unreadable` is true, not when it is "
        f"false: {ast.dump(tests[0])}"
    )


def test_stale_state_is_cleaned_before_the_container_runs():
    # Cleaning up afterwards would let the container read stale markers and append
    # to stale logs, then delete the fresh outputs before classification.
    import ast
    import inspect

    tree = ast.parse(inspect.getsource(job.run_fuzz_job))
    cleanups = [
        n.lineno
        for n in ast.walk(tree)
        if isinstance(n, ast.Call)
        and isinstance(n.func, ast.Name)
        and n.func.id == "_clean_stale_run_state"
    ]
    runs = [
        n.lineno
        for n in ast.walk(tree)
        if isinstance(n, ast.Call)
        and isinstance(n.func, ast.Attribute)
        and n.func.attr == "check"
        and any(
            isinstance(a, ast.Name) and a.id == "run_command"
            for a in ast.walk(n)
        )
    ]
    assert cleanups and runs, (
        f"expected a cleanup and a container run, got {cleanups} and {runs}"
    )
    assert max(cleanups) < min(runs), (
        f"_clean_stale_run_state (line {max(cleanups)}) must run BEFORE the "
        f"container (line {min(runs)})"
    )
    # Line order is not execution: under `if False` the cleanup still precedes the
    # container in the source while stale markers and logs reach the run.
    cleanup_stmt = min(
        (
            n
            for n in ast.walk(tree)
            if isinstance(n, ast.stmt)
            and any(
                isinstance(c, ast.Call)
                and isinstance(c.func, ast.Name)
                and c.func.id == "_clean_stale_run_state"
                for c in ast.walk(n)
            )
        ),
        key=lambda n: (n.end_lineno or n.lineno) - n.lineno,
    )
    assert_statically_reachable(job.run_fuzz_job, cleanup_stmt, "the stale-state cleanup")


def test_the_marker_and_watchdog_come_from_their_own_producers():
    # Every forwarding test above pins names and positions, but not WHICH producer
    # filled each name. Swapping the two source assignments keeps them all green
    # while making a memory marker follow watchdog precedence and signal-filter
    # semantics, so the harness's own SIGKILL row can displace the stable
    # memory-stuck classification.
    import ast
    import inspect

    tree = ast.parse(inspect.getsource(job.run_fuzz_job))
    expected = {
        "marker_result": "_memory_stuck_result",
        "watchdog_result": "_harness_watchdog_result",
    }
    found = {}
    for node in ast.walk(tree):
        if not isinstance(node, ast.Assign) or len(node.targets) != 1:
            continue
        target = node.targets[0]
        if not (isinstance(target, ast.Name) and target.id in expected):
            continue
        assert isinstance(node.value, ast.Call) and isinstance(
            node.value.func, ast.Name
        ), f"{target.id} must be assigned from a producer call: {ast.dump(node)}"
        found[target.id] = node.value.func.id
        assert_statically_reachable(job.run_fuzz_job, node, f"the {target.id} producer")
    assert found == expected, (
        f"the records are bound to the wrong producers: {found} != {expected}"
    )


def test_assemble_sub_results_is_wired_into_the_job():
    # The helper only protects the report if run_fuzz_job actually uses it, and the
    # inline assembly must be gone so there is one implementation to test.
    import ast
    import inspect

    assert "_assemble_sub_results" in set(job.run_fuzz_job.__code__.co_names), (
        "run_fuzz_job must build its sub-results through _assemble_sub_results"
    )
    tree = ast.parse(inspect.getsource(job.run_fuzz_job))
    handler_nodes = {
        n
        for h in ast.walk(tree)
        if isinstance(h, ast.ExceptHandler)
        for n in ast.walk(h)
    }
    outside_parser_calls = [
        n
        for n in ast.walk(tree)
        if isinstance(n, ast.Call)
        and isinstance(n.func, ast.Name)
        and n.func.id == "_parse_and_select_failure"
        and n not in handler_nodes
    ]
    assert outside_parser_calls == [], (
        "the normal-path parser call must live in _assemble_sub_results, not "
        "inline in run_fuzz_job, so it is covered by the behavioral tests"
    )


def test_top_level_status_is_called_with_the_attached_results():
    # Name-only wiring cannot see the ARGUMENTS: _top_level_status(status, [])
    # keeps every other test green while a watchdog-only run silently reports FAIL
    # instead of ERROR, because an empty list makes the helper return the
    # intermediate status verbatim. Pin the two names the call must pass, read
    # from the source (no execution, no live server).
    import ast
    import inspect

    tree = ast.parse(inspect.getsource(job.run_fuzz_job))
    calls = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "_top_level_status"
    ]
    assert len(calls) == 1, f"expected one _top_level_status call, found {len(calls)}"
    args = calls[0].args
    assert [type(a) for a in args] == [ast.Name, ast.Name], (
        "_top_level_status must be called with the status and results variables"
    )
    assert [a.id for a in args] == ["status", "results"], (
        f"_top_level_status called with {[ast.dump(a) for a in args]}: it must get "
        "the same results list that is attached to the Result, or the derived "
        "status will not reflect the attached sub-results"
    )
    # And the Result built from that status must attach the same list: the job has
    # several create_from calls (the SKIPPED short-circuit and the early abort),
    # so pick the one whose status IS this helper call.
    assembly = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "create_from"
        and any(
            k.arg == "status" and k.value is calls[0] for k in node.keywords
        )
    ]
    assert len(assembly) == 1, (
        "expected exactly one Result.create_from whose status is _top_level_status(...)"
    )
    results_kw = [k for k in assembly[0].keywords if k.arg == "results"]
    assert results_kw, "that Result.create_from must be given the results"
    assert "results" in {
        n.id for n in ast.walk(results_kw[0].value) if isinstance(n, ast.Name)
    }, "the attached results must include the same list passed to _top_level_status"


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
