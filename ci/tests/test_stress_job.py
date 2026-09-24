"""
Tests for `select_replica_failures` (ci/jobs/stress_job.py).

The stress job hands each replica's server and stderr log families to the parser
and must not let a failure on one replica hide a (possibly higher-signal) failure
on another: every replica is scanned, all distinct specific classifications are
reported, and a generic `<Fatal>`, the memory limit or an expected-only line /
"Unknown error" is used only when nothing better was found on any replica.
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from ci.jobs.scripts.log_parser import FuzzerLogParser
from ci.jobs.stress_job import (
    oom_explains_failure,
    select_replica_failures,
    server_log_reports_oom,
)
from ci.praktika.result import Result

_ORACLE_MISMATCH_LOG = (
    "2026.09.04 00:44:57.972626 [ 1068 ] {q} <Fatal> ASTFuzzer: "
    "AST Fuzzer oracle mismatch detected!\n"
    "Fuzzed query: SELECT g FROM t GROUP BY g\n"
    "TLP Aggregate oracle mismatch!\n"
    "2026.09.04 00:44:58.000000 [ 1068 ] {} <Information> Application: shutting down\n"
)
_ASAN_STDERR = (
    "==1234==ERROR: AddressSanitizer: heap-use-after-free on address 0x1\n"
    "    #0 0x55b9b8db9bc7 in DB::Foo::bar() src/Foo.cpp:10:5\n"
    "SUMMARY: AddressSanitizer: heap-use-after-free src/Foo.cpp:10:5\n"
)
_GENERIC_FATAL_LOG = (
    "2026.09.04 00:44:57.972626 [ 1068 ] {q} <Fatal> SomeComponent: "
    "transient generic fatal\n"
    "2026.09.04 00:44:58.000000 [ 1068 ] {} <Information> Application: shutting down\n"
)
_UNKNOWN_LOG = (
    "2026.09.04 00:44:57.900000 [ 1068 ] {q} <Debug> executeQuery: SELECT 1\n"
    "2026.09.04 00:44:58.000000 [ 1068 ] {} <Information> Application: shutting down\n"
)


def _pair(tmp_path, replica, server_text=None, stderr_text=None):
    # Build one (replica_name, server_log_files, stderr_log_files) triple. A missing
    # log is left out of its list, matching what the job passes when a file is absent.
    server_log = tmp_path / f"clickhouse-server-{replica}.err.log"
    stderr_log = tmp_path / f"stderr-{replica}.log"
    server_logs, stderr_logs = [], []
    if server_text is not None:
        server_log.write_text(server_text, encoding="utf-8")
        server_logs.append(server_log)
    if stderr_text is not None:
        stderr_log.write_text(stderr_text, encoding="utf-8")
        stderr_logs.append(stderr_log)
    return (replica, server_logs, stderr_logs)


def test_specific_failure_on_second_replica_not_hidden_by_first(tmp_path):
    # Main replica has an oracle mismatch, sc1 has an ASan report. Both are
    # specific classifications and must both be reported - crucially the ASan
    # report on the second replica is not suppressed by the first.
    pairs = [
        _pair(tmp_path, "main", server_text=_ORACLE_MISMATCH_LOG),
        _pair(tmp_path, "sc1", server_text=_UNKNOWN_LOG, stderr_text=_ASAN_STDERR),
    ]

    results = select_replica_failures(pairs).results
    names = [name for name, _, _ in results]

    assert any(n.startswith("AST Fuzzer oracle mismatch") for n in names), names
    assert any(n.startswith("AddressSanitizer") for n in names), names
    assert len(results) == 2


def test_generic_fatal_does_not_suppress_sanitizer_on_second_replica(tmp_path):
    # Main replica has only a generic <Fatal>, sc1 has an ASan report. The
    # specific sanitizer failure wins; the generic fatal is dropped.
    pairs = [
        _pair(tmp_path, "main", server_text=_GENERIC_FATAL_LOG),
        _pair(tmp_path, "sc1", server_text=_UNKNOWN_LOG, stderr_text=_ASAN_STDERR),
    ]

    results = select_replica_failures(pairs).results
    names = [name for name, _, _ in results]

    assert len(results) == 1
    assert names[0].startswith("AddressSanitizer")
    assert "transient generic fatal" not in names[0]


def test_same_specific_failure_across_replicas_reported_once(tmp_path):
    # Replicated setups surface the same failure on several replicas; report a
    # given specific classification only once.
    pairs = [
        _pair(tmp_path, "main", server_text=_ORACLE_MISMATCH_LOG),
        _pair(tmp_path, "sc1", server_text=_ORACLE_MISMATCH_LOG),
    ]

    results = select_replica_failures(pairs).results

    assert len(results) == 1
    assert results[0][0].startswith("AST Fuzzer oracle mismatch")


def test_generic_fatal_used_when_no_specific_failure(tmp_path):
    # No replica has a specific classification: the generic <Fatal> is reported
    # (once) rather than "Unknown error".
    pairs = [
        _pair(tmp_path, "main", server_text=_GENERIC_FATAL_LOG),
        _pair(tmp_path, "sc1", server_text=_UNKNOWN_LOG),
    ]

    results = select_replica_failures(pairs).results

    assert len(results) == 1
    assert results[0][0] == "SomeComponent: transient generic fatal"


def test_unknown_error_when_nothing_classified(tmp_path):
    # Neither specific nor generic fatal anywhere: a single "Unknown error".
    pairs = [
        _pair(tmp_path, "main", server_text=_UNKNOWN_LOG),
        _pair(tmp_path, "sc1", server_text=_UNKNOWN_LOG),
    ]

    results = select_replica_failures(pairs).results

    assert len(results) == 1
    assert results[0][0] == FuzzerLogParser.UNKNOWN_ERROR


_EXPECTED_KILL_LOG = (
    "2026.09.04 00:44:57.900000 [ 1068 ] {q} <Debug> executeQuery: SELECT 1\n"
    "2026.09.04 00:44:58.000000 [ 1 ] {} <Fatal> Application: "
    "Child process was terminated by signal 9 (KILL).\n"
)
_MEMORY_LIMIT_LOG = (
    "2026.09.04 00:44:57.900000 [ 1068 ] {q} <Error> executeQuery: Code: 241. "
    "DB::Exception: (total) memory limit exceeded: would use 10.00 GiB\n"
)
_SANITIZER_OOM_STDERR = (
    "==1234==ERROR: AddressSanitizer: out of memory: allocator is trying to "
    "allocate 0x100000000 bytes\n"
)


def test_expected_kill_only_is_flagged_not_reported_as_crash(tmp_path):
    # Every replica ended with the expected SIGKILL and nothing else: the run is named
    # after it, but flagged as expected-only so the job can decide not to fail on it.
    pairs = [
        _pair(tmp_path, "main", server_text=_EXPECTED_KILL_LOG),
        _pair(tmp_path, "sc1", server_text=_EXPECTED_KILL_LOG),
    ]

    failures = select_replica_failures(pairs)

    assert len(failures.results) == 1
    assert failures.results[0][0].startswith("Child process was terminated by signal 9")
    assert failures.expected_only is True
    assert failures.expected_only_oom is False
    assert failures.crash_named is False


def test_sanitizer_oom_only_is_flagged_as_oom(tmp_path):
    pairs = [
        _pair(
            tmp_path,
            "main",
            server_text=_UNKNOWN_LOG,
            stderr_text=_SANITIZER_OOM_STDERR,
        ),
    ]

    failures = select_replica_failures(pairs)

    assert failures.expected_only is True
    assert failures.expected_only_oom is True
    assert failures.crash_named is False


def test_generic_fatal_on_other_replica_beats_expected_kill(tmp_path):
    # The expected kill on the first replica must not name the run before the second
    # replica's unclassified <Fatal> is seen; that fatal is a crash.
    pairs = [
        _pair(tmp_path, "main", server_text=_EXPECTED_KILL_LOG),
        _pair(tmp_path, "sc1", server_text=_GENERIC_FATAL_LOG),
    ]

    failures = select_replica_failures(pairs)

    assert [n for n, _, _ in failures.results] == [
        "SomeComponent: transient generic fatal"
    ]
    assert failures.crash_named is True
    assert failures.expected_only is False


def test_memory_limit_ranks_below_crash_and_above_expected_only(tmp_path):
    memory_only = select_replica_failures(
        [
            _pair(tmp_path, "main", server_text=_MEMORY_LIMIT_LOG),
            _pair(tmp_path, "sc1", server_text=_EXPECTED_KILL_LOG),
        ]
    )
    assert [n for n, _, _ in memory_only.results] == [
        FuzzerLogParser.MEMORY_LIMIT_ERROR
    ]
    assert memory_only.crash_named is False
    assert memory_only.expected_only is False

    with_crash = select_replica_failures(
        [
            _pair(tmp_path, "main", server_text=_MEMORY_LIMIT_LOG),
            _pair(tmp_path, "sc1", server_text=_GENERIC_FATAL_LOG),
        ]
    )
    assert [n for n, _, _ in with_crash.results] == [
        "SomeComponent: transient generic fatal"
    ]
    assert with_crash.crash_named is True


def test_nothing_parsed_returns_empty(tmp_path):
    assert select_replica_failures([]).results == []


# ---------------------------------------------------------------------------
# The final OOM downgrade: `server_log_reports_oom` / `oom_explains_failure`.
#
# `run_stress_test` rewrites a failing run to OK when it ran out of memory. The
# only in-log evidence of a kernel OOM kill is the watchdog's `signal 9` line -
# which the harness's own `clickhouse stop --force` produces too, announced by a
# `Warning: server did not stop yet` row - so a kill the harness sent, a kill in a
# rotated log, or a failing row an OOM cannot produce must all keep the run red.
# ---------------------------------------------------------------------------

_KILL_LINE = (
    "2026.09.04 00:44:57.972626 [ 1 ] {} <Fatal> Application: "
    "Child process was terminated by signal 9 (KILL). If it is not done by 'forcestop' "
    "command or manually, the possible cause is OOM Killer "
    "(see 'dmesg' and look at the '/var/log/kern.log' for the details).\n"
)
_QUIET_LOG = (
    "2026.09.04 00:44:58.000000 [ 1 ] {} <Information> Application: shutting down\n"
)


def _row(name, ok=True):
    return Result.create_from(
        name=name, status=Result.Status.OK if ok else Result.Status.FAIL
    )


def test_kill_line_without_harness_kill_is_oom(tmp_path):
    (tmp_path / "clickhouse-server.log").write_text(_QUIET_LOG + _KILL_LINE)
    assert server_log_reports_oom(tmp_path, [_row("Check failed", ok=False)])


def test_kill_line_announced_by_harness_is_not_oom(tmp_path):
    # The harness could not stop the server with SIGTERM, said so, and SIGKILLed it
    # itself: the resulting kill line must not pass whatever failed as an OOM.
    (tmp_path / "clickhouse-server.log").write_text(_QUIET_LOG + _KILL_LINE)
    results = [_row("Warning: server did not stop yet"), _row("Check failed", ok=False)]
    assert not server_log_reports_oom(tmp_path, results)


def test_kill_line_beyond_the_harness_kills_is_oom(tmp_path):
    # One announced kill, two kill lines: the second one is the kernel's.
    (tmp_path / "clickhouse-server.log").write_text(
        _KILL_LINE + _QUIET_LOG + _KILL_LINE
    )
    assert server_log_reports_oom(tmp_path, [_row("Warning: server did not stop yet")])


def test_kill_line_in_rotated_log_only_is_not_oom(tmp_path):
    # A kill in a rotated log belongs to an incarnation that was already replaced;
    # only the current `clickhouse-server*.log` files count.
    (tmp_path / "clickhouse-server.log.1").write_text(_KILL_LINE)
    (tmp_path / "clickhouse-server.log").write_text(_QUIET_LOG)
    (tmp_path / "clickhouse-server.err.log").write_text(_QUIET_LOG)
    assert not server_log_reports_oom(tmp_path, [])


def test_kill_line_in_current_err_log_counts(tmp_path):
    (tmp_path / "clickhouse-server.err.log").write_text(_KILL_LINE)
    assert server_log_reports_oom(tmp_path, [])


def test_kill_line_in_archived_phase_log_only_is_not_oom(tmp_path):
    # `stress_runner.sh` / `upgrade_runner.sh` archive each phase's log by renaming it, so
    # by the time this runs `clickhouse-server.log` is gone and only the phase logs remain.
    # A kill in one of them belongs to a server that was already replaced and restarted.
    for phase in ("initial", "stress", "final", "upgrade"):
        (tmp_path / f"clickhouse-server.{phase}.log").write_text(_KILL_LINE)
    assert not server_log_reports_oom(tmp_path, [])


def test_kill_line_duplicated_into_err_log_counts_once(tmp_path):
    # The watchdog logs the kill at `Fatal`, so the same event lands in both channels.
    # Summing them would turn one harness-sent kill into two and pass the run as an OOM.
    (tmp_path / "clickhouse-server.log").write_text(_QUIET_LOG + _KILL_LINE)
    (tmp_path / "clickhouse-server.err.log").write_text(_KILL_LINE)
    assert not server_log_reports_oom(
        tmp_path, [_row("Warning: server did not stop yet")]
    )


def test_kill_lines_on_separate_replicas_are_summed(tmp_path):
    # Folding the two channels of one process together must not fold two processes
    # together: each replica can be killed in its own right.
    (tmp_path / "clickhouse-server.log").write_text(_KILL_LINE)
    (tmp_path / "clickhouse-server-sc1.log").write_text(_KILL_LINE)
    assert server_log_reports_oom(tmp_path, [_row("Warning: server did not stop yet")])


def test_no_server_logs_is_not_oom(tmp_path):
    assert not server_log_reports_oom(tmp_path / "missing", [])
    assert not server_log_reports_oom(tmp_path, [])


def test_oom_explains_plain_check_failed_only():
    assert oom_explains_failure(True, False, [_row("Check failed", ok=False)])
    assert not oom_explains_failure(False, False, [_row("Check failed", ok=False)])


def test_oom_does_not_explain_a_named_crash():
    assert not oom_explains_failure(True, True, [_row("Check failed", ok=False)])


def test_oom_does_not_explain_a_non_oom_finding_row():
    # The suite's own verdicts an OOM kill cannot produce keep the run red even
    # though the log parser named no crash.
    for name in (
        "Hung check failed",
        "Possible deadlock on shutdown (see gdb.log)",
        "Logical error thrown (see clickhouse-server.log or logical_errors.txt)",
        "Sanitizer assert (in stderr.log)",
        "Lost forever for part",
        "No such key errors",
    ):
        assert not oom_explains_failure(True, False, [_row(name, ok=False)]), name
