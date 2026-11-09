"""
Tests for `select_replica_failures` (ci/jobs/stress_job.py).

The stress job pairs each replica's server log with its stderr log and must not
let a failure on one replica hide a (possibly higher-signal) failure on another:
every pair is scanned, all distinct specific classifications are reported, and a
generic `<Fatal>` / "Unknown error" is used only when nothing specific was found.
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from ci.jobs.scripts.log_parser import FuzzerLogParser
from ci.jobs.stress_job import select_replica_failures

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
    # Build one (replica_name, server_log, stderr_log) pair. Missing logs are
    # non-existent Paths, matching what the job passes when a file is absent.
    server_log = tmp_path / f"clickhouse-server-{replica}.err.log"
    stderr_log = tmp_path / f"stderr-{replica}.log"
    if server_text is not None:
        server_log.write_text(server_text, encoding="utf-8")
    if stderr_text is not None:
        stderr_log.write_text(stderr_text, encoding="utf-8")
    return (replica, server_log, stderr_log)


def test_specific_failure_on_second_replica_not_hidden_by_first(tmp_path):
    # Main replica has an oracle mismatch, sc1 has an ASan report. Both are
    # specific classifications and must both be reported - crucially the ASan
    # report on the second replica is not suppressed by the first.
    pairs = [
        _pair(tmp_path, "main", server_text=_ORACLE_MISMATCH_LOG),
        _pair(tmp_path, "sc1", server_text=_UNKNOWN_LOG, stderr_text=_ASAN_STDERR),
    ]

    results = select_replica_failures(pairs)
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

    results = select_replica_failures(pairs)
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

    results = select_replica_failures(pairs)

    assert len(results) == 1
    assert results[0][0].startswith("AST Fuzzer oracle mismatch")


def _asan_stderr(column_type):
    return (
        "==1234==ERROR: AddressSanitizer: heap-use-after-free on address 0x1\n"
        f"    #0 0x55b9b8db9bc7 in DB::ColumnVector<{column_type}>::insertRangeFrom("
        "DB::IColumn const&) src/Columns/ColumnVector.cpp:10:5\n"
        "    #1 0x55b9b8db9bc8 in DB::Foo::bar() src/Foo.cpp:10:5\n"
        "SUMMARY: AddressSanitizer: heap-use-after-free src/Columns/ColumnVector.cpp:10:5\n"
    )


def test_same_name_different_qualified_stacks_both_reported(tmp_path):
    # The STID drops template arguments, so these share a name. The AI triage may pass
    # a run on a known stack, so the other one must reach it too; sc2 repeats sc1.
    pairs = [
        _pair(tmp_path, "main", _UNKNOWN_LOG, _asan_stderr("unsigned long")),
        _pair(tmp_path, "sc1", _UNKNOWN_LOG, _asan_stderr("unsigned char")),
        _pair(tmp_path, "sc2", _UNKNOWN_LOG, _asan_stderr("unsigned char")),
    ]

    results = select_replica_failures(pairs)

    assert len({name for name, _, _ in results}) == 1
    assert [FuzzerLogParser.qualified_stack(info)[0] for _, info, _ in results] == [
        "DB::ColumnVector<unsigned long>::insertRangeFrom",
        "DB::ColumnVector<unsigned char>::insertRangeFrom",
    ]


def test_generic_fatal_used_when_no_specific_failure(tmp_path):
    # No replica has a specific classification: the generic <Fatal> is reported
    # (once) rather than "Unknown error".
    pairs = [
        _pair(tmp_path, "main", server_text=_GENERIC_FATAL_LOG),
        _pair(tmp_path, "sc1", server_text=_UNKNOWN_LOG),
    ]

    results = select_replica_failures(pairs)

    assert len(results) == 1
    assert results[0][0] == "SomeComponent: transient generic fatal"


def test_unknown_error_when_nothing_classified(tmp_path):
    # Neither specific nor generic fatal anywhere: a single "Unknown error".
    pairs = [
        _pair(tmp_path, "main", server_text=_UNKNOWN_LOG),
        _pair(tmp_path, "sc1", server_text=_UNKNOWN_LOG),
    ]

    results = select_replica_failures(pairs)

    assert len(results) == 1
    assert results[0][0] == FuzzerLogParser.UNKNOWN_ERROR



_CRASH_BANNER = (
    "2026.09.04 00:44:57.972626 [ 1 ] {} <Fatal> BaseDaemon: "
    "########################################\n"
)


def _logical_error_log(func):
    # A logical exception aborts through the signal handler, which logs the crash header.
    return (
        "2026.09.04 00:44:57.000000 [ 1068 ] {q} <Fatal> : Logical error: "
        f"'Bad cast in {func}'.\n"
        "2026.09.04 00:44:57.000001 [ 1068 ] {q} <Fatal> : Stack trace (when copying this "
        "message, always include the lines below):\n"
        f"0. src/{func}.cpp:10: DB::{func}() @ 0x0000000000000001\n"
        f"1. src/{func}.cpp:20: DB::{func}Caller() @ 0x0000000000000002\n\n" + _CRASH_BANNER + (
        "2026.09.04 00:44:57.972627 [ 1 ] {} <Fatal> BaseDaemon: (version 26.11.1.1) (from thread 1068) "
        "(query_id: q) (query: SELECT 1) Received signal Aborted (6)\n"
    )
    )


def _inventory(results_omitted):
    return [r for r in results_omitted if r[0] == "Incomplete failure inventory"]


def test_second_report_in_one_stderr_blocks_known_issue_pass(tmp_path):
    # Two independent ASan reports in one stderr (a restart appends to it): the parser names
    # the first only, so an issue of that one must not pass the run over the second. A
    # sanitizer abort resets the signal handlers first, so it logs no crash header.
    pairs = [
        _pair(
            tmp_path, "main", _UNKNOWN_LOG, _asan_stderr("unsigned long") + _ASAN_STDERR
        )
    ]
    omitted = []
    results = select_replica_failures(pairs, omitted)

    assert len(results) == 1
    assert "main logged 2 reports, 1 named" in _inventory(omitted)[0][1]


def test_logical_error_next_to_sanitizer_report_blocks_known_issue_pass(tmp_path):
    # The ASan report wins the classification, and the crash header belongs to an independent
    # logical exception, which no issue of the ASan report may pass the run over.
    pairs = [_pair(tmp_path, "main", _logical_error_log("ServerNew"), _ASAN_STDERR)]
    omitted = []
    results = select_replica_failures(pairs, omitted)

    assert [n for n, _, _ in results][0].startswith("AddressSanitizer")
    assert "main logged 2 reports, 1 named" in _inventory(omitted)[0][1]


def test_crash_in_rotated_log_unnamed_blocks_known_issue_pass(tmp_path):
    # The server before the restart crashed, the current one has an ASan report: each server
    # log is paired with the same stderr, whose report wins every time, so the crash is not named.
    stderr = tmp_path / "stderr.log"
    stderr.write_text(_ASAN_STDERR, encoding="utf-8")
    current = tmp_path / "clickhouse-server.err.log"
    rotated = tmp_path / "clickhouse-server.err.log.0"
    current.write_text(_UNKNOWN_LOG, encoding="utf-8")
    rotated.write_text(_logical_error_log("Rotated"), encoding="utf-8")
    omitted = []
    select_replica_failures(
        [("main", current, stderr), ("main", rotated, stderr)], omitted
    )

    assert "main logged 2 reports, 1 named" in _inventory(omitted)[0][1]


def test_one_report_per_replica_is_complete(tmp_path):
    # The same report on two replicas is reported once, but each replica named its own.
    pairs = [
        _pair(tmp_path, "main", _UNKNOWN_LOG, _ASAN_STDERR),
        _pair(tmp_path, "sc1", _logical_error_log("Replica"), None),
    ]
    omitted = []
    results = select_replica_failures(pairs, omitted)

    assert len(results) == 2
    assert _inventory(omitted) == []


def test_oracle_mismatch_next_to_sanitizer_report_blocks_known_issue_pass(tmp_path):
    # An oracle mismatch throws without aborting: a `<Fatal>` record without a crash report.
    pairs = [_pair(tmp_path, "main", _ORACLE_MISMATCH_LOG, _ASAN_STDERR)]
    omitted = []
    results = select_replica_failures(pairs, omitted)

    assert [n for n, _, _ in results][0].startswith("AddressSanitizer")
    assert "main logged 2 reports, 1 named" in _inventory(omitted)[0][1]


def test_oracle_mismatch_alone_adds_no_inventory_failure(tmp_path):
    # A failure without a stack keeps the run red anyway, so it counts as named.
    omitted = []
    select_replica_failures([_pair(tmp_path, "main", _ORACLE_MISMATCH_LOG)], omitted)

    assert _inventory(omitted) == []


def _fatal_failures(tmp_path, text):
    log = tmp_path / "server.err.log"
    log.write_text(text, encoding="utf-8")
    return FuzzerLogParser.count_server_log_failures(log)


def test_count_server_log_failures(tmp_path):
    watchdog = (
        "2026.09.04 00:45:00.000000 [ 7 ] {} <Fatal> Application: Child process was "
        "terminated by signal 6.\n"
    )
    # The message of the logical exception and its crash report are one failure.
    assert _fatal_failures(tmp_path, _logical_error_log("A") + watchdog) == 1
    # An earlier query of the thread that aborted logged an oracle mismatch: two failures.
    other_query = _ORACLE_MISMATCH_LOG.replace("{q}", "{q2}")
    assert _fatal_failures(tmp_path, other_query + _logical_error_log("A")) == 2
    # Two oracle mismatches of different queries of one thread.
    assert _fatal_failures(tmp_path, _ORACLE_MISMATCH_LOG + other_query) == 2
    assert _fatal_failures(tmp_path, watchdog) == 0


def _survived_logical_error(sizes, thread=2001, query="q7"):
    # A logical exception that does not abort: the server logs it at `<Error>` and goes on.
    return (
        f"2026.09.04 00:44:50.000000 [ {thread} ] {{{query}}} <Error> executeQuery: Code: 190. "
        "DB::Exception: Logical error: Sizes of nested columns are inconsistent: "
        f"{sizes}. (SIZES_OF_NESTED_COLUMNS_ARE_INCONSISTENT) (version 26.11.1.1) "
        "(in query: SELECT 1), Stack trace (when copying this message, always include the lines below):\n"
    )


def test_count_survived_logical_errors(tmp_path):
    # A retry logs the same exception again, with other values: one failure.
    retried = _survived_logical_error("3 and 4") + _survived_logical_error("5 and 6", 2002, "")
    assert _fatal_failures(tmp_path, retried) == 1
    other = retried.replace("Sizes of nested columns", "Columns sizes")
    assert _fatal_failures(tmp_path, retried + other) == 2
    # Only the record's own message counts, not the query it quotes.
    quoted = (
        "2026.09.04 00:44:50.000000 [ 2001 ] {q7} <Error> executeQuery: Code: 62. "
        "DB::Exception: Syntax error. (SYNTAX_ERROR) (in query: SELECT 'Logical error: x')\n"
    )
    assert _fatal_failures(tmp_path, quoted) == 0
    # The exception of the query that then crashed belongs to its crash report.
    same_query = _survived_logical_error("3 and 4", 1068, "q")
    assert _fatal_failures(tmp_path, same_query + _logical_error_log("A")) == 1
    assert _fatal_failures(tmp_path, _survived_logical_error("3 and 4") + _logical_error_log("A")) == 2


def test_survived_logical_error_next_to_sanitizer_report_blocks_known_issue_pass(tmp_path):
    # The ASan report wins the classification, and an independent logical exception the server
    # survived must keep the run red: no issue of the ASan report covers it.
    pairs = [_pair(tmp_path, "main", _UNKNOWN_LOG + _survived_logical_error("3 and 4"), _ASAN_STDERR)]
    omitted = []
    results = select_replica_failures(pairs, omitted)

    assert [n for n, _, _ in results][0].startswith("AddressSanitizer")
    assert "main logged 2 reports, 1 named" in _inventory(omitted)[0][1]


_DEADLY_SIGNAL = "AddressSanitizer:DEADLYSIGNAL\n"
_ASAN_SEGV_STDERR = (
    "AddressSanitizer:DEADLYSIGNAL\n"
    "=================================================================\n"
    "==1234==ERROR: AddressSanitizer: SEGV on unknown address 0x000000000000\n"
    "    #0 0x55b9b8db9bc7 in DB::Foo::bar() src/Foo.cpp:10:5\n"
    "SUMMARY: AddressSanitizer: SEGV src/Foo.cpp:10:5\n"
)


def test_interrupted_report_next_to_sanitizer_report_blocks_known_issue_pass(tmp_path):
    # A restart appended a report cut short after its `DEADLYSIGNAL` line to the same stderr.
    pairs = [_pair(tmp_path, "main", _UNKNOWN_LOG, _ASAN_STDERR + _DEADLY_SIGNAL)]
    omitted = []
    results = select_replica_failures(pairs, omitted)

    assert [n for n, _, _ in results][0].startswith("AddressSanitizer")
    assert "main logged 2 reports, 1 named" in _inventory(omitted)[0][1]


def test_complete_deadly_signal_report_is_counted_once(tmp_path):
    omitted = []
    select_replica_failures(
        [_pair(tmp_path, "main", _UNKNOWN_LOG, _ASAN_SEGV_STDERR)], omitted
    )

    assert _inventory(omitted) == []


def test_restart_failure_next_to_named_crash_blocks_known_issue_pass():
    # The report drops the restart row as a consequence of the named crash, but a later
    # readiness timeout can be independent of it, so no issue of the crash may pass it.
    from ci.jobs.scripts.ai_fuzz_triage import (
        Occurrences,
        group_failures,
        known_issue_verdict,
    )
    from ci.jobs.stress_job import drop_crash_consequences
    from ci.praktika.result import Result

    crash = "Logical error: 'Bad cast' (STID: 1234-abcd)"
    timeout = (
        "Cannot start clickhouse-server",
        "a clickhouse-server process is running but did not become ready in 600s",
    )
    failed = [
        Result.create_from(name=name, info=info, status=Result.Status.FAIL)
        for name, info in [timeout, ("Test script failed", "exit code: 1"), (crash, "")]
    ]
    restart_failures = []

    kept = drop_crash_consequences(failed, restart_failures)

    assert [r.name for r in kept] == [crash]
    assert restart_failures == [timeout]
    occurrences = Occurrences(["issue"], [], exact=["issue"], branch="master")
    groups = group_failures([(crash, "")])
    assert known_issue_verdict(groups, occurrences, True)
    groups = group_failures([(crash, "")] + restart_failures)
    assert groups.uncovered == [timeout[0]]
    assert known_issue_verdict(groups, occurrences, True) is None
