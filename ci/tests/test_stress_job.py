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
from ci.jobs.stress_job import find_log, has_sanitizer_report, select_replica_failures

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


def test_recovered_cfi_report_in_stderr_is_detected(tmp_path):
    # `ENABLE_CFI_DIAGNOSTICS` reports and continues: no `<Fatal>`, the server stays alive.
    assert not has_sanitizer_report(tmp_path)
    (tmp_path / "stderr.log").write_text("Logging trace to /var/log/clickhouse-server/clickhouse-server.log\n")
    assert not has_sanitizer_report(tmp_path)
    (tmp_path / "stderr-sc1.log").write_text(
        "Parser.rl:1752:55: runtime error: control flow integrity check for type 'std::ostream' "
        "failed during non-virtual call (vtable address 0x7f8482817808)\n"
    )
    assert has_sanitizer_report(tmp_path)


def test_recovered_report_left_in_server_log_dir_is_detected(tmp_path):
    # The runner exits before moving `stderr.log` to the results, so it stays with the server logs.
    result_path, server_log_path = tmp_path / "result_path", tmp_path / "server_log"
    result_path.mkdir()
    server_log_path.mkdir()
    assert not has_sanitizer_report(result_path, server_log_path)
    (server_log_path / "stderr.log").write_text(
        "src/Foo.cpp:1:1: runtime error: control flow integrity check for type 'void ()' failed\n"
    )
    assert has_sanitizer_report(result_path, server_log_path)
    assert find_log("stderr.log", result_path, server_log_path) == server_log_path / "stderr.log"
