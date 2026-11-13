"""
Tests for the report inventory of `triage_failure` (ci/jobs/ast_fuzzer_job.py).

The server and the client of the AST fuzzer write their sanitizer reports to one
`stderr.log`, and the log parser names the first report only. The AI triage may
pass a run on a known issue of the named report, so a second, unnamed report must
reach it as a failure no issue covers.
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

import types

import pytest

import ci.jobs.ast_fuzzer_job as ast_job
import ci.jobs.scripts.ai_fuzz_triage as triage
import ci.praktika.result as praktika_result
from ci.jobs.scripts.ai_fuzz_triage import (
    Occurrences,
    group_failures,
    known_issue_verdict,
)

_BANNER = (
    "2026.11.09 10:00:00.000000 [ 1 ] {} <Fatal> BaseDaemon: "
    "########################################\n"
)


def _asan(func, pid):
    return (
        f"=== sanitizer report from /workspace/sanitizer.log.{pid} ===\n"
        f"=={pid}==ERROR: AddressSanitizer: heap-use-after-free on address 0x1\n"
        f"    #0 0x1 in DB::{func}() src/{func}.cpp:10:5\n"
        f"    #1 0x2 in DB::{func}Caller() src/{func}.cpp:20:5\n"
        f"SUMMARY: AddressSanitizer: heap-use-after-free src/{func}.cpp:10:5\n\n"
    )


def _logical_error(func):
    # A logical exception aborts through the signal handler, which logs the crash header.
    return (
        "2026.11.09 10:00:00.000000 [ 1068 ] {q} <Fatal> : Logical error: "
        f"'Bad cast in {func}'.\n"
        "2026.11.09 10:00:00.000001 [ 1068 ] {q} <Fatal> : Stack trace (when copying this "
        "message, always include the lines below):\n"
        f"0. src/{func}.cpp:10: DB::{func}() @ 0x0000000000000001\n"
        f"1. src/{func}.cpp:20: DB::{func}Caller() @ 0x0000000000000002\n\n" + _BANNER + (
        "2026.11.09 10:00:00.000001 [ 1 ] {} <Fatal> BaseDaemon: (version 26.11.1.1) (from thread 1068) "
        "(query_id: q) (query: SELECT 1) Received signal Aborted (6)\n"
    )
    )


def _triaged_failures(
    tmp_path,
    monkeypatch,
    stderr_text,
    server_text="",
    fuzzer_text="Fuzzing step 1 out of 1\nSELECT 1\n",
    fuzzer_exit_code=0,
):
    # The real analysis of a dead server, then the failures `triage_failure` hands over.
    # `run-fuzzer.sh` appends the sanitizer reports to `server.log` too.
    artifacts = tuple(
        tmp_path / n
        for n in ("server.log", "fuzzer.log", "stderr.log", "dmesg.log", "fatal.log")
    )
    server_log, fuzzer_log, stderr_log, _, _ = artifacts
    server_log.write_text(server_text + stderr_text, encoding="utf-8")
    stderr_log.write_text(stderr_text, encoding="utf-8")
    fuzzer_log.write_text(fuzzer_text, encoding="utf-8")
    monkeypatch.setattr(ast_job, "JOB_ARTIFACTS", artifacts)
    # `Result.create_from` names the result after the job, which only a CI run has.
    monkeypatch.setattr(
        praktika_result._Environment,
        "get",
        staticmethod(lambda: types.SimpleNamespace(JOB_NAME="AST fuzzer (amd_debug)")),
    )
    result = ast_job.analyze_job_logs(
        list(artifacts), True, 134, fuzzer_exit_code, True, fuzzer_log
    )
    assert result.is_failure()

    handed = []
    monkeypatch.setattr(
        triage, "triage_and_apply", lambda r, **kw: handed.extend(kw["failures"])
    )
    ast_job.triage_failure(
        result, "AST Fuzzer", list(artifacts), "", report_logs=([server_log], stderr_log)
    )
    return handed


def _known_issue_passes(failures):
    # The fingerprint's issue is open and saw this stack before the PR.
    groups = group_failures(failures)
    occurrences = Occurrences([], [], ["https://example/issue"], [], {}, "master", False)
    return bool(known_issue_verdict(groups, occurrences, True))


def test_second_report_blocks_known_issue_pass(tmp_path, monkeypatch):
    failures = _triaged_failures(
        tmp_path, monkeypatch, _asan("ClientKnown", 11) + _asan("ServerNew", 22)
    )

    assert [n for n, _ in failures][-1] == "Incomplete failure inventory"
    assert "logged 2 reports, 1 named" in failures[-1][1]
    assert not _known_issue_passes(failures)


def test_single_report_still_passes_on_known_issue(tmp_path, monkeypatch):
    failures = _triaged_failures(tmp_path, monkeypatch, _asan("ClientKnown", 11))

    assert "Incomplete failure inventory" not in [n for n, _ in failures]
    assert _known_issue_passes(failures)


# What the client's own signal handler writes to its stderr, so to `fuzzer.log`.
_CLIENT_ASSERTION = (
    "Fuzzing step 7 out of 9\nSELECT 1\n"
    "########## Short fault info ############\n"
    "(version 26.11.1.1, build id: X, git hash: Y, architecture: x86_64) (from thread 1) "
    "Received signal Aborted (6)\n"
    "Stack trace: 0x1 0x2\n"
    "0. src/Client/ClientBase.cpp:10: DB::ClientBase::onData() @ 0x1\n"
)


@pytest.mark.parametrize("fuzzer_exit_code", [134, 139])
def test_client_failure_next_to_known_server_report_blocks_pass(
    tmp_path, monkeypatch, fuzzer_exit_code
):
    # The client's report is in `fuzzer.log` only, which no server-side count reads.
    failures = _triaged_failures(
        tmp_path,
        monkeypatch,
        _asan("ServerKnown", 11),
        fuzzer_text=_CLIENT_ASSERTION,
        fuzzer_exit_code=fuzzer_exit_code,
    )

    assert failures[0][0].startswith("AddressSanitizer")
    client = [info for name, info in failures if name == "Client failure"]
    assert len(client) == 1 and "DB::ClientBase::onData" in client[0]
    assert "Incomplete failure inventory" not in [n for n, _ in failures]
    assert not _known_issue_passes(failures)


@pytest.mark.parametrize("fuzzer_exit_code", [32, 210])
def test_client_losing_the_server_still_passes_on_known_issue(
    tmp_path, monkeypatch, fuzzer_exit_code
):
    failures = _triaged_failures(
        tmp_path, monkeypatch, _asan("ServerKnown", 11), fuzzer_exit_code=fuzzer_exit_code
    )

    assert "Client failure" not in [n for n, _ in failures]
    assert _known_issue_passes(failures)


def test_logical_error_next_to_known_sanitizer_report_blocks_pass(tmp_path, monkeypatch):
    # A sanitizer abort logs no crash header, so the header is an independent server failure
    # that the parser does not name next to the ASan report.
    failures = _triaged_failures(
        tmp_path, monkeypatch, _asan("ClientKnown", 11), _logical_error("ServerNew")
    )

    assert failures[0][0].startswith("AddressSanitizer")
    assert "logged 2 reports, 1 named" in failures[-1][1]
    assert not _known_issue_passes(failures)


_ORACLE = (
    "2026.11.09 10:00:00.000000 [ 2000 ] {q2} <Fatal> executeQuery: AST Fuzzer oracle "
    "mismatch detected!\n"
    "Fuzzed query: SELECT g FROM t GROUP BY g\n"
    "TLP Aggregate oracle mismatch!\n"
)


def test_oracle_mismatch_next_to_known_sanitizer_report_blocks_pass(tmp_path, monkeypatch):
    # An oracle mismatch throws without aborting, so it logs a `<Fatal>` record but no crash
    # report, and the parser names the ASan report.
    failures = _triaged_failures(
        tmp_path, monkeypatch, _asan("ClientKnown", 11), _ORACLE
    )

    assert failures[0][0].startswith("AddressSanitizer")
    assert "logged 2 reports, 1 named" in failures[-1][1]
    assert not _known_issue_passes(failures)



_SURVIVED_LOGICAL_ERROR = (
    "2026.11.09 10:00:00.000000 [ 2001 ] {q7} <Error> executeQuery: Code: 190. DB::Exception: "
    "Logical error: Sizes of nested columns are inconsistent: 3 and 4. "
    "(SIZES_OF_NESTED_COLUMNS_ARE_INCONSISTENT) (version 26.11.1.1) (in query: SELECT 1)\n"
)


def test_survived_logical_error_next_to_known_sanitizer_report_blocks_pass(tmp_path, monkeypatch):
    # A logical exception that does not abort is logged at `<Error>`, and the parser names the
    # ASan report, so no issue of the ASan report may pass the run over the exception.
    failures = _triaged_failures(
        tmp_path, monkeypatch, _asan("ClientKnown", 11), _SURVIVED_LOGICAL_ERROR
    )

    assert failures[0][0].startswith("AddressSanitizer")
    assert "logged 2 reports, 1 named" in failures[-1][1]
    assert not _known_issue_passes(failures)

_DEADLY_SIGNAL = "AddressSanitizer:DEADLYSIGNAL\n"


def _asan_segv(func, pid):
    # A report of a deadly signal: the sanitizer writes its first line straight to stderr,
    # so with `log_path` it is in `stderr.log` long before the rest is merged in.
    return (
        f"=== sanitizer report from /workspace/sanitizer.log.{pid} ===\n"
        "=================================================================\n"
        f"=={pid}==ERROR: AddressSanitizer: SEGV on unknown address 0x000000000000\n"
        f"    #0 0x1 in DB::{func}() src/{func}.cpp:10:5\n"
        f"    #1 0x2 in DB::{func}Caller() src/{func}.cpp:20:5\n"
        f"SUMMARY: AddressSanitizer: SEGV src/{func}.cpp:10:5\n\n"
    )


def test_interrupted_report_next_to_known_sanitizer_report_blocks_pass(tmp_path, monkeypatch):
    # A second report cut short after its `DEADLYSIGNAL` line is not a report the parser names.
    failures = _triaged_failures(
        tmp_path, monkeypatch, _DEADLY_SIGNAL + _asan("ClientKnown", 11)
    )

    assert failures[0][0].startswith("AddressSanitizer")
    assert "logged 2 reports, 1 named" in failures[-1][1]
    assert not _known_issue_passes(failures)


def test_complete_deadly_signal_report_is_counted_once(tmp_path, monkeypatch):
    failures = _triaged_failures(
        tmp_path, monkeypatch, _DEADLY_SIGNAL + _asan_segv("ServerKnown", 22)
    )

    assert "Incomplete failure inventory" not in [n for n, _ in failures]
    assert _known_issue_passes(failures)
