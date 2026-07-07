"""
Regression test for the memory-limit classification in ci/jobs/ast_fuzzer_job.py.

A fuzzed query can push the server over its memory cap; the memory tracker
rejects the allocation with Code 241 (MEMORY_LIMIT_EXCEEDED) and the server
stays alive (server_died=0, clean shutdown). clickhouse-client returns the
server error code as its exit status, so the fuzzer exits 241. Before the fix,
that landed in the catch-all `else` branch and turned the whole job into a bogus
ERROR ("Client failure"), even though run-fuzzer.sh's liveness loop already
treats a 241 as "server alive, busy". These assert the classifier now treats a
server-survived 241 as OK while a genuine crash / non-241 client failure is not.
"""

import os

_JOB = os.path.join(
    os.path.dirname(__file__), "..", "jobs", "ast_fuzzer_job.py"
)


def _is_benign_memory_limit(*args):
    # Load the module by path without importing its heavy CI dependencies:
    # read the source and exec only the target function's def.
    src = open(_JOB, encoding="utf-8").read()
    ns = {}
    # The helper is self-contained (no imports needed); extract and exec it.
    marker = "def _is_benign_memory_limit("
    start = src.index(marker)
    # find the end of the function: next top-level 'def ' after start
    end = src.index("\ndef ", start + 1)
    exec(src[start:end], ns)  # noqa: S102 - trusted first-party source
    return ns["_is_benign_memory_limit"](*args)


def test_server_survived_memory_limit_is_benign():
    # 241 + server alive + MLE in log -> benign, job OK.
    assert _is_benign_memory_limit(False, 241, True) is True


def test_memory_limit_without_log_evidence_is_not_benign():
    # 241 but no MEMORY_LIMIT_EXCEEDED in the log -> treat as a real client
    # failure (don't guess the cause was memory).
    assert _is_benign_memory_limit(False, 241, False) is False


def test_dead_server_is_not_benign():
    # Server died -> a crash, never benign, even if the log mentions MLE.
    assert _is_benign_memory_limit(True, 241, True) is False


def test_other_client_failure_is_not_benign():
    # A non-241 client failure (e.g. a real finding) stays an error.
    assert _is_benign_memory_limit(False, 139, True) is False
    assert _is_benign_memory_limit(False, 0, True) is False
