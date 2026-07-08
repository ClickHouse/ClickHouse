"""
Regression test for the memory-limit classification in ci/jobs/ast_fuzzer_job.py.

A fuzzed query can push the SERVER over its memory cap; the memory tracker
rejects the allocation with Code 241 (MEMORY_LIMIT_EXCEEDED) and the server
stays alive (server_died=0, clean shutdown). The server transmits that
exception to the client, which prints it prefixed with "Received from <host>."
and exits with the server error code (241). Before the fix, that landed in the
catch-all `else` branch and turned the whole job into a bogus ERROR ("Client
failure"), even though run-fuzzer.sh's liveness loop already treats a 241 as
"server alive, busy".

The benign path is keyed off SERVER-ORIGIN evidence (SERVER_MLE_SIGNATURE),
not any MEMORY_LIMIT_EXCEEDED text: clickhouse-client itself can raise 241
under --max_memory_usage_in_client (see
tests/queries/0_stateless/02003_memory_limit_in_client.sh) and
mainEntryClickHouseClient returns that code verbatim. Such a client-side 241
has no "Received from" prefix and must NOT be swallowed, or a real
client/harness regression would be masked.
"""

import os
import re

_JOB = os.path.join(
    os.path.dirname(__file__), "..", "jobs", "ast_fuzzer_job.py"
)


def _load(name):
    # Load a top-level def/assignment by name from ast_fuzzer_job.py without
    # importing its heavy CI dependencies: exec only the target snippet.
    src = open(_JOB, encoding="utf-8").read()
    ns = {}
    if name == "SERVER_MLE_SIGNATURE":
        marker = "SERVER_MLE_SIGNATURE = "
        start = src.index(marker)
        end = src.index("\n", start)
        exec(src[start:end], ns)  # noqa: S102 - trusted first-party source
    else:
        marker = f"def {name}("
        start = src.index(marker)
        end = src.index("\ndef ", start + 1)
        exec(src[start:end], ns)  # noqa: S102 - trusted first-party source
    return ns[name]


def _is_benign_memory_limit(*args):
    return _load("_is_benign_memory_limit")(*args)


def _server_mle_matches(log_text):
    pattern = _load("SERVER_MLE_SIGNATURE")
    return any(re.search(pattern, line) for line in log_text.splitlines())


# Real client output shapes (single line, as clickhouse-client prints them).
_SERVER_MLE_LINE = (
    "Received from localhost:9000. DB::Exception: Memory limit (total) "
    "exceeded: would use 9.32 GiB, maximum: 9.31 GiB. (MEMORY_LIMIT_EXCEEDED)"
)
_CLIENT_MLE_LINE = (
    "Code: 241. DB::Exception: Client memory limit exceeded: would use "
    "10.05 MiB, maximum: 10.00 MiB. (MEMORY_LIMIT_EXCEEDED)"
)


# --- SERVER_MLE_SIGNATURE: server-origin evidence only ---


def test_signature_matches_server_transmitted_241():
    # "Received from <host>. ... (MEMORY_LIMIT_EXCEEDED)" -> server-origin.
    assert _server_mle_matches(_SERVER_MLE_LINE) is True


def test_signature_ignores_client_side_241():
    # Client-side 241 (--max_memory_usage_in_client) has no "Received from"
    # prefix -> must NOT count as benign evidence.
    assert _server_mle_matches(_CLIENT_MLE_LINE) is False


# --- _is_benign_memory_limit: gate on (server_died, exit, server-origin MLE) ---


def test_server_survived_memory_limit_is_benign():
    # 241 + server alive + server-origin MLE -> benign, job OK.
    assert _is_benign_memory_limit(False, 241, True) is True


def test_client_side_241_is_not_benign():
    # 241 + server alive but NO server-origin MLE (e.g. a client-side cap or a
    # harness 241) -> not benign; a real client/harness regression must surface.
    assert _is_benign_memory_limit(False, 241, False) is False


def test_dead_server_is_not_benign():
    # Server died -> a crash, never benign, even with server-origin MLE.
    assert _is_benign_memory_limit(True, 241, True) is False


def test_other_client_failure_is_not_benign():
    # A non-241 client failure (e.g. a real finding) stays an error.
    assert _is_benign_memory_limit(False, 139, True) is False
    assert _is_benign_memory_limit(False, 0, True) is False
