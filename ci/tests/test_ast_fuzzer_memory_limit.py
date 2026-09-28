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

The benign path is keyed off SERVER-ORIGIN evidence (SERVER_MLE_SIGNATURE) in
the TERMINAL query block (the text after the last "Fuzzing step <n> out of <m>"
marker the client prints on stderr before each fuzz step), not any
MEMORY_LIMIT_EXCEEDED text anywhere in fuzzer.log:
- clickhouse-client itself can raise 241 under --max_memory_usage_in_client
  (see tests/queries/0_stateless/02003_memory_limit_in_client.sh) and
  mainEntryClickHouseClient returns that code verbatim. Such a client-side 241
  has no "Received from" prefix and must NOT be swallowed.
- The AST fuzzer swallows query-side server MEMORY_LIMIT_EXCEEDED and keeps
  running (Client::processASTFuzzerStep, programs/client/FuzzLoop.cpp), so a
  30-minute run accumulates many recovered "Received from ... memory limit
  exceeded" lines that did NOT terminate it, each in its own (non-terminal)
  step block. A fixed line/byte tail can still hold such a swallowed limit
  together with a later client-side 241 when only a few frames separate the two
  steps, so the check anchors on the terminal step block: only a server MLE
  after the last "Fuzzing step" marker explains the exit; a swallowed earlier
  one must not mask a terminal client/harness 241, or a real regression would be
  missed.

The anchor is the stderr "Fuzzing step" marker, not the "Dump of fuzzed AST:"
line, because that dump is printed to stdout: run-fuzzer.sh merges the client's
streams with "> fuzzer.log 2>&1", and the block-buffered stdout dump of the
terminal step is flushed at process exit -- AFTER the terminal exception on the
unbuffered stderr -- so a dump-based anchor lands on that trailing re-dump,
past the evidence. The server error text is also matched in either its enum
"(MEMORY_LIMIT_EXCEEDED)" or prose "... memory limit exceeded" form (they print
on separate lines).
"""

import os
import re
import tempfile

_JOB = os.path.join(
    os.path.dirname(__file__), "..", "jobs", "ast_fuzzer_job.py"
)


def _job_src():
    return open(_JOB, encoding="utf-8").read()


def _scalar(src, name):
    marker = f"{name} = "
    start = src.index(marker)
    end = src.index("\n", start)
    ns = {}
    exec(src[start:end], ns)  # noqa: S102 - trusted first-party source
    return ns[name]


def _def_snippet(src, name):
    marker = f"def {name}("
    start = src.index(marker)
    end = src.index("\ndef ", start + 1)
    return src[start:end]


def _load(name):
    # Load a top-level def/assignment by name from ast_fuzzer_job.py without
    # importing its heavy CI dependencies: exec only the target snippet.
    src = _job_src()
    if name.isupper():
        return _scalar(src, name)
    ns = {}
    exec(_def_snippet(src, name), ns)  # noqa: S102 - trusted first-party source
    return ns[name]


def _assign_src(src, name):
    # Grab a single-line `NAME = ...` assignment verbatim (used for the compiled
    # regex constants) so the test exercises the exact pattern the job uses.
    marker = f"{name} = "
    start = src.index(marker)
    end = src.index("\n", start)
    return src[start:end]


def _load_terminal_block_helper():
    # _fuzzer_log_terminal_block_has_server_mle depends on _terminal_query_block,
    # re, and the module constants; exec all of them into one namespace so the
    # real function runs against a file on disk.
    src = _job_src()
    ns = {"os": os, "re": re, "Path": __import__("pathlib").Path}
    exec(f"{SERVER_MLE_SIGNATURE_SRC}", ns)  # noqa: S102
    exec(_assign_src(src, "CLIENT_241_SIGNATURE"), ns)  # noqa: S102
    exec(_assign_src(src, "STEP_MARKER"), ns)  # noqa: S102
    exec(  # noqa: S102
        f"TERMINAL_BLOCK_MAX_BYTES = {_load('TERMINAL_BLOCK_MAX_BYTES')}", ns
    )
    exec(_def_snippet(src, "_terminal_query_block"), ns)  # noqa: S102
    exec(_def_snippet(src, "_fuzzer_log_terminal_block_has_server_mle"), ns)  # noqa: S102
    return ns["_fuzzer_log_terminal_block_has_server_mle"]


SERVER_MLE_SIGNATURE_SRC = "SERVER_MLE_SIGNATURE = " + repr(
    _load("SERVER_MLE_SIGNATURE")
)


def _is_benign_memory_limit(*args):
    return _load("_is_benign_memory_limit")(*args)


def _server_mle_matches(log_text):
    pattern = _load("SERVER_MLE_SIGNATURE")
    return any(re.search(pattern, line) for line in log_text.splitlines())


def _terminal_block_has_server_mle(log_text):
    helper = _load_terminal_block_helper()
    with tempfile.NamedTemporaryFile(
        "w", suffix=".log", delete=False, encoding="utf-8"
    ) as fh:
        fh.write(log_text)
        path = fh.name
    try:
        from pathlib import Path

        return helper(Path(path))
    finally:
        os.unlink(path)


# Real client output shapes (single line, as clickhouse-client prints them).
_SERVER_MLE_LINE = (
    "Received from localhost:9000. DB::Exception: Memory limit (total) "
    "exceeded: would use 9.32 GiB, maximum: 9.31 GiB. (MEMORY_LIMIT_EXCEEDED)"
)
# The prose server form, taken verbatim from the PR #109006 fuzzer.log: the
# "Received from" line carries the lowercase "(total) memory limit exceeded"
# message while the enum "(MEMORY_LIMIT_EXCEEDED)" trails on a separate line, so
# a signature keyed only on the enum token misses this (real) shape entirely.
_SERVER_MLE_PROSE_LINE = (
    "Code: 241. DB::Exception: Received from localhost:9000. DB::Exception: "
    "(total) memory limit exceeded: would use 35.96 GiB (attempt to allocate "
    "chunk of 0.00 B), current RSS: 44.31 GiB, maximum: 44.29 GiB. Untracked "
    "memory across all threads: -8.94 MiB.. Stack trace:"
)
_CLIENT_MLE_LINE = (
    "Code: 241. DB::Exception: Client memory limit exceeded: would use "
    "10.05 MiB, maximum: 10.00 MiB. (MEMORY_LIMIT_EXCEEDED)"
)
# The stderr marker the client prints before each AST fuzz step; the classifier
# anchors the terminal block on the LAST one of these.
_STEP = "Fuzzing step 7 out of 1000"


# --- SERVER_MLE_SIGNATURE: server-origin evidence only ---


def test_signature_matches_server_transmitted_241():
    # "Received from <host>. ... (MEMORY_LIMIT_EXCEEDED)" -> server-origin.
    assert _server_mle_matches(_SERVER_MLE_LINE) is True


def test_signature_matches_server_transmitted_241_prose_form():
    # Real logs print the prose "... memory limit exceeded" on the "Received
    # from" line (enum on a separate line) -> must still match as server-origin.
    assert _server_mle_matches(_SERVER_MLE_PROSE_LINE) is True


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


# --- terminal-block scoping: only a server MLE from the LAST fuzz step counts ---

_DUMP = "Dump of fuzzed AST:\nSELECT 1"
_FRAMES = "\n".join(f"  {i}. some_frame_symbol(...)" for i in range(15))


def test_terminal_block_matches_server_mle_after_last_step():
    # A terminal server-transmitted 241 in the last step block -> benign evidence.
    log = "\n".join([_STEP, _DUMP, _SERVER_MLE_LINE] * 3 + [_STEP, _SERVER_MLE_LINE])
    assert _terminal_block_has_server_mle(log) is True


def test_terminal_block_ignores_server_mle_from_earlier_step():
    # The fuzzer swallows a query-side server MLE on one step and keeps running;
    # a LATER step exits 241 for an unrelated (e.g. client) reason. The swallowed
    # server MLE sits before the last step marker -> must NOT count as evidence.
    log = "\n".join(
        [_STEP, _DUMP, _SERVER_MLE_LINE]  # step N: swallowed server limit
        + [_STEP, _DUMP, "Query succeeded"]  # terminal step: no server MLE
    )
    assert _terminal_block_has_server_mle(log) is False


def test_terminal_block_reproduction_from_review():
    # Exact shape the reviewer reproduced: one swallowed server MLE, ~15 stack
    # frames, then a later client-side 241. A fixed 50-line tail wrongly held
    # both; anchoring on the last "Fuzzing step" marker isolates the terminal
    # client 241 in its own step block.
    log = "\n".join(
        [
            _STEP,  # step N
            _DUMP,
            _SERVER_MLE_LINE,  # swallowed on step N, fuzzer kept going
            _FRAMES,
            _STEP,  # terminal step N+1
            _DUMP,
            _CLIENT_MLE_LINE,  # exit-241 was client-side here
        ]
    )
    assert _terminal_block_has_server_mle(log) is False


def test_terminal_block_reconnect_path_with_trailing_buffered_dump():
    # The real PR #109006 shape: the terminal step's server-transmitted 241
    # (prose form, from the reconnect path) is followed by a trailing
    # "Dump of fuzzed AST:" -- the terminal step's stdout dump, block-buffered
    # and flushed at process exit AFTER the stderr exception. A dump-anchored
    # check would land on that trailing re-dump (no evidence); the "Fuzzing
    # step" anchor keeps the server MLE in the terminal block -> benign.
    log = "\n".join(
        [
            _STEP,  # terminal step marker (stderr)
            "Dump of fuzzed AST:",  # step's stdout dump
            "EXPLAIN WHATIF graph = 1 SELECT 1",
            _SERVER_MLE_PROSE_LINE,  # reconnect-path server MLE (stderr)
            _FRAMES,
            ". (MEMORY_LIMIT_EXCEEDED) (version 26.7.1.1)",
            "Lost connection to the server.",
            "Changed settings: max_insert_threads = '1', ...",
            "No changed MergeTree settings.",
            "Dump of fuzzed AST:",  # trailing buffered stdout re-dump
            "EXPLAIN WHATIF graph = 1 SELECT 1",
        ]
    )
    assert _terminal_block_has_server_mle(log) is True


def test_terminal_block_ignores_client_side_241_at_end():
    # A terminal client-side 241 (no "Received from" prefix) -> not benign.
    log = "\n".join([_STEP, _DUMP, _CLIENT_MLE_LINE])
    assert _terminal_block_has_server_mle(log) is False


def test_terminal_block_no_step_marker_scans_read_tail():
    # No AST fuzz step ran (a startup/handshake 241 before any step, or a
    # BuzzHouse run that prints no step markers): the whole read tail is the
    # terminal region, so a server MLE there still counts.
    log = "\n".join(["starting up"] + [_SERVER_MLE_LINE])
    assert _terminal_block_has_server_mle(log) is True


# --- no-marker fallback: a later client 241 must not be masked by an earlier
#     recovered server limit (BuzzHouse prints no "Fuzzing step" markers) ---


def test_no_marker_earlier_server_mle_then_client_241_not_benign():
    # BuzzHouse (no step markers): a server limit recovered from earlier in the
    # workload, then a terminal client-side 241 (e.g. --max_memory_usage_in_client
    # or a harness limit). The whole read tail holds both; the later client 241
    # is the real exit cause and must NOT be masked by the earlier server MLE.
    log = "\n".join(
        [
            "BuzzHouse: running workload",
            "SELECT 1;",
            _SERVER_MLE_PROSE_LINE,  # recovered earlier
            _FRAMES,
            "INSERT INTO t VALUES (1);",
            _CLIENT_MLE_LINE,  # terminal client-side 241
        ]
    )
    assert _terminal_block_has_server_mle(log) is False


def test_no_marker_terminal_server_mle_is_benign():
    # BuzzHouse (no step markers): a client 241 recovered from earlier, then the
    # run ends on a server-transmitted limit with no later client 241 -> benign.
    log = "\n".join(
        [
            "BuzzHouse: running workload",
            "SELECT 1;",
            _CLIENT_MLE_LINE,  # recovered earlier
            "INSERT INTO t VALUES (1);",
            _SERVER_MLE_PROSE_LINE,  # terminal server-transmitted limit
        ]
    )
    assert _terminal_block_has_server_mle(log) is True


def test_marker_delimited_block_is_authoritative_despite_earlier_client_241():
    # With a terminal "Fuzzing step" marker the block is authoritative: an AST
    # fuzz step's server-transmitted 241 stays benign even if an earlier
    # (pre-marker, hence excluded) part of the log had a client 241.
    log = "\n".join(
        [
            _CLIENT_MLE_LINE,  # earlier, before the terminal step -> excluded
            _STEP,
            "Dump of fuzzed AST:",
            _SERVER_MLE_PROSE_LINE,
        ]
    )
    assert _terminal_block_has_server_mle(log) is True


def test_marker_delimited_block_later_client_241_not_benign():
    # Even inside a marker-delimited terminal block, a client-origin 241 that
    # follows the last server-origin MLE (e.g. a step recovers a query limit,
    # then a client-side reconnect/handshake 241 sets the exit) is the real exit
    # cause and must NOT be masked. The ordering check applies to both the marker
    # and no-marker forms.
    log = "\n".join(
        [
            _STEP,
            "Dump of fuzzed AST:",
            _SERVER_MLE_PROSE_LINE,  # recovered server limit on this step
            _FRAMES,
            _CLIENT_MLE_LINE,  # later client-side 241 -> the real exit cause
        ]
    )
    assert _terminal_block_has_server_mle(log) is False


def test_terminal_block_empty_or_missing_log():
    assert _terminal_block_has_server_mle("") is False
