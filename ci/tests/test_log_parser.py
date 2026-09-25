"""
Tests for `FuzzerLogParser` (ci/jobs/scripts/log_parser.py).

The synthetic server logs below reproduce the exact line format emitted by the
server, in particular the AST Fuzzer oracle mismatch `<Fatal>` message logged by
`executeQuery` (src/Interpreters/executeQuery.cpp). Before the parser learned to
recognize it, such a `<Fatal>` produced a bare "Unknown error" in the report even
though the message was right there in the log.
"""

import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from ci.jobs.scripts.log_parser import FuzzerLogParser

# The real message: "<Fatal> ASTFuzzer: AST Fuzzer oracle mismatch detected!"
# followed by the reproducer query and the oracle-specific message.
_ORACLE_MISMATCH_LOG = """\
2026.09.04 00:44:57.900000 [ 1068 ] {7af77722-1ba2-40a2-aa08-54e577a54015} <Debug> executeQuery: (from 127.0.0.1) SELECT g, count(), min(v), approx_top_k(v) FROM oracle_tlp_agg_counter WHERE v > 50 GROUP BY g ORDER BY g ASC (stage: Complete)
2026.09.04 00:44:57.972626 [ 1068 ] {7af77722-1ba2-40a2-aa08-54e577a54015} <Fatal> ASTFuzzer: AST Fuzzer oracle mismatch detected!
Fuzzed query: SELECT g, count(), min(v), approx_top_k(v) FROM oracle_tlp_agg_counter WHERE v > 50 GROUP BY g ORDER BY g ASC
TLP Aggregate oracle mismatch!
Original result had 3 rows, partitioned result had 4 rows
2026.09.04 00:44:58.000000 [ 1068 ] {} <Information> Application: shutting down
"""


def test_parse_ast_fuzzer_oracle_mismatch(tmp_path):
    # The oracle kind ("TLP Aggregate") is folded into the failure name so that
    # distinct oracles group separately in CI DB, and the reproducer query is
    # kept in the info.
    server_log = tmp_path / "clickhouse-server.err.log"
    server_log.write_text(_ORACLE_MISMATCH_LOG, encoding="utf-8")

    parser = FuzzerLogParser(server_logs=[server_log])
    result_name, info, files = parser.parse_failure()

    assert result_name == "AST Fuzzer oracle mismatch: TLP Aggregate"
    assert result_name != FuzzerLogParser.UNKNOWN_ERROR
    assert "AST Fuzzer oracle mismatch detected!" in info
    assert "Fuzzed query: SELECT g, count(), min(v), approx_top_k(v)" in info
    assert files == []


# Every oracle-kind message emitted by `QueryOracleChecker` (src/Interpreters/
# QueryOracleChecker.cpp), mapped to the kind the parser should extract. The
# "Identity WHERE (...)" and DQP "Setting: <name>" variants are the tricky ones:
# the kind carries a parenthesized label, and DQP appends a variable trailer
# after the "!" that must be excluded.
_ORACLE_KIND_CASES = [
    ("TLP WHERE oracle mismatch!", "TLP WHERE"),
    ("NoREC oracle mismatch!", "NoREC"),
    ("TLP DISTINCT oracle mismatch!", "TLP DISTINCT"),
    ("TLP GROUP BY oracle mismatch!", "TLP GROUP BY"),
    ("TLP HAVING oracle mismatch!", "TLP HAVING"),
    ("DQP oracle mismatch! Setting: allow_experimental_analyzer", "DQP"),
    ("TLP Aggregate oracle mismatch!", "TLP Aggregate"),
    ("Identity WHERE (NOT(NOT p)) oracle mismatch!", "Identity WHERE (NOT(NOT p))"),
    ("Identity WHERE (p AND 1) oracle mismatch!", "Identity WHERE (p AND 1)"),
    ("Identity WHERE (p OR 0) oracle mismatch!", "Identity WHERE (p OR 0)"),
    ("Subquery wrap oracle mismatch!", "Subquery wrap"),
]


@pytest.mark.parametrize("oracle_line, expected_kind", _ORACLE_KIND_CASES)
def test_oracle_kind_extraction(tmp_path, oracle_line, expected_kind):
    # Each oracle kind must group under its own name, including the parenthesized
    # "Identity WHERE (...)" variants that a word-only capture would have missed.
    server_log = tmp_path / "clickhouse-server.err.log"
    server_log.write_text(
        "2026.09.04 00:44:57.972626 [ 1068 ] {q} <Fatal> ASTFuzzer: "
        "AST Fuzzer oracle mismatch detected!\n"
        "Fuzzed query: SELECT 1\n"
        f"{oracle_line}\n"
        "2026.09.04 00:44:58.000000 [ 1068 ] {} <Information> Application: shutting down\n",
        encoding="utf-8",
    )

    parser = FuzzerLogParser(server_logs=[server_log])
    result_name, _, _ = parser.parse_failure()

    assert result_name == f"AST Fuzzer oracle mismatch: {expected_kind}"


def test_sanitizer_wins_over_oracle_mismatch(tmp_path):
    # When both a server-side oracle mismatch and a sanitizer report are present,
    # the higher-signal sanitizer failure must be reported, not the oracle
    # mismatch. `parse_failure` stops at the first matching pattern, so Sanitizer
    # must stay ahead of the oracle pattern in ERROR_PATTERNS.
    server_log = tmp_path / "clickhouse-server.err.log"
    stderr_log = tmp_path / "stderr.log"
    server_log.write_text(_ORACLE_MISMATCH_LOG, encoding="utf-8")
    stderr_log.write_text(
        "==1234==ERROR: AddressSanitizer: heap-use-after-free on address 0x1\n"
        "    #0 0x55b9b8db9bc7 in DB::Foo::bar() src/Foo.cpp:10:5\n"
        "SUMMARY: AddressSanitizer: heap-use-after-free src/Foo.cpp:10:5\n",
        encoding="utf-8",
    )

    parser = FuzzerLogParser(server_logs=[server_log], stderr_logs=[stderr_log])
    result_name, info, _ = parser.parse_failure()

    assert result_name.startswith("AddressSanitizer")
    assert "oracle mismatch" not in result_name
    assert "heap-use-after-free" in info


def test_parse_ast_fuzzer_oracle_mismatch_unknown_kind(tmp_path):
    # If no "<kind> oracle mismatch!" line is present, the name stays generic but
    # still classified (not "Unknown error").
    server_log = tmp_path / "clickhouse-server.err.log"
    server_log.write_text(
        "2026.09.04 00:44:57.972626 [ 1068 ] {q} <Fatal> ASTFuzzer: "
        "AST Fuzzer oracle mismatch detected!\n"
        "Fuzzed query: SELECT 1\n"
        "2026.09.04 00:44:58.000000 [ 1068 ] {} <Information> Application: shutting down\n",
        encoding="utf-8",
    )

    parser = FuzzerLogParser(server_logs=[server_log])
    result_name, info, files = parser.parse_failure()

    assert result_name == "AST Fuzzer oracle mismatch"
    assert "Fuzzed query: SELECT 1" in info


def test_generic_fatal_fallback_surfaces_message(tmp_path):
    # An unrecognized <Fatal> message (no specific pattern matches) is surfaced
    # verbatim instead of being reported as a bare "Unknown error".
    server_log = tmp_path / "clickhouse-server.err.log"
    server_log.write_text(
        "2026.09.04 00:44:57.900000 [ 1068 ] {q} <Debug> executeQuery: SELECT 1\n"
        "2026.09.04 00:44:57.972626 [ 1068 ] {q} <Fatal> SomeNewComponent: "
        "Brand new fatal condition nobody parses yet\n"
        "Extra detail line about the failure\n"
        "2026.09.04 00:44:58.000000 [ 1068 ] {} <Information> Application: shutting down\n",
        encoding="utf-8",
    )

    parser = FuzzerLogParser(server_logs=[server_log])
    result_name, info, files = parser.parse_failure()

    assert result_name != FuzzerLogParser.UNKNOWN_ERROR
    assert result_name == (
        "SomeNewComponent: Brand new fatal condition nobody parses yet"
    )
    assert "Extra detail line about the failure" in info
    # The "<Fatal> " prefix and the following unrelated log line are not folded in.
    assert "<Fatal>" not in result_name
    assert "Application: shutting down" not in info
    # This is a lower-confidence result: callers scanning several logs must be able
    # to tell it apart from a specific classification.
    assert parser.is_generic_fatal is True


def test_quoted_fatal_in_query_text_is_not_a_generic_fatal(tmp_path):
    # A "<Fatal>" substring quoted inside query text (or a comment) on an ordinary
    # <Debug>/<Error> line must not be mistaken for a fatal record: the generic
    # fallback is anchored to the "[ <tid> ] {<qid>} <Fatal>" log-level prefix.
    server_log = tmp_path / "clickhouse-server.err.log"
    server_log.write_text(
        "2026.09.04 00:44:57.900000 [ 1068 ] {q} <Debug> executeQuery: "
        "(from 127.0.0.1) SELECT '<Fatal> not an error' (stage: Complete)\n"
        "2026.09.04 00:44:58.000000 [ 1068 ] {} <Information> Application: shutting down\n",
        encoding="utf-8",
    )

    parser = FuzzerLogParser(server_logs=[server_log])
    result_name, info, _ = parser.parse_failure()

    assert result_name == FuzzerLogParser.UNKNOWN_ERROR
    assert parser.is_generic_fatal is False
    assert "not an error" not in result_name
    # Same anchoring in the unnamed-fatal scan: callers treat anything it returns as
    # crash evidence, so a quoted "<Fatal>" there fails a run that never failed.
    assert parser.find_unnamed_fatals() == []


def test_find_unnamed_fatals_returns_real_fatal_records(tmp_path):
    # The counterpart to the anchoring above: a genuine unclassified <Fatal> record must
    # still be returned, whole line, while the quoted one beside it is left out.
    server_log = tmp_path / "clickhouse-server.err.log"
    server_log.write_text(
        "2026.09.04 00:44:57.900000 [ 1068 ] {q} <Debug> executeQuery: "
        "(from 127.0.0.1) SELECT '<Fatal> not an error' (stage: Complete)\n"
        "2026.09.04 00:44:58.000000 [ 1068 ] {q} <Fatal> SomeComponent: "
        "unexplained fatal\n",
        encoding="utf-8",
    )

    parser = FuzzerLogParser(server_logs=[server_log])
    found = parser.find_unnamed_fatals()

    assert len(found) == 1
    assert found[0].endswith("<Fatal> SomeComponent: unexplained fatal")


def test_unknown_error_when_no_fatal(tmp_path):
    # With neither a specific pattern nor any <Fatal> message, the parser still
    # falls back to "Unknown error".
    server_log = tmp_path / "clickhouse-server.err.log"
    server_log.write_text(
        "2026.09.04 00:44:57.900000 [ 1068 ] {q} <Debug> executeQuery: SELECT 1\n"
        "2026.09.04 00:44:58.000000 [ 1068 ] {} <Information> Application: shutting down\n",
        encoding="utf-8",
    )

    parser = FuzzerLogParser(server_logs=[server_log])
    result_name, info, _ = parser.parse_failure()

    assert result_name == FuzzerLogParser.UNKNOWN_ERROR
    assert "Lost connection to server" in info
    assert parser.is_generic_fatal is False


def test_generic_fatal_flag_not_set_for_specific_classification(tmp_path):
    # A specific classification (here an oracle mismatch) must not be flagged as a
    # generic fatal, so `stress_job.py` treats it as a definitive, higher-priority
    # result than a generic <Fatal> on another replica.
    server_log = tmp_path / "clickhouse-server.err.log"
    server_log.write_text(_ORACLE_MISMATCH_LOG, encoding="utf-8")

    parser = FuzzerLogParser(server_logs=[server_log])
    parser.parse_failure()

    assert parser.is_generic_fatal is False


def test_specific_pattern_wins_over_generic_fatal(tmp_path):
    # A logical error is a <Fatal> too; the specific pattern must classify it
    # rather than the generic fallback treating it as an opaque message.
    server_log = tmp_path / "clickhouse-server.err.log"
    server_log.write_text(
        "2026.09.04 00:44:57.972626 [ 1068 ] {q} <Fatal> : Logical error: "
        "'Bad cast from type A to type B'.\n"
        "2026.09.04 00:44:58.000000 [ 1068 ] {} <Information> Application: shutting down\n",
        encoding="utf-8",
    )

    parser = FuzzerLogParser(server_logs=[server_log])
    result_name, _, _ = parser.parse_failure()

    assert result_name.startswith("Logical error")


_EXPECTED_KILL_LOG = (
    "2026.09.04 00:44:57.900000 [ 1068 ] {q} <Debug> executeQuery: SELECT 1\n"
    "2026.09.04 00:44:58.000000 [ 1 ] {} <Fatal> Application: "
    "Child process was terminated by signal 9 (KILL).\n"
)


def test_expected_kill_is_not_a_generic_fatal(tmp_path):
    # The end-of-run SIGKILL is a <Fatal> record a healthy node writes too. It is
    # deferred by the Signal pattern, and the generic fallback must not pick it up
    # either, or every run would be reported as failed for its own teardown. Only a
    # caller that already knows the run failed may name it, on the second pass.
    server_log = tmp_path / "clickhouse-server.err.log"
    server_log.write_text(_EXPECTED_KILL_LOG, encoding="utf-8")

    parser = FuzzerLogParser(server_logs=[server_log])
    result_name, _, _ = parser.parse_failure()

    assert result_name == FuzzerLogParser.UNKNOWN_ERROR
    assert parser.is_generic_fatal is False
    assert parser.find_unnamed_fatals() == []

    result_name, _, _ = parser.parse_failure(allow_expected_only=True)
    assert result_name.startswith("Child process was terminated by signal 9")
    assert parser.is_generic_fatal is False


def test_generic_fatal_outranks_expected_only_line(tmp_path):
    # An unclassified <Fatal> next to the expected kill is crash evidence: it wins
    # even when the caller opted into naming the run after the expected line.
    server_log = tmp_path / "clickhouse-server.err.log"
    server_log.write_text(
        "2026.09.04 00:44:57.972626 [ 1068 ] {q} <Fatal> SomeComponent: "
        "unexplained fatal\n" + _EXPECTED_KILL_LOG,
        encoding="utf-8",
    )

    parser = FuzzerLogParser(server_logs=[server_log])
    result_name, _, _ = parser.parse_failure(allow_expected_only=True)

    assert result_name == "SomeComponent: unexplained fatal"
    assert parser.is_generic_fatal is True


def test_generic_fatal_found_in_rotated_log(tmp_path):
    # The fallback scans every server log handed in, the gzipped rotated ones too.
    import gzip

    rotated = tmp_path / "clickhouse-server.err.log.1.gz"
    with gzip.open(rotated, "wt", encoding="utf-8") as f:
        f.write(
            "2026.09.04 00:44:57.972626 [ 1068 ] {q} <Fatal> SomeComponent: "
            "rotated away\n"
            "2026.09.04 00:44:58.000000 [ 1068 ] {} <Information> Application: x\n"
        )
    current = tmp_path / "clickhouse-server.err.log"
    current.write_text(_EXPECTED_KILL_LOG, encoding="utf-8")

    parser = FuzzerLogParser(server_logs=[current, rotated])
    result_name, _, _ = parser.parse_failure()

    assert result_name == "SomeComponent: rotated away"
    assert parser.is_generic_fatal is True


def test_watchdog_signal_anchored_to_fatal_record(tmp_path):
    # A live server's ShellCommand logging "<Error> ... terminated by signal 6" is not
    # the server dying; only the watchdog's <Fatal> Application record is, and its
    # "<Fatal> Application: " prefix is stripped from the name.
    server_log = tmp_path / "clickhouse-server.err.log"
    server_log.write_text(
        "2026.09.04 00:44:57.900000 [ 1068 ] {q} <Error> ShellCommand: "
        "Child process was terminated by signal 6.\n",
        encoding="utf-8",
    )
    parser = FuzzerLogParser(server_logs=[server_log])
    assert parser.parse_failure()[0] == FuzzerLogParser.UNKNOWN_ERROR

    server_log.write_text(
        "2026.09.04 00:44:58.000000 [ 1 ] {} <Fatal> Application: "
        "Child process was terminated by signal 6.\n",
        encoding="utf-8",
    )
    parser = FuzzerLogParser(server_logs=[server_log])
    result_name, _, _ = parser.parse_failure()
    assert result_name.startswith("Child process was terminated by signal 6")
    assert "<Fatal>" not in result_name
