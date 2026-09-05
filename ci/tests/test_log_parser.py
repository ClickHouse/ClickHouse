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

    parser = FuzzerLogParser(
        server_log=str(server_log), stderr_log="", fuzzer_log=""
    )
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

    parser = FuzzerLogParser(
        server_log=str(server_log), stderr_log="", fuzzer_log=""
    )
    result_name, _, _ = parser.parse_failure()

    assert result_name == f"AST Fuzzer oracle mismatch: {expected_kind}"


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

    parser = FuzzerLogParser(
        server_log=str(server_log), stderr_log="", fuzzer_log=""
    )
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

    parser = FuzzerLogParser(
        server_log=str(server_log), stderr_log="", fuzzer_log=""
    )
    result_name, info, files = parser.parse_failure()

    assert result_name != FuzzerLogParser.UNKNOWN_ERROR
    assert result_name == (
        "SomeNewComponent: Brand new fatal condition nobody parses yet"
    )
    assert "Extra detail line about the failure" in info
    # The "<Fatal> " prefix and the following unrelated log line are not folded in.
    assert "<Fatal>" not in result_name
    assert "Application: shutting down" not in info


def test_unknown_error_when_no_fatal(tmp_path):
    # With neither a specific pattern nor any <Fatal> message, the parser still
    # falls back to "Unknown error".
    server_log = tmp_path / "clickhouse-server.err.log"
    server_log.write_text(
        "2026.09.04 00:44:57.900000 [ 1068 ] {q} <Debug> executeQuery: SELECT 1\n"
        "2026.09.04 00:44:58.000000 [ 1068 ] {} <Information> Application: shutting down\n",
        encoding="utf-8",
    )

    parser = FuzzerLogParser(
        server_log=str(server_log), stderr_log="", fuzzer_log=""
    )
    result_name, info, _ = parser.parse_failure()

    assert result_name == FuzzerLogParser.UNKNOWN_ERROR
    assert "Lost connection to server" in info


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

    parser = FuzzerLogParser(
        server_log=str(server_log), stderr_log="", fuzzer_log=""
    )
    result_name, _, _ = parser.parse_failure()

    assert result_name.startswith("Logical error")
