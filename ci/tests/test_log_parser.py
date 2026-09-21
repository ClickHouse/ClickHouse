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

    parser = FuzzerLogParser(
        server_log=str(server_log), stderr_log=str(stderr_log), fuzzer_log=""
    )
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

    parser = FuzzerLogParser(
        server_log=str(server_log), stderr_log="", fuzzer_log=""
    )
    result_name, info, _ = parser.parse_failure()

    assert result_name == FuzzerLogParser.UNKNOWN_ERROR
    assert parser.is_generic_fatal is False
    assert "not an error" not in result_name


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
    assert parser.is_generic_fatal is False


def test_generic_fatal_flag_not_set_for_specific_classification(tmp_path):
    # A specific classification (here an oracle mismatch) must not be flagged as a
    # generic fatal, so `stress_job.py` treats it as a definitive, higher-priority
    # result than a generic <Fatal> on another replica.
    server_log = tmp_path / "clickhouse-server.err.log"
    server_log.write_text(_ORACLE_MISMATCH_LOG, encoding="utf-8")

    parser = FuzzerLogParser(
        server_log=str(server_log), stderr_log="", fuzzer_log=""
    )
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

    parser = FuzzerLogParser(
        server_log=str(server_log), stderr_log="", fuzzer_log=""
    )
    result_name, _, _ = parser.parse_failure()

    assert result_name.startswith("Logical error")


# A real record from `Upgrade check (amd_release)`: the stress phase injected a
# memory-limit fault, so `executeQuery` logged the exception together with the whole
# query text. `toOneLineQuery` keeps the newline after every SQL comment, so the six
# comment lines of tests/queries/0_stateless/
# 04057_aggregate_function_nothing_with_parameters.sql land on their own log lines and
# only the first one carries the "(in query:" marker.
_QUOTED_MULTILINE_QUERY_LOG = """\
2026.09.21 08:26:23.414235 [ 27178 ] {f1e16e4a-075e-436f-9bd2-da2f5d230e4f} <Error> executeQuery: Code: 241. DB::Exception: Query memory tracker: fault injected. Would use 149.85 KiB, maximum: 4.66 GiB: While executing AggregatingTransform. (MEMORY_LIMIT_EXCEEDED) (version 26.8.9.10 (official build)) (from [::1]:42798) (comment: 04057_aggregate_function_nothing_with_parameters.sql-test_v0i7w6q0l1b9) (query 1, line 1) (in query: -- Regression test for assertion failure when aggregate function combinators
 -- wrap AggregateFunctionNothing that carries parameters.
 -- These queries used to crash with: Assertion `parameters == nested_func->getParameters()' failed
 SELECT quantileIfArrayArray(0.5)([[NULL]], [[1]]);), Stack trace (when copying this message, always include the lines below):

0. src/Common/Exception.cpp:166:1: DB::Exception::Exception(DB::Exception::MessageMasked&&, int, bool) @ 0x000000001707dbaa
4. src/Common/MemoryTracker.cpp:332:19: MemoryTracker::allocImpl(long, bool, MemoryTracker*, double) @ 0x000000001711c0b6
 (version 26.8.9.10 (official build))
"""

# A genuine assertion: glibc writes it to stderr as a bare line, with no log-record
# prefix to anchor on.
_REAL_ASSERTION_LINE = (
    "clickhouse: /src/Foo.h:31: DB::Foo::Foo(): Assertion `real_expr' failed.\n"
)


def test_assertion_quoted_in_multiline_query_is_not_a_failure(tmp_path):
    # The failure phrase sits on a continuation line of the quoted query, so the
    # line itself carries no marker to skip it by.
    server_log = tmp_path / "clickhouse-server.err.log"
    server_log.write_text(_QUOTED_MULTILINE_QUERY_LOG, encoding="utf-8")

    parser = FuzzerLogParser(
        server_log=str(server_log), stderr_log="", fuzzer_log=""
    )
    result_name, _, _ = parser.parse_failure()

    assert "nested_func" not in result_name
    assert result_name == FuzzerLogParser.UNKNOWN_ERROR


def test_real_failure_after_a_quoted_multiline_query_wins(tmp_path):
    # Naming the run after the quote is not just noise: `find_failure` returns the
    # first candidate, so the quote hides every real failure logged after it.
    server_log = tmp_path / "clickhouse-server.err.log"
    server_log.write_text(
        _QUOTED_MULTILINE_QUERY_LOG + _REAL_ASSERTION_LINE, encoding="utf-8"
    )

    parser = FuzzerLogParser(
        server_log=str(server_log), stderr_log="", fuzzer_log=""
    )
    result_name, _, _ = parser.parse_failure()

    assert "real_expr" in result_name
    assert "nested_func" not in result_name


def test_match_in_stack_frame_after_the_quoted_query_is_still_found(tmp_path):
    # The stack-trace marker ends the quoted query, so a failing frame of the very
    # record that quoted a query is still reported.
    server_log = tmp_path / "clickhouse-server.err.log"
    server_log.write_text(
        "2026.09.21 08:26:23.414235 [ 27178 ] {q} <Error> executeQuery: Code: 241. "
        "DB::Exception: fault injected. (MEMORY_LIMIT_EXCEEDED) "
        "(in query: -- a comment\n"
        " -- Assertion `quoted_expr' failed\n"
        " SELECT 1;), Stack trace (when copying this message, always include the "
        "lines below):\n"
        "\n"
        "0. src/Common/Foo.cpp:1: _LIBCPP_ASSERT_VALID_ELEMENT_ACCESS @ 0x1\n",
        encoding="utf-8",
    )

    parser = FuzzerLogParser(
        server_log=str(server_log), stderr_log="", fuzzer_log=""
    )
    result_name, _, _ = parser.parse_failure()

    assert "_LIBCPP_ASSERT_VALID_ELEMENT_ACCESS" in result_name
    assert "quoted_expr" not in result_name


def test_quoted_query_closed_on_its_own_line_does_not_shadow_a_later_match(tmp_path):
    # A record without a stack trace ends at the ")" that closes "(in query:", so a
    # later failure is not read as part of that query either.
    server_log = tmp_path / "clickhouse-server.err.log"
    server_log.write_text(
        "2026.09.21 08:26:23.414235 [ 27178 ] {q} <Error> executeQuery: Code: 47. "
        "DB::Exception: Unknown expression identifier. (UNKNOWN_IDENTIFIER) "
        "(in query: -- a comment\n"
        " -- Assertion `quoted_expr' failed\n"
        " SELECT 1;)\n" + _REAL_ASSERTION_LINE,
        encoding="utf-8",
    )

    parser = FuzzerLogParser(
        server_log=str(server_log), stderr_log="", fuzzer_log=""
    )
    result_name, _, _ = parser.parse_failure()

    assert "real_expr" in result_name
    assert "quoted_expr" not in result_name


def test_failure_on_a_record_start_after_an_unterminated_quote_is_still_found(tmp_path):
    # A log truncated mid-record leaves a quoted query that never closes. A failure
    # opening a record of its own is never a continuation of it, whatever precedes it.
    server_log = tmp_path / "clickhouse-server.err.log"
    server_log.write_text(
        "2026.09.21 08:26:23.414235 [ 27178 ] {q} <Error> executeQuery: Code: 241. "
        "DB::Exception: fault injected. (MEMORY_LIMIT_EXCEEDED) "
        "(in query: -- a comment\n"
        " -- Logical error: 'quoted only'\n"
        " SELECT 1;\n"
        "2026.09.21 08:26:24.000000 [ 27178 ] {q2} <Error> executeQuery: Code: 49. "
        "DB::Exception: Logical error: 'Bad cast from type A to type B'. "
        "(LOGICAL_ERROR) (version 26.8.9.10 (official build)) (from [::1]:1) "
        "(in query: SELECT 1)\n",
        encoding="utf-8",
    )

    parser = FuzzerLogParser(
        server_log=str(server_log), stderr_log="", fuzzer_log=""
    )
    result_name, _, _ = parser.parse_failure()

    assert "Bad cast from type A to type B" in result_name
    assert "quoted only" not in result_name
