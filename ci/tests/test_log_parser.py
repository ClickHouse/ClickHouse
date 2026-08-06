import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from ci.jobs.scripts.log_parser import FuzzerLogParser

_ASAN_CHECK_FAILED_STDERR = """\
==2138==WARNING: ASan doesn't fully support makecontext/swapcontext functions and may produce false positives in some cases!
AddressSanitizer: CHECK failed: sanitizer_allocator_secondary.h:200 "((nearest_chunk)) < ((h->map_beg + h->map_size))" (0x7b44c2461000, 0x0) (tid=3005)
    #0 0x55b9822b6021 in __asan::CheckUnwind() asan_rtl.cpp
    #1 0x55b9822cedcb in __sanitizer::CheckFailed(char const*, int, char const*, unsigned long long, unsigned long long) sanitizer_termination.cpp
    #2 0x55b9b8db9bc7 in DB::ExceptionKeepingTransform::work() src/Processors/Transforms/ExceptionKeepingTransform.cpp:189:42
    #3 0x55b9b85a724d in DB::executeJob(DB::ExecutingGraph::Node*, DB::ReadProgressCallback*) src/Processors/Executors/ExecutionThreadContext.cpp:54:28
    #4 0x55b9b85a724d in DB::ExecutionThreadContext::executeTask() src/Processors/Executors/ExecutionThreadContext.cpp:103:9
    #5 0x55b9b85728d8 in DB::PipelineExecutor::executeStepImpl(unsigned long, DB::IAcquiredSlot*, std::__1::atomic<bool>*) src/Processors/Executors/PipelineExecutor.cpp:363:26
    #6 0x55b9b8571ae1 in DB::PipelineExecutor::executeStep(std::__1::atomic<bool>*) src/Processors/Executors/PipelineExecutor.cpp:191:5
    #7 0x55b9b85cb318 in DB::PushingPipelineExecutor::finish() src/Processors/Executors/PushingPipelineExecutor.cpp:131:47
    #8 0x7f4ae1fdb8cf  misc/../sysdeps/unix/sysv/linux/x86_64/clone3.S:81

dpkg: error processing package clickhouse-server (--install):
"""


def test_parse_failure_prefers_asan_check_failed_over_server_assertion(tmp_path):
    server_log = tmp_path / "clickhouse-server.err.log"
    stderr_log = tmp_path / "stderr.log"

    server_log.write_text(
        "2026.06.09 00:00:00.000000 [ 1 ] {} <Fatal> Application: "
        "Assertion 'px != 0' failed.\n",
        encoding="utf-8",
    )
    stderr_log.write_text(
        "AddressSanitizer: CHECK failed: sanitizer_allocator_secondary.h:200 "
        "\"((nearest_chunk)) < ((h->map_beg + h->map_size))\" "
        "(0x7b44c2461000, 0x0) (tid=3005)\n"
        "    <empty stack>\n"
        "\n"
        "dpkg: error processing package clickhouse-server (--install):\n",
        encoding="utf-8",
    )

    parser = FuzzerLogParser(
        server_log=str(server_log),
        stderr_log=str(stderr_log),
        fuzzer_log="",
    )

    result_name, info, files = parser.parse_failure()

    assert result_name == "AddressSanitizer (STID: None)"
    assert "AddressSanitizer: CHECK failed:" in info
    assert "Assertion 'px != 0' failed" not in info
    assert "dpkg" not in info
    assert files == []


def test_parse_failure_asan_check_failed_with_stack_trace(tmp_path):
    server_log = tmp_path / "clickhouse-server.err.log"
    stderr_log = tmp_path / "stderr.log"

    server_log.write_text(
        "2026.06.09 00:00:00.000000 [ 1 ] {} <Fatal> Application: "
        "Assertion 'px != 0' failed.\n",
        encoding="utf-8",
    )
    stderr_log.write_text(_ASAN_CHECK_FAILED_STDERR, encoding="utf-8")

    parser = FuzzerLogParser(
        server_log=str(server_log),
        stderr_log=str(stderr_log),
        fuzzer_log="",
    )

    result_name, info, files = parser.parse_failure()

    assert result_name == "AddressSanitizer (STID: 1288-3bd5)"
    assert "AddressSanitizer: CHECK failed:" in info
    assert "Assertion 'px != 0' failed" not in info
    assert "dpkg" not in info
    assert files == []


def test_parse_failure_logical_error_name_drops_dangling_stack_trace_marker(tmp_path):
    server_log = tmp_path / "clickhouse-server.err.log"

    server_log.write_text(
        "2026.06.14 20:00:01.000000 [ 200 ] {} <Fatal> : Logical error: "
        "'std::exception. Code: 1001, type: std::__1::future_error, "
        "e.what() = The associated promise has been destructed prior to the "
        "associated state becoming ready., Stack trace (when copying this "
        "message, always include the lines below):\n"
        "\n"
        "0. ./contrib/llvm-project/libcxx/include/future:509:25: "
        "std::promise<void>::~promise() @ 0x000000002cbf6d04\n"
        "2026.06.14 20:00:02.000000 [ 200 ] {} <Fatal> BaseDaemon: Stack trace:\n"
        "2026.06.14 20:00:02.000000 [ 200 ] {} <Fatal> BaseDaemon: "
        "1. ./src/Common/Exception.cpp:60: DB::abortOnFailedAssertion() @ 0x14d2262e\n",
        encoding="utf-8",
    )

    parser = FuzzerLogParser(
        server_log=str(server_log),
        stderr_log="",
        fuzzer_log="",
    )

    result_name, info, files = parser.parse_failure()

    # The failure name must not end with the "always include the lines below):"
    # promise when no frames follow it (the first log line is all the name has).
    assert "always include the lines below" not in result_name
    assert result_name.startswith("Logical error: 'std::exception.")
    assert "The associated promise has been destructed" in result_name
    assert "(STID:" in result_name
    # The frames are still preserved in the separate stack-trace section of the info.
    assert "abortOnFailedAssertion" in info


def test_parse_failure_logical_error_finds_format_string_despite_interleaving(
    tmp_path,
):
    server_log = tmp_path / "clickhouse-server.err.log"

    # A message from another thread (with its multi-line stack trace) lands between
    # the `Logical error:` and `Format string:` fatal lines, pushing the format
    # string out of the 10-line window around the match. The failure name must
    # still be built from the format string (without the raw line's quotes).
    interleaved_message = (
        "2026.08.01 18:35:11.673118 [ 4327 ] {} <Error> TCPHandler: Code: 210. "
        "DB::NetException: Connection reset by peer, while writing to socket. "
        "(NETWORK_ERROR), Stack trace (when copying this message, always include "
        "the lines below):\n"
        "\n"
        + "".join(
            f"{i}. src/Server/TCPHandler.cpp:{i}: DB::someFunction() @ 0x{i:016x}\n"
            for i in range(14)
        )
        + "\n"
    )
    server_log.write_text(
        "2026.08.01 18:35:11.672071 [ 4353 ] {} <Fatal> : Logical error: "
        "'Query context must be created after authentication'.\n"
        + interleaved_message
        + "2026.08.01 18:35:11.675219 [ 4353 ] {} <Fatal> : Format string: "
        "'Query context must be created after authentication'.\n"
        "2026.08.01 18:35:11.694220 [ 4353 ] {} <Fatal> : Stack trace (when "
        "copying this message, always include the lines below):\n"
        "\n"
        "0. ./src/Common/Exception.cpp:66:5: DB::abortOnFailedAssertion() "
        "@ 0x00000000139d383c\n"
        "1. ./src/Interpreters/Session.cpp:690:15: "
        "DB::Session::makeQueryContextImpl() @ 0x0000000018143638\n",
        encoding="utf-8",
    )

    parser = FuzzerLogParser(
        server_log=str(server_log),
        stderr_log="",
        fuzzer_log="",
    )

    result_name, info, files = parser.parse_failure()

    assert result_name.startswith(
        "Logical error: Query context must be created after authentication (STID:"
    )
    assert "'" not in result_name


def test_parse_failure_logical_error_without_own_format_string(tmp_path):
    server_log = tmp_path / "clickhouse-server.err.log"

    # `abortOnFailedAssertion(description)` logs no `Format string:` line when the
    # format string is empty. The search for it must stay inside the matched failure
    # (the next fatal message of the same thread), so that a later unrelated logical
    # error does not rename the first one with its own format string.
    server_log.write_text(
        "2026.08.01 18:35:11.672071 [ 4353 ] {} <Fatal> : Logical error: "
        "'first error without format'.\n"
        "2026.08.01 18:35:11.694220 [ 4353 ] {} <Fatal> : Stack trace (when "
        "copying this message, always include the lines below):\n"
        "\n"
        "0. ./src/Common/Exception.cpp:66:5: DB::abortOnFailedAssertion() "
        "@ 0x00000000139d383c\n"
        "2026.08.01 18:36:00.000000 [ 4400 ] {} <Fatal> : Logical error: "
        "'second error normalized A'.\n"
        "2026.08.01 18:36:00.000001 [ 4400 ] {} <Fatal> : Format string: "
        "'second error normalized {}'.\n",
        encoding="utf-8",
    )

    parser = FuzzerLogParser(
        server_log=str(server_log),
        stderr_log="",
        fuzzer_log="",
    )

    result_name, _, _ = parser.parse_failure()

    assert result_name.startswith("Logical error: 'first error without format' (STID:")
    assert "second error normalized" not in result_name


def test_parse_failure_logical_error_stack_trace_str_without_thread_ids():
    # Same scenario as above, but with the `stack_trace_str` input, whose lines have
    # no server-log `[ thread_id ] {}` prefixes. The search for the `Format string:`
    # line cannot be bounded by the thread there, so it must be bounded by the
    # failure block instead: a `Format string:` that appears after the next failure
    # line belongs to that failure, not to the matched one.
    parser = FuzzerLogParser(
        server_log="",
        stack_trace_str=(
            "Logical error: 'first error without format'.\n"
            "Stack trace (when copying this message, always include the lines "
            "below):\n"
            "\n"
            "0. ./src/Common/Exception.cpp:66:5: DB::abortOnFailedAssertion() "
            "@ 0x00000000139d383c\n"
            "Logical error: 'second error normalized A'.\n"
            "Format string: 'second error normalized {}'.\n"
        ),
    )

    result_name, _, _ = parser.parse_failure()

    assert result_name.startswith("Logical error: first error without format (STID:")
    assert "second error normalized" not in result_name


def test_parse_failure_logical_error_stack_trace_str_next_failure_other_pattern():
    # Same as above, but the later failure is carried by a different pattern variant
    # (an assertion instead of another `Logical error:`). The thread-less search must
    # stop at the next failure line of any kind, so the assertion's `Format string:`
    # must not rename the first failure.
    parser = FuzzerLogParser(
        server_log="",
        stack_trace_str=(
            "Logical error: 'first error without format'.\n"
            "Stack trace (when copying this message, always include the lines "
            "below):\n"
            "\n"
            "0. ./src/Common/Exception.cpp:66:5: DB::abortOnFailedAssertion() "
            "@ 0x00000000139d383c\n"
            "Assertion `count == 0` failed.\n"
            "Format string: 'second assertion normalized {}'.\n"
        ),
    )

    result_name, _, _ = parser.parse_failure()

    assert result_name.startswith("Logical error: first error without format (STID:")
    assert "second assertion normalized" not in result_name


def test_parse_failure_logical_error_file_without_thread_ids(tmp_path):
    # A plain file input whose lines carry no server-log `[ thread_id ] {}`
    # prefixes - e.g. stderr.log standing in for an absent server log. The search
    # for the `Format string:` line cannot be bounded by the thread, so it must be
    # bounded by the failure block, exactly as for the `stack_trace_str` input: a
    # `Format string:` that appears after the next failure line of any kind (here
    # an assertion) belongs to that failure and must not rename the matched one.
    server_log = tmp_path / "stderr.log"
    server_log.write_text(
        "Logical error: 'first error without format'.\n"
        "Stack trace (when copying this message, always include the lines "
        "below):\n"
        "\n"
        "0. ./src/Common/Exception.cpp:66:5: DB::abortOnFailedAssertion() "
        "@ 0x00000000139d383c\n"
        "Assertion `count == 0` failed.\n"
        "Format string: 'second assertion normalized {}'.\n",
        encoding="utf-8",
    )

    parser = FuzzerLogParser(
        server_log=str(server_log),
        stderr_log="",
        fuzzer_log="",
    )

    result_name, _, _ = parser.parse_failure()

    assert result_name.startswith("Logical error: 'first error without format' (STID:")
    assert "second assertion normalized" not in result_name


def test_parse_failure_logical_error_file_without_thread_ids_own_format_string(
    tmp_path,
):
    # The positive counterpart: on a thread-less file input, a `Format string:`
    # line that belongs to the matched failure (before any other failure line)
    # must still normalize the name.
    server_log = tmp_path / "stderr.log"
    server_log.write_text(
        "Logical error: 'Cannot parse element A: expected B'.\n"
        "Format string: 'Cannot parse element {}: expected {}'.\n"
        "Stack trace (when copying this message, always include the lines "
        "below):\n"
        "\n"
        "0. ./src/Common/Exception.cpp:66:5: DB::abortOnFailedAssertion() "
        "@ 0x00000000139d383c\n",
        encoding="utf-8",
    )

    parser = FuzzerLogParser(
        server_log=str(server_log),
        stderr_log="",
        fuzzer_log="",
    )

    result_name, _, _ = parser.parse_failure()

    assert result_name.startswith(
        "Logical error: Cannot parse element A: expected B (STID:"
    )
    assert "'" not in result_name.partition(" (STID:")[0].removeprefix(
        "Logical error: "
    )


def test_parse_failure_logical_error_ignores_query_echo_comment(tmp_path):
    # A regression test's SQL comment that quotes a crash signature is echoed verbatim into
    # the server log by executeQuery. The scraper must name the failure after the real
    # <Fatal> logical-error record, not the earlier query-echo comment (which would otherwise
    # mislabel an unrelated crash and pollute failure tracking). It must also report the
    # crashing query (of the real record), not the earlier, unrelated echoed query.
    server_log = tmp_path / "clickhouse-server.err.log"

    server_log.write_text(
        # 1) An unrelated query (q1) that quotes the crash signature on its own FORMATTED
        #    executeQuery line (not a continuation). The whole-log fallback would extract this
        #    q1 record, so the test only passes if the SELECTED fatal record's id (q2) is reused.
        "2026.07.23 16:42:06.439000 [ 100 ] {q1} <Debug> executeQuery: "
        "(from [::1]:1) (in query: SELECT 1 AS q1_marker -- Logical error '!part.empty()')\n"
        # 2) The crashing query (q2) is echoed too, before it aborts.
        "2026.07.23 17:27:48.500000 [ 3327 ] {q2} <Debug> executeQuery: "
        "(from [::1]:2) (in query: SELECT q2_marker FROM parallel_replicas_table)\n"
        # 3) The real crash: a formatted <Fatal> logical-error record for q2.
        "2026.07.23 17:27:48.618979 [ 3327 ] {q2} <Fatal> : Logical error: "
        "'Got read request from replica 1 for unknown stream test_db.t'.\n"
        "2026.07.23 17:27:48.619099 [ 3327 ] {q2} <Fatal> BaseDaemon: Stack trace:\n"
        "2026.07.23 17:27:48.619100 [ 3327 ] {q2} <Fatal> BaseDaemon: "
        "1. ./src/Common/Exception.cpp:60: DB::abortOnFailedAssertion() @ 0x14d2262e\n",
        encoding="utf-8",
    )

    parser = FuzzerLogParser(
        server_log=str(server_log),
        stderr_log="",
        fuzzer_log="",
    )

    result_name, info, files = parser.parse_failure()

    # Named after the genuine <Fatal> record, not the query-echo comment.
    assert result_name.startswith("Logical error: 'Got read request")
    assert "!part.empty()" not in result_name
    # The reported failing query is the crashing query (q2), selected via the fatal record's
    # query id - not the earlier, unrelated q1 query the whole-log grep would have found first.
    assert "q2_marker" in info
    assert "q1_marker" not in info


def test_parse_failure_ignores_nonfatal_logical_error_warning(tmp_path):
    # Benign non-fatal records may mention the phrase (e.g. KafkaConsumer logs
    # "<Warning> ...: Logical error. Not all polled messages were processed."). The pattern
    # requires the <Fatal> level and the "Logical error:" colon, so such a warning must not be
    # selected ahead of a genuine later fatal logical error.
    server_log = tmp_path / "clickhouse-server.err.log"

    server_log.write_text(
        "2026.07.23 09:00:00.000000 [ 50 ] {q1} <Warning> StorageKafka (kafka): "
        "Logical error. Not all polled messages were processed.\n"
        "2026.07.23 10:00:00.000000 [ 100 ] {q2} <Fatal> : Logical error: "
        "'the genuine fatal one'.\n",
        encoding="utf-8",
    )

    parser = FuzzerLogParser(
        server_log=str(server_log),
        stderr_log="",
        fuzzer_log="",
    )

    result_name, info, files = parser.parse_failure()

    assert result_name.startswith("Logical error: 'the genuine fatal one'")
    assert "Not all polled messages" not in result_name


def test_parse_failure_logical_error_ignores_fake_prefix_in_embedded_query(tmp_path):
    # The signal handler logs the crashing query inline, before the real diagnostic. A query
    # that embeds a COMPLETE log-record prefix (date, time, thread, id, level) mid-line must not
    # be mistaken for a genuine logical error: the pattern is anchored to the line start (^), so
    # a fully-formed prefix appearing mid-line does not match and the actual signal is reported.
    server_log = tmp_path / "clickhouse-server.err.log"

    server_log.write_text(
        "2026.07.23 10:00:00.000000 [ 100 ] {realqid} <Fatal> BaseDaemon: "
        "(version 26.8) (from thread 100) (query_id: realqid) "
        "(query: SELECT 1 -- 2026.07.23 08:00:00.000000 [ 9 ] {fake} <Fatal> : "
        "Logical error: 'injected') Received signal 11 (SIGSEGV).\n"
        "2026.07.23 10:00:00.100000 [ 100 ] {realqid} <Fatal> BaseDaemon: Stack trace:\n",
        encoding="utf-8",
    )

    parser = FuzzerLogParser(
        server_log=str(server_log),
        stderr_log="",
        fuzzer_log="",
    )

    result_name, info, files = parser.parse_failure()

    assert "Received signal 11" in result_name
    assert "injected" not in result_name


def test_get_failed_query_extracts_inline_inconsistent_ast_query(tmp_path):
    # `Inconsistent AST formatting` logical errors abort during construction (debug/sanitizer)
    # before any executeQuery record is logged; the offending query is inline in the fatal
    # message. get_failed_query must return it even when a (nonempty) fatal-record query id is
    # supplied - otherwise the id-based executeQuery lookup finds nothing.
    server_log = tmp_path / "clickhouse-server.err.log"

    server_log.write_text(
        "2026.07.23 10:00:00.000000 [ 100 ] {qid} <Fatal> : Logical error: "
        "'Inconsistent AST formatting: the query:\n"
        "SELECT inline_ast_marker FROM t\n"
        "cannot parse query back from ...'.\n",
        encoding="utf-8",
    )

    parser = FuzzerLogParser(
        server_log=str(server_log),
        stderr_log="",
        fuzzer_log="",
    )

    # Even with a nonempty query id (as parse_failure would pass), the inline query is returned.
    assert parser.get_failed_query(query_id="qid") == "SELECT inline_ast_marker FROM t"


def test_parse_failure_empty_query_id_reports_no_failed_query(tmp_path):
    # A background-exception logical error has an empty query id. Through the full parse_failure
    # capture-and-forward path, the reported info must NOT attribute an unrelated earlier query
    # (the old whole-log fallback would). This exercises the production path, not get_failed_query
    # in isolation.
    server_log = tmp_path / "clickhouse-server.err.log"

    server_log.write_text(
        # An earlier, unrelated query whose comment quotes a generic logical error.
        "2026.07.23 09:00:00.000000 [ 50 ] {q1} <Debug> executeQuery: "
        "(from [::1]:1) (in query: SELECT unrelated_marker -- Logical error 'x')\n"
        # The genuine crash: a background exception with no query (empty query id).
        "2026.07.23 10:00:00.000000 [ 100 ] {} <Fatal> : Logical error: 'background failure'.\n",
        encoding="utf-8",
    )

    parser = FuzzerLogParser(
        server_log=str(server_log),
        stderr_log="",
        fuzzer_log="",
    )

    result_name, info, files = parser.parse_failure()

    # Named after the genuine background failure; the unrelated earlier query is not attributed.
    assert result_name.startswith("Logical error: 'background failure'")
    assert "unrelated_marker" not in info


def test_get_failed_query_inline_ast_scoped_to_selected_record(tmp_path):
    # An earlier query echo embeds a fake full record ("{q2} <Fatal> ... Inconsistent AST
    # formatting: the query:\nFAKE_INLINE") mid-line, then the genuine selected record with the
    # same id follows. The scoped, prefix-anchored inline-AST search must return the genuine
    # inline query, not the fake one an unanchored search would grab first.
    server_log = tmp_path / "clickhouse-server.err.log"

    server_log.write_text(
        # The fake occurrence embeds a COMPLETE formatted prefix (date/time/thread) mid-line, so
        # the test proves the record-start (^) anchor specifically, not just the mid-line position.
        "2026.07.23 09:00:00.000000 [ 50 ] {q2} <Debug> executeQuery: (in query: SELECT x "
        "-- 2026.07.23 08:00:00.000000 [ 9 ] {q2} <Fatal> : "
        "Logical error: Inconsistent AST formatting: the query:\n"
        "FAKE_INLINE\n"
        ") (stage: Complete)\n"
        "2026.07.23 10:00:00.000000 [ 100 ] {q2} <Fatal> : Logical error: "
        "'Inconsistent AST formatting: the query:\n"
        "GENUINE_INLINE\n"
        "cannot parse'.\n",
        encoding="utf-8",
    )

    parser = FuzzerLogParser(
        server_log=str(server_log),
        stderr_log="",
        fuzzer_log="",
    )

    assert parser.get_failed_query(query_id="q2") == "GENUINE_INLINE"


def test_get_failed_query_treats_query_id_as_literal(tmp_path):
    # Query ids are arbitrary user-controlled strings. They must be matched literally, not as
    # shell or regex text: a metacharacter id must select its OWN executeQuery record, and a
    # prefix id (`q2`) must not match a longer id (`q20`). Without escaping / exact `{...}`
    # matching this would run wrong or attribute the wrong query.
    server_log = tmp_path / "clickhouse-server.err.log"

    # Two records whose ids are prefix-related and contain a regex metacharacter.
    server_log.write_text(
        "2026.07.23 09:00:00.000000 [ 50 ] {q2.a} <Debug> executeQuery: "
        "(from [::1]:1) (in query: SELECT short_id_query)\n"
        "2026.07.23 09:00:01.000000 [ 51 ] {q2.a0} <Debug> executeQuery: "
        "(from [::1]:2) (in query: SELECT long_id_query)\n",
        encoding="utf-8",
    )

    parser = FuzzerLogParser(
        server_log=str(server_log),
        stderr_log="",
        fuzzer_log="",
    )

    # The metacharacter id `q2.a` must select its own record, not the `q2.a0` one: a raw
    # `{q2.a} <...> executeQuery:` regex treats `.` as "any char" and the exact `{...}` delimiter
    # is what stops `{q2.a}` from also matching `{q2.a0}` - without escaping + exact matching,
    # both records match and `tail -n1` returns the later, WRONG one (long_id_query).
    result = parser.get_failed_query(query_id="q2.a")
    assert result is not None
    assert "short_id_query" in result
    assert "long_id_query" not in result


def test_parse_failure_preserves_spaced_query_id(tmp_path):
    # Query ids are arbitrary strings and may carry surrounding whitespace (e.g. `{ q }`). The
    # id captured from the fatal record must be preserved byte-for-byte: if it is stripped to
    # `q`, the exact `{...}` executeQuery lookup no longer matches the real `{ q }` record and
    # the crashing query is dropped from the reported info.
    server_log = tmp_path / "clickhouse-server.err.log"

    server_log.write_text(
        # An earlier, unrelated query whose comment quotes a logical error. The legacy whole-log
        # fallback would extract THIS record's query, so the test only passes if the SELECTED
        # fatal record's spaced id is captured (byte-for-byte) and used for the lookup.
        "2026.07.23 08:00:00.000000 [ 40 ] {q1} <Debug> executeQuery: "
        "(from [::1]:1) (in query: SELECT unrelated_marker -- Logical error 'x')\n"
        "2026.07.23 09:00:00.000000 [ 50 ] { q } <Debug> executeQuery: "
        "(from [::1]:2) (in query: SELECT spaced_id_query)\n"
        "2026.07.23 10:00:00.000000 [ 100 ] { q } <Fatal> : Logical error: "
        "'background failure'.\n",
        encoding="utf-8",
    )

    parser = FuzzerLogParser(
        server_log=str(server_log),
        stderr_log="",
        fuzzer_log="",
    )

    result_name, info, files = parser.parse_failure()

    assert result_name.startswith("Logical error: 'background failure'")
    # The crashing query is attributed via the record's spaced id, not lost to a strip; and it is
    # the selected record's query, not the earlier unrelated one the whole-log fallback would grab.
    assert "spaced_id_query" in info
    assert "unrelated_marker" not in info


def test_parse_failure_logical_error_format_string_bounded_by_fatal_record(tmp_path):
    # The `Format string:` search is bounded by the line the scan matched, so it must be
    # bounded by the genuine <Fatal> record and not by an earlier query-echo comment on the
    # same thread: the comment's own "next fatal message" is the logical-error line itself,
    # which carries no format string, and the name would keep its raw quotes.
    server_log = tmp_path / "clickhouse-server.err.log"

    server_log.write_text(
        "2026.07.23 16:42:06.439000 [ 100 ] {q1} <Debug> executeQuery: "
        "(from [::1]:1) (in query: SELECT 1 AS m -- Logical error 'echoed marker')\n"
        "2026.07.23 16:42:07.000000 [ 100 ] {q1} <Fatal> : Logical error: "
        "'Cannot parse element A: expected B'.\n"
        "2026.07.23 16:42:07.100000 [ 100 ] {q1} <Fatal> : Format string: "
        "'Cannot parse element {}: expected {}'.\n",
        encoding="utf-8",
    )

    parser = FuzzerLogParser(
        server_log=str(server_log),
        stderr_log="",
        fuzzer_log="",
    )

    result_name, _, _ = parser.parse_failure()

    assert result_name.startswith(
        "Logical error: Cannot parse element A: expected B (STID:"
    )
    assert "echoed marker" not in result_name
