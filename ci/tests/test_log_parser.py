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
