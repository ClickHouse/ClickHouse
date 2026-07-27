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


def _error_section(info):
    # `info` is "Error:\n<error_output>\n" followed by optional "---\n\n<section>"
    # blocks (failed query, reproduce commands, stack trace). Assertions about the
    # extracted error message must look only at the Error section: the same text
    # can also appear in the Stack trace section, which would make an assertion
    # over the whole `info` pass even when the message itself was dropped.
    assert info.startswith("Error:\n"), info
    return info[len("Error:\n") :].split("\n---\n")[0]


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


def test_parse_failure_keeps_memory_limit_message(tmp_path):
    server_log = tmp_path / "clickhouse-server.err.log"

    server_log.write_text(
        "2026.07.23 12:49:49.312341 [ 3574 ] "
        "{0166dacb-a214-46d1-ab34-6c0cc36ed52d::201403_1_29_1} <Error> "
        "virtual bool DB::MergePlainMergeTreeTask::executeStep(): "
        "Exception is in merge_task.: Code: 241. DB::Exception: "
        "(total) memory limit exceeded: would use 43.50 GiB "
        "(attempt to allocate chunk of 0.00 B), current RSS: 47.10 GiB, "
        "maximum: 47.07 GiB. Untracked memory across all threads: 1.40 MiB. "
        "(MEMORY_LIMIT_EXCEEDED), Stack trace (when copying this message, "
        "always include the lines below):\n"
        "\n"
        "0. src/Common/Exception.cpp:160:1: DB::Exception::Exception("
        "DB::Exception::MessageMasked&&, int, bool) @ 0x00000000136ea890\n"
        "4. src/Common/MemoryTracker.cpp:462:23: MemoryTracker::allocImpl("
        "long, bool, MemoryTracker*, double) @ 0x000000001376f418\n"
        "8. src/Common/CurrentMemoryTracker.cpp:118:39: "
        "DB::IMergeTreeReader::IMergeTreeReader() @ 0x0000000018ca017c\n"
        "2026.07.23 12:49:49.313365 [ 3574 ] {} <Debug> MemoryTracker: "
        "Peak memory usage: 1.00 GiB.\n"
        "2026.07.23 12:49:49.313400 [ 3574 ] {} <Debug> executeQuery: "
        "Read 1 rows.\n",
        encoding="utf-8",
    )

    parser = FuzzerLogParser(
        server_log=str(server_log),
        stderr_log="",
        fuzzer_log="",
    )

    result_name, info, files = parser.parse_failure()

    error = _error_section(info)

    assert result_name == "Server unresponsive: memory limit exceeded"
    # The message and the allocation site are the only diagnostics this failure
    # has; an empty Error section makes the leaf useless.
    assert "(total) memory limit exceeded" in error
    assert "current RSS: 47.10 GiB" in error
    assert "MemoryTracker::allocImpl" in error
    assert "IMergeTreeReader" in error
    # The capture stops at the next log record - it must not swallow unrelated
    # records that follow.
    assert "Peak memory usage" not in error
    assert "Read 1 rows" not in error


def test_parse_failure_keeps_libcpp_assert_message(tmp_path):
    server_log = tmp_path / "clickhouse-server.err.log"

    server_log.write_text(
        "2026.07.23 12:49:49.312341 [ 3574 ] {q1} <Fatal> BaseDaemon: "
        "_LIBCPP_ASSERT(!empty()) failed: front() called on an empty vector\n"
        "2026.07.23 12:49:49.313365 [ 3574 ] {} <Debug> Application: "
        "Shutting down.\n",
        encoding="utf-8",
    )

    parser = FuzzerLogParser(
        server_log=str(server_log),
        stderr_log="",
        fuzzer_log="",
    )

    result_name, info, files = parser.parse_failure()

    error = _error_section(info)

    assert "_LIBCPP_ASSERT(!empty()) failed" in error
    assert "front() called on an empty vector" in error
    assert "Shutting down" not in error


def test_parse_failure_keeps_watchdog_signal_message(tmp_path):
    server_log = tmp_path / "clickhouse-server.err.log"

    server_log.write_text(
        "2026.07.23 12:49:49.312341 [ 4177 ] {} <Fatal> Application: "
        "Child process was terminated by signal 9 (KILL).\n"
        "2026.07.23 12:49:49.313365 [ 4177 ] {} <Debug> Application: "
        "Forking to watchdog.\n",
        encoding="utf-8",
    )

    parser = FuzzerLogParser(
        server_log=str(server_log),
        stderr_log="",
        fuzzer_log="",
    )

    result_name, info, files = parser.parse_failure()

    error = _error_section(info)

    assert "Child process was terminated by signal 9 (KILL)" in error
    assert "Forking to watchdog" not in error


def test_parse_failure_keeps_sanitizer_check_failed_from_server_log(tmp_path):
    # stress_runner.sh may leave no stderr.log, in which case the caller passes
    # the server log as both logs (see clickhouse_proc.py). The sanitizer report
    # then carries a ClickHouse log-record prefix.
    server_log = tmp_path / "clickhouse-server.err.log"

    server_log.write_text(
        "2026.07.23 12:49:49.312341 [ 3574 ] {} <Fatal> BaseDaemon: "
        "AddressSanitizer: CHECK failed: sanitizer_allocator_secondary.h:200 "
        '"((nearest_chunk)) < ((h->map_beg + h->map_size))" '
        "(0x7b44c2461000, 0x0) (tid=3005)\n"
        "\n"
        "2026.07.23 12:49:49.313365 [ 3574 ] {} <Debug> Application: "
        "Shutting down.\n",
        encoding="utf-8",
    )

    parser = FuzzerLogParser(
        server_log=str(server_log),
        stderr_log=str(server_log),
        fuzzer_log="",
    )

    result_name, info, files = parser.parse_failure()

    error = _error_section(info)

    assert "AddressSanitizer: CHECK failed:" in error
    assert "sanitizer_allocator_secondary.h:200" in error
    assert "Shutting down" not in error


def test_parse_failure_keeps_runtime_error_message_from_server_log(tmp_path):
    # RUNTIME_ERROR_PATTERN's `.*is located.*` alternative also matches a line that
    # carries a ClickHouse log-record prefix.
    server_log = tmp_path / "clickhouse-server.err.log"

    server_log.write_text(
        "2026.07.23 12:49:49.312341 [ 3574 ] {} <Fatal> BaseDaemon: "
        "0x7b44c2461000 is located 0 bytes inside of 8-byte region\n"
        "2026.07.23 12:49:49.313365 [ 3574 ] {} <Debug> Application: "
        "Shutting down.\n",
        encoding="utf-8",
    )

    parser = FuzzerLogParser(
        server_log=str(server_log),
        stderr_log=str(server_log),
        fuzzer_log="",
    )

    result_name, info, files = parser.parse_failure()

    error = _error_section(info)

    assert "is located 0 bytes inside of 8-byte region" in error
    assert "Shutting down" not in error


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
