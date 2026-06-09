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
