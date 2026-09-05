"""
Tests for ClickHouse unit-test (gtest) result parsing from stdout.

gtest writes its JSON/XML report only once, at end-of-run, so a crash (signal,
OOM, uncaught exception on a worker thread) leaves no machine-readable file. We
therefore parse the always-present stdout markers ([ RUN ]/[ OK ]/[ FAILED ])
as the single source of truth, which covers normal failures and crashes
uniformly:

  * ResultTranslator.from_gtest_stdout  — full run -> per-test Results
  * ResultTranslator.parse_gtest_crash  — (crashed_test, crash_message) recovery

Regression context: a coverage unit-tests shard crashed with an uncaught SSL
exception, but the report showed test name "-FunctionsStress" (the gtest filter
of the shard, character-set-stripped of ".*") with the message "Backup checksum
should not be zero" (a stack trace logged by a *passing* EXPECT_THROW negative
test early in the run). Both were wrong; these tests pin the correct behavior.
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from ci.praktika.result import Result, ResultTranslator

JOB_NAME = "Unit tests (amd_llvm_coverage)"


def _crash(log):
    return ResultTranslator.parse_gtest_crash(log.splitlines(), fallback_name=JOB_NAME)


def _stdout(log):
    return ResultTranslator.from_gtest_stdout(log.splitlines(), fallback_name=JOB_NAME)


def _by_name(results):
    return {r.name: r for r in results}


# The real failure (trimmed): a passing negative test logs a full "Code: ..."
# stack trace early, then the SecurePolicy SSL test aborts the process with an
# uncaught exception far below.
REAL_SSL_CRASH_LOG = """\
[----------] 6 tests from BackupDataFileNameGeneratorTest
[ RUN      ] BackupDataFileNameGeneratorTest.ThrowsOnZeroChecksum
Code: 49. DB::Exception: Backup checksum should not be zero (test.bin). (LOGICAL_ERROR), Stack trace (when copying this message, always include the lines below):

0. Poco::Exception::Exception(String&&, int)
6. src/Backups/tests/gtest_backup_data_file_name_generator.cpp:95: TestBody()
[       OK ] BackupDataFileNameGeneratorTest.ThrowsOnZeroChecksum (181 ms)
[----------] 6 tests from BackupDataFileNameGeneratorTest (181 ms total)
[----------] 4 tests from SilkFiberSocketTest/1, where TypeParam = (anonymous namespace)::SecurePolicy
[ RUN      ] SilkFiberSocketTest/1.RequestResponse
libc++abi: terminating due to uncaught exception of type Poco::Net::SSLException: SSL Exception
unknown file: Failure
C++ exception with description "SSL Exception" thrown in the test body.

[  FAILED  ] SilkFiberSocketTest/1.RequestResponse, where TypeParam = (anonymous namespace)::SecurePolicy (61 ms)
[ RUN      ] SilkFiberSocketTest/1.PollAndReceiveTimeout

Thread 17 "unit_tests_dbms" received signal SIGABRT, Aborted.
"""


# --- parse_gtest_crash ---


def test_real_ssl_crash_names_the_running_test_not_the_filter():
    name, _ = _crash(REAL_SSL_CRASH_LOG)
    # The test that was executing when the process died (the last "[ RUN ]" with
    # no close marker), NOT "-FunctionsStress" and NOT the job name.
    assert name == "SilkFiberSocketTest/1.PollAndReceiveTimeout"
    assert "FunctionsStress" not in name


def test_real_ssl_crash_message_describes_the_crash_not_the_passing_test():
    _, message = _crash(REAL_SSL_CRASH_LOG)
    assert "terminating due to uncaught exception" in message
    assert "SSLException" in message
    assert "received signal SIGABRT" in message
    # The backup stack trace is from a test that PASSED — it must not appear.
    assert "Backup checksum" not in message


def test_passing_negative_test_exception_is_ignored_for_later_crash():
    log = """\
[ RUN      ] NegativeTest.ThrowsAsExpected
Code: 49. DB::Exception: Backup checksum should not be zero (test.bin). (LOGICAL_ERROR)
[       OK ] NegativeTest.ThrowsAsExpected (1 ms)
[ RUN      ] OtherSuite.Crashes
Thread 1 "unit_tests_dbms" received signal SIGSEGV, Segmentation fault.
"""
    name, message = _crash(log)
    assert name == "OtherSuite.Crashes"
    assert "Backup checksum" not in message
    assert "received signal SIGSEGV" in message


def test_logical_error_in_crash_region_is_kept():
    # The function fuzzer logs a repro and a logical error from the crashing
    # test, then aborts. Both should be surfaced.
    log = """\
[ RUN      ] FunctionsStress.Fuzz
(while executing: SELECT lower(materialize('x')) FROM numbers(10);)
Code: 99. DB::Exception: Illegal type. (ILLEGAL_TYPE_OF_ARGUMENT)
unit_tests_dbms received signal SIGABRT, Aborted.
"""
    name, message = _crash(log)
    assert name == "FunctionsStress.Fuzz"
    assert "Illegal type" in message
    assert "SELECT lower(materialize('x'))" in message
    assert "received signal SIGABRT" in message


def test_fallback_name_when_no_test_started():
    # Crash during static initialization, before any "[ RUN ]" line.
    log = """\
Some early startup output
SUMMARY: AddressSanitizer: heap-use-after-free
"""
    name, message = _crash(log)
    assert name == JOB_NAME  # the fallback
    assert "AddressSanitizer" in message


def test_typed_test_close_marker_clears_running_test():
    log = """\
[ RUN      ] Suite/0.A
[       OK ] Suite/0.A, where TypeParam = int (0 ms)
[ RUN      ] Suite/1.B
[  FAILED  ] Suite/1.B, where TypeParam = long (0 ms)
[ RUN      ] Suite/2.C
Thread 1 received signal SIGABRT, Aborted.
"""
    name, _ = _crash(log)
    assert name == "Suite/2.C"


def test_no_markers_returns_empty_message():
    log = """\
[ RUN      ] Suite.A
some unremarkable output
"""
    name, message = _crash(log)
    assert name == "Suite.A"
    assert message == ""


# --- from_gtest_stdout ---


NORMAL_MIXED_LOG = """\
[==========] Running 3 tests from 1 test suite.
[----------] Global test environment set-up.
[----------] 3 tests from MySuite
[ RUN      ] MySuite.Passes
[       OK ] MySuite.Passes (0 ms)
[ RUN      ] MySuite.Fails
/clickhouse/foo.cpp:10: Failure
Expected equality of these values:
  1
  2
[  FAILED  ] MySuite.Fails (5 ms)
[ RUN      ] MySuite.Skipped
[  SKIPPED ] MySuite.Skipped (0 ms)
[----------] 3 tests from MySuite (5 ms total)
[----------] Global test environment tear-down
[==========] 3 tests from 1 test suite ran. (6 ms total)
[  PASSED  ] 1 test.
[  FAILED  ] 1 test, listed below:
[  FAILED  ] MySuite.Fails

 1 FAILED TEST
"""


def test_stdout_normal_mixed_counts_and_statuses():
    status, results, info = _stdout(NORMAL_MIXED_LOG)
    assert status == Result.Status.FAIL
    by = _by_name(results)
    assert by["MySuite.Passes"].status == Result.Status.OK
    assert by["MySuite.Fails"].status == Result.Status.FAIL
    assert by["MySuite.Skipped"].status == Result.Status.SKIPPED
    assert info == "fail: 1, passed: 1, skipped: 1"


def test_stdout_summary_failed_lines_not_double_counted():
    # The trailing "[  FAILED  ] MySuite.Fails" summary line must not create a
    # second failed result.
    _, results, _ = _stdout(NORMAL_MIXED_LOG)
    assert sum(1 for r in results if r.name == "MySuite.Fails") == 1
    assert len(results) == 3


def test_stdout_failed_test_captures_failure_output():
    _, results, _ = _stdout(NORMAL_MIXED_LOG)
    fails = _by_name(results)["MySuite.Fails"]
    assert "foo.cpp:10: Failure" in fails.info
    assert "Expected equality" in fails.info


def test_stdout_durations_parsed_in_seconds():
    _, results, _ = _stdout(NORMAL_MIXED_LOG)
    assert _by_name(results)["MySuite.Fails"].duration == 0.005


def test_stdout_all_pass_is_ok():
    log = """\
[==========] Running 2 tests from 1 test suite.
[ RUN      ] S.A
[       OK ] S.A (0 ms)
[ RUN      ] S.B
[       OK ] S.B (1 ms)
[==========] 2 tests from 1 test suite ran. (1 ms total)
[  PASSED  ] 2 tests.
"""
    status, results, info = _stdout(log)
    assert status == Result.Status.OK
    assert info == "fail: 0, passed: 2"
    assert all(r.status == Result.Status.OK for r in results)
    assert len(results) == 2


def test_stdout_crash_adds_culprit_and_keeps_earlier_results():
    status, results, info = _stdout(REAL_SSL_CRASH_LOG)
    assert status == Result.Status.FAIL
    by = _by_name(results)
    # The passing negative test is still recorded as OK.
    assert by["BackupDataFileNameGeneratorTest.ThrowsOnZeroChecksum"].status == (
        Result.Status.OK
    )
    # The test that gtest itself flagged failed.
    assert by["SilkFiberSocketTest/1.RequestResponse"].status == Result.Status.FAIL
    # The crash culprit (unclosed RUN) is added as a failure.
    assert "SilkFiberSocketTest/1.PollAndReceiveTimeout" in by
    assert by["SilkFiberSocketTest/1.PollAndReceiveTimeout"].status == (
        Result.Status.FAIL
    )
    # Top-level info is the crash message, not the passing test's stack trace.
    assert "received signal SIGABRT" in info
    assert "Backup checksum" not in info


def test_stdout_zero_matching_tests_is_ok_not_a_crash():
    # A filter that matches nothing still prints the end-of-run banner; this is a
    # clean run with zero tests, not a crash.
    log = """\
[==========] Running 0 tests from 0 test suites.
[==========] 0 tests from 0 test suites ran. (0 ms total)
[  PASSED  ] 0 tests.
"""
    status, results, info = _stdout(log)
    assert status == Result.Status.OK
    assert results == []
    assert info == "fail: 0, passed: 0"


def test_stdout_no_output_at_all_is_a_failure():
    # Empty / truncated log with no banner and no markers: the binary never ran.
    status, results, info = _stdout("")
    assert status == Result.Status.FAIL
    assert len(results) == 1
    assert results[0].name == JOB_NAME


def test_stdout_asan_summary_after_unclosed_run():
    # A sanitizer abort: the test is open ("[ RUN ]" with no close), the binary
    # prints an ASan report (no "received signal" line) and dies.
    log = """\
[==========] Running 1 test from 1 test suite.
[ RUN      ] Mem.UseAfterFree
=================================================================
==1234==ERROR: AddressSanitizer: heap-use-after-free on address 0xdeadbeef
SUMMARY: AddressSanitizer: heap-use-after-free foo.cpp:10 in Bar()
"""
    status, results, info = _stdout(log)
    assert status == Result.Status.FAIL
    by = _by_name(results)
    assert by["Mem.UseAfterFree"].status == Result.Status.FAIL
    assert "AddressSanitizer" in info


# --- indented markers in a test's own output must not be mistaken for real ones ---


def test_crash_ignores_indented_run_marker_in_test_output():
    # gtest emits markers at column 0; an indented "[ RUN ]" in a test's output
    # must not overwrite the real running test.
    log = """\
[ RUN      ] RealSuite.Crashes
    [ RUN      ] FakeSuite.NotATest
    [  FAILED  ] FakeSuite.NotATest (0 ms)
Thread 1 received signal SIGABRT, Aborted.
"""
    name, message = _crash(log)
    assert name == "RealSuite.Crashes"
    assert "Fake" not in name
    assert "received signal SIGABRT" in message


def test_crash_ignores_indented_error_prefix():
    # An indented "Code: ..." (structured test output) is not the crash error.
    log = """\
[ RUN      ] Suite.Crashes
    Code: 10. DB::Exception: indented benign message
some output
"""
    name, message = _crash(log)
    assert name == "Suite.Crashes"
    assert "indented benign message" not in message


def test_stdout_ignores_indented_markers_in_passing_test_output():
    log = """\
[==========] Running 1 test from 1 test suite.
[ RUN      ] RealSuite.PrintsMarkers
    [ RUN      ] Fake.Inner
    [  FAILED  ] Fake.Inner (0 ms)
[       OK ] RealSuite.PrintsMarkers (1 ms)
[==========] 1 test from 1 test suite ran. (1 ms total)
[  PASSED  ] 1 test.
"""
    status, results, info = _stdout(log)
    assert status == Result.Status.OK
    assert [r.name for r in results] == ["RealSuite.PrintsMarkers"]
    assert results[0].status == Result.Status.OK
