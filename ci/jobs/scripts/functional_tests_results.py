import dataclasses
import re
import runpy
import signal
import traceback
from pathlib import Path
from typing import List, Optional

from praktika.result import Result

OK_SIGN = "[ OK "
FAIL_SIGN = "[ FAIL "
UNKNOWN_SIGN = "[ UNKNOWN "
SKIPPED_SIGN = "[ SKIPPED "
NOT_FAILED_SIGN = "[ NOT_FAILED "
HUNG_SIGN = "Found hung queries in processlist"
DATABASE_SIGN = "Database: "

# Pick up `STOP_TESTING_EXIT_CODE` straight from `tests/clickhouse-test` so
# the contract has a single source of truth.
_clickhouse_test = Path(__file__).resolve().parents[3] / "tests" / "clickhouse-test"
STOP_TESTING_EXIT_CODE = runpy.run_path(str(_clickhouse_test))["STOP_TESTING_EXIT_CODE"]

# Exit codes that mean the run was aborted mid-flight, so per-test results
# (if any) are incomplete and we cannot trust which test "caused" the
# failure. `STOP_TESTING_EXIT_CODE` is the in-band signal — the parent
# raised `StopTesting` and reached the outer handler. The kill-by-signal
# variants cover the out-of-band cases where the parent was killed before
# it could exit through that handler (currently reachable via the
# worker -> parent SIGTERM feedback loop in `stop_tests`: each worker the
# parent terminates re-broadcasts SIGTERM to the whole process group via
# `killpg`, hitting the parent before it can `sys.exit(STOP_TESTING_EXIT_CODE)`;
# also covers external kills like job-level timeouts and runner shutdown).
#
# Both `128 + N` (bash's convention when its child died from signal N) and
# the negative form `-N` are included: `Shell.run` wraps the command in
# `bash -c`, so most kills surface as `128 + N` via bash's exit status, but
# `Shell._check_timeout` calls `os.killpg` on the whole group, so the
# wrapper bash can itself die from the signal — and Python's
# `subprocess.Popen.returncode` reports that as `-N`, not `128 + N`.
#
# Exit code 1 is deliberately NOT in this set: it is set by end-of-run
# checks (final hung-check, `runner_process_killed`, `total_tests_run == 0`)
# that run AFTER all tests have finished. Per-test results in that case are
# complete and authoritative and must not be demoted.
ABORTED_RUN_EXIT_CODES = frozenset(
    {
        STOP_TESTING_EXIT_CODE,
        128 + signal.SIGTERM,  # 143
        128 + signal.SIGKILL,  # 137
        -signal.SIGTERM,  # -15
        -signal.SIGKILL,  # -9
    }
)

SUCCESS_FINISH_SIGNS = ["All tests have finished", "No tests were run"]

RETRIES_SIGN = "Some tests were restarted"

# Regex pattern to match test result lines.
# The shape `name: [ STATUS ] N.NN sec.` is specific enough that we don't pin
# the leading timestamp - the bounded `^.{0,32}?` lets through any expected
# framing (raw=0, `ts`=20, `[YYYY-MM-DD HH:MM:SS] `=22) but rules out matches
# embedded deeper in an error/exception message (see PR #88825). Test names
# can contain letters, digits, underscores, hyphens, and dots.
TEST_RESULT_PATTERN = re.compile(
    r"^.{0,32}?"
    r"([\w\-\.]+):\s+(\[ (?:OK|FAIL|SKIPPED|UNKNOWN|NOT_FAILED) \])\s+([\d.]+) sec\."
)

# Markers that indicate a real server crash. If any of these appears in
# `clickhouse-server.err.log`, the run must NOT be reclassified as a
# CIDB-staging-cluster overload, even if the rest of the log looks like
# shipping failures.
_REAL_CRASH_PATTERN = re.compile(
    r"<Fatal>|AddressSanitizer:|MemorySanitizer:|ThreadSanitizer:|UndefinedBehaviorSanitizer:|LOGICAL_ERROR"
)

# Logger names / table names that identify CIDB log-export shipping errors.
# When the CIDB staging log cluster is overloaded or unresponsive, the
# server keeps retrying to ship rows from `system.<table>_sender`
# `Distributed` tables; these retries are logged as `<Error>` lines with
# the logger names / source tables below. See `ci/jobs/scripts/functional_tests/setup_log_cluster.sh`.
_STAGING_SHIPPING_LOGGER_PATTERN = re.compile(
    r"DistributedAsyncInsertQueue|DistributedAsyncInsertBatch|DistributedSink|"
    r"BgDistSchPool|StorageDistributed|system\.\w+_sender"
)

# Minimum number of `<Error>` lines that must point at the CIDB staging
# cluster before we treat the run as "infrastructure-only". A handful of
# transient shipping errors should not silence a real failure; the
# pathological cases @alexey-milovidov flagged on PR #106154 had ~1900
# such errors in a single job, so a threshold in the low hundreds is
# both safely above noise and well below real overload signatures.
_STAGING_OVERLOAD_MIN_ERRORS = 100

# Fraction of `<Error>` lines that must be CIDB-shipping retries for the
# log to be classified as "staging-cluster-only". Conservative (95%) so
# any non-shipping `<Error>` line — almost always a more interesting
# failure — keeps us on the `Server died` path.
_STAGING_OVERLOAD_MIN_FRACTION = 0.95


def is_ci_logs_cluster_overload(server_err_log: Path) -> bool:
    """Return ``True`` iff `clickhouse-server.err.log` looks like the CIDB
    staging log cluster was unresponsive during the run and the server
    itself was healthy.

    The classifier requires:

    * no real-crash markers (`<Fatal>`, sanitizer report, `LOGICAL_ERROR`)
      anywhere in the file;
    * at least `_STAGING_OVERLOAD_MIN_ERRORS` `<Error>` lines in the file;
    * at least `_STAGING_OVERLOAD_MIN_FRACTION` of those `<Error>` lines
      naming a Distributed-shipping logger (`DistributedAsyncInsertQueue`,
      `BgDistSchPool`, `system.<table>_sender`, ...).

    The file is streamed line by line because under chronic staging
    overload it can grow to hundreds of MiB; any fixed byte cap on the
    scan window could hide a real-crash marker appended after a large
    initial block of shipping noise.

    This is the heuristic @alexey-milovidov asked for on PR #106154: when
    the only thing wrong is a flaky CIDB log staging cluster, the harness
    wall-clock timeout that kills `clickhouse-test` should not paint the
    job red. A green run with a `SKIPPED` informational leaf is more
    honest than a synthetic `Server died` `FAIL`.
    """
    if not server_err_log.exists():
        return False

    error_lines = 0
    shipping_lines = 0
    try:
        with server_err_log.open("r", encoding="utf-8", errors="replace") as f:
            for line in f:
                # A real-crash marker anywhere in the file disqualifies
                # the run, so scan the full stream — never bail early on
                # the dominance counts before the whole file is read.
                if _REAL_CRASH_PATTERN.search(line):
                    return False
                if "<Error>" not in line:
                    continue
                error_lines += 1
                if _STAGING_SHIPPING_LOGGER_PATTERN.search(line):
                    shipping_lines += 1
    except OSError:
        return False

    if error_lines < _STAGING_OVERLOAD_MIN_ERRORS:
        return False
    return shipping_lines >= _STAGING_OVERLOAD_MIN_FRACTION * error_lines


class FTResultsProcessor:
    @dataclasses.dataclass
    class Summary:
        total: int
        skipped: int
        unknown: int
        failed: int
        success: int
        test_results: List[Result]
        hung: bool = False
        retries: bool = False
        success_finish: bool = False
        test_end: bool = True

    def __init__(self, wd, server_err_log_path: Optional[str] = None):
        self.tests_output_file = f"{wd}/test_result.txt"
        # Path to the server's `err.log`. Used by the CIDB-staging-cluster
        # overload classifier to distinguish a real `Server died` from a
        # harness-killed runner that timed out shipping logs to a flaky
        # staging cluster. Falls back to the conventional location used by
        # `ci/jobs/scripts/clickhouse_proc.py` so callers that don't pass
        # the path explicitly still get the heuristic.
        self.server_err_log_path = Path(
            server_err_log_path
            or f"{wd}/var/log/clickhouse-server/clickhouse-server.err.log"
        )
        self.debug_files = []

    def _process_test_output(self):
        total = 0
        skipped = 0
        unknown = 0
        failed = 0
        success = 0
        hung = False
        retries = False
        success_finish = False
        test_results = []
        test_end = True

        with open(self.tests_output_file, "r", encoding="utf-8") as test_file:
            for line in test_file:
                original_line = line
                line = line.strip()

                if any(s in line for s in SUCCESS_FINISH_SIGNS):
                    success_finish = True
                # Ignore hung check report, since it may be quite large.
                # (and may break python parser which has limit of 128KiB for each row).
                if HUNG_SIGN in line:
                    hung = True
                    break
                if RETRIES_SIGN in line:
                    retries = True

                # Use regex to match test result lines precisely
                # This prevents false matches from error messages containing test patterns
                match = TEST_RESULT_PATTERN.match(line)
                if match:
                    test_name = match.group(1)
                    status_marker = match.group(2)
                    test_time = match.group(3)

                    if test_name == "+":
                        # TODO: investigate and remove
                        # https://github.com/ClickHouse/ClickHouse/issues/81888
                        print(
                            f"ERROR: incorrect test name: {test_name} in line:\n{line}"
                        )
                        continue

                    total += 1
                    if FAIL_SIGN in status_marker:
                        failed += 1
                        test_results.append((test_name, "FAIL", test_time, []))
                    elif UNKNOWN_SIGN in status_marker:
                        unknown += 1
                        test_results.append((test_name, "FAIL", test_time, []))
                    elif NOT_FAILED_SIGN in status_marker:
                        # Test was on a blacklist (expected to fail) but passed -
                        # the blacklist needs updating. Surface as a failure.
                        failed += 1
                        test_results.append((test_name, "NOT_FAILED", test_time, []))
                    elif SKIPPED_SIGN in status_marker:
                        skipped += 1
                        test_results.append((test_name, "SKIPPED", test_time, []))
                    else:
                        success += int(OK_SIGN in status_marker)
                        test_results.append((test_name, "OK", test_time, []))
                    test_end = False
                elif (
                    len(test_results) > 0
                    and test_results[-1][1] in ("FAIL", "SKIPPED", "NOT_FAILED")
                    and not test_end
                ):
                    test_results[-1][3].append(original_line)
                # Database printed after everything else in case of failures,
                # so this is a stop marker for capturing test output.
                #
                # And it is handled after everything else to include line with database into the report.
                if DATABASE_SIGN in line:
                    test_end = True

        test_results_ = []
        for test in test_results:
            try:
                test_results_.append(
                    Result(
                        name=test[0],
                        status=test[1],
                        start_time=None,
                        duration=float(test[2]),
                        info="".join(test[3])[:16384],
                    )
                )
            except Exception as e:
                print(f"ERROR: Failed to parse test results: {test}")
                traceback.print_exc()
                self.debug_files.append(self.tests_output_file)
                if test[0] == "+":
                    # TODO: investigate and remove
                    # https://github.com/ClickHouse/ClickHouse/issues/81888
                    continue
                test_results_.append(
                    Result(
                        name=test[0],
                        status=Result.Status.ERROR,
                        start_time=None,
                        duration=None,
                        info=f"test results parse failure:\n{traceback.print_exc()}",
                    )
                )
        test_results = test_results_

        s = self.Summary(
            total=total,
            skipped=skipped,
            unknown=unknown,
            failed=failed,
            success=success,
            test_results=test_results,
            hung=hung,
            success_finish=success_finish,
            retries=retries,
        )

        return s

    def run(self, task_name="Tests", runner_exit_code: Optional[int] = None):
        state = Result.Status.OK
        s = self._process_test_output()
        test_results = s.test_results

        if s.failed != 0 or s.unknown != 0:
            state = Result.Status.FAIL

        info = ""
        # Set when the run was killed by the harness but the only thing
        # wrong was the CIDB staging log cluster being unresponsive. In
        # that case we surface an informational `SKIPPED` leaf instead of
        # a synthetic `Server died` `FAIL`, and we must also bypass the
        # non-zero-exit-code `FAIL` guard further down.
        ci_logs_cluster_overload = False
        if s.hung:
            state = Result.Status.FAIL
            test_results.append(
                Result("Some queries hung", Result.Status.FAIL, info="Some queries hung")
            )
        elif runner_exit_code in ABORTED_RUN_EXIT_CODES:
            failed_results = [r for r in test_results if r.is_failure()]
            # @alexey-milovidov directive on PR #106154: when the only
            # evidence in `clickhouse-server.err.log` is repeated
            # `Distributed`-shipping retries to the CIDB staging cluster
            # (`DistributedAsyncInsertQueue` / `BgDistSchPool` errors with
            # `TOO_MANY_PARTS` / `SOCKET_TIMEOUT` / `NETWORK_ERROR`), the
            # ClickHouse server itself is healthy. The runner only got
            # killed by the harness wall-clock timeout because flushing
            # logs to the unresponsive staging cluster piled up. Treat
            # this as an infrastructure outage, not a server crash, and
            # don't paint the check red.
            #
            # `s.success_finish` is required so an incomplete run is
            # never reclassified: if the wall-clock fires mid-suite, no
            # test may have emitted `FAIL` yet, but not all selected
            # tests ran either — the result must stay `Server died`.
            if (
                s.success_finish
                and not failed_results
                and is_ci_logs_cluster_overload(self.server_err_log_path)
            ):
                ci_logs_cluster_overload = True
                test_results.append(
                    Result(
                        "CIDB log cluster unresponsive",
                        Result.Status.SKIPPED,
                        info=(
                            "Test runner was killed by the wall-clock timeout while the "
                            "ClickHouse server was healthy; `clickhouse-server.err.log` is "
                            "dominated by `Distributed`-shipping retries to the CIDB staging "
                            "log cluster. Not failing CI for an external-infrastructure outage. "
                            "See ClickHouse/ClickHouse#106154."
                        ),
                    )
                )
            else:
                state = Result.Status.FAIL
                if len(failed_results) > 1:
                    # Multiple tests failed when the server died - this is a parallel
                    # run where we can't tell which test (if any) caused the crash.
                    # Mark them all as UNKNOWN so they don't pollute failure reports.
                    # The actual failure is captured by the "Server died" / LOGICAL_ERROR
                    # entry added from the server log.
                    for result in failed_results:
                        result.status = Result.Status.UNKNOWN
                elif len(failed_results) == 1:
                    # Single test failed - sequential run, this test is the culprit.
                    failed_results[0].status = Result.Status.ERROR
                test_results.append(Result("Server died", Result.Status.FAIL, info="Server died"))
        elif not s.success_finish:
            state = Result.Status.ERROR
            info = "The test runner was terminated unexpectedly"
        elif s.retries:
            test_results.append(
                Result("Some tests restarted", Result.Status.SKIPPED, info="Some tests restarted")
            )
        else:
            pass

        # The runner's exit code is the authoritative signal: if `clickhouse-test`
        # exited non-zero, the job must not report OK even when log parsing finds
        # nothing to blame. The synthetic leaf is added only when the parser
        # found nothing - otherwise the real failure already explains the result
        # and a duplicate entry is just noise.
        #
        # The CIDB-staging-cluster overload heuristic above is the one
        # exception: when the runner was killed by the wall-clock timeout
        # because shipping system logs to the unresponsive staging cluster
        # piled up, the server was healthy and the run must read green.
        if (
            runner_exit_code is not None
            and runner_exit_code != 0
            and not ci_logs_cluster_overload
        ):
            if state == Result.Status.OK:
                state = Result.Status.FAIL
                test_results.append(
                    Result(
                        name="clickhouse-test",
                        status=Result.Status.FAIL,
                        info=f"clickhouse-test exited with code {runner_exit_code}",
                    )
                )

        if not info:
            info = f"Failed: {s.failed}, Passed: {s.success}, Skipped: {s.skipped}"

        result = Result.create_from(
            name=task_name,
            results=test_results,
            status=state,
            files=[],
            info=info,
            with_info_from_results=False,
        )

        if not result.is_ok():
            order = {
                "FAIL": 0,
                "SERVER_DIED": 1,
                "Timeout": 2,
                "NOT_FAILED": 3,
                "BROKEN": 4,
                "UNKNOWN": 5,
                "OK": 6,
                "SKIPPED": 7,
            }
            result.results.sort(key=lambda x: order.get(x.status, -1))

        return result
