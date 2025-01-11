import dataclasses
from typing import List

from praktika.environment import Environment
from praktika.result import Result

OK_SIGN = "[ OK "
FAIL_SIGN = "[ FAIL "
TIMEOUT_SIGN = "[ Timeout! "
UNKNOWN_SIGN = "[ UNKNOWN "
SKIPPED_SIGN = "[ SKIPPED "
HUNG_SIGN = "Found hung queries in processlist"
SERVER_DIED_SIGN = "Server died, terminating all processes"
SERVER_DIED_SIGN2 = "Server does not respond to health check"
DATABASE_SIGN = "Database: "

SUCCESS_FINISH_SIGNS = ["All tests have finished", "No tests were run"]

RETRIES_SIGN = "Some tests were restarted"


# def write_results(results_file, status_file, results, status):
#     with open(results_file, "w", encoding="utf-8") as f:
#         out = csv.writer(f, delimiter="\t")
#         out.writerows(results)
#     with open(status_file, "w", encoding="utf-8") as f:
#         out = csv.writer(f, delimiter="\t")
#         out.writerow(status)

BROKEN_TESTS_ANALYZER_TECH_DEBT = [
    "01624_soft_constraints",
    # Check after ConstantNode refactoring
    "02944_variant_as_common_type",
]


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
        server_died: bool = False
        retries: bool = False
        success_finish: bool = False
        test_end: bool = True

    def __init__(self, wd):
        self.tests_output_file = f"{wd}/test_result.txt"
        # self.test_results_parsed_file = f"{wd}/test_result.tsv"
        # self.status_file = f"{wd}/check_status.tsv"
        self.broken_tests = BROKEN_TESTS_ANALYZER_TECH_DEBT

    def _process_test_output(self):
        total = 0
        skipped = 0
        unknown = 0
        failed = 0
        success = 0
        hung = False
        server_died = False
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
                if SERVER_DIED_SIGN in line or SERVER_DIED_SIGN2 in line:
                    server_died = True
                if RETRIES_SIGN in line:
                    retries = True
                if any(
                    sign in line
                    for sign in (OK_SIGN, FAIL_SIGN, UNKNOWN_SIGN, SKIPPED_SIGN)
                ):
                    test_name = line.split(" ")[2].split(":")[0]

                    test_time = ""
                    try:
                        time_token = line.split("]")[1].strip().split()[0]
                        float(time_token)
                        test_time = time_token
                    except:
                        pass

                    total += 1
                    if TIMEOUT_SIGN in line:
                        if test_name in self.broken_tests:
                            success += 1
                            test_results.append((test_name, "BROKEN", test_time, []))
                        else:
                            failed += 1
                            test_results.append((test_name, "Timeout", test_time, []))
                    elif FAIL_SIGN in line:
                        if test_name in self.broken_tests:
                            success += 1
                            test_results.append((test_name, "BROKEN", test_time, []))
                        else:
                            failed += 1
                            test_results.append((test_name, "FAIL", test_time, []))
                    elif UNKNOWN_SIGN in line:
                        unknown += 1
                        test_results.append((test_name, "FAIL", test_time, []))
                    elif SKIPPED_SIGN in line:
                        skipped += 1
                        test_results.append((test_name, "SKIPPED", test_time, []))
                    else:
                        if OK_SIGN in line and test_name in self.broken_tests:
                            skipped += 1
                            test_results.append(
                                (
                                    test_name,
                                    "NOT_FAILED",
                                    test_time,
                                    [
                                        "This test passed. Update analyzer_tech_debt.txt.\n"
                                    ],
                                )
                            )
                        else:
                            success += int(OK_SIGN in line)
                            test_results.append((test_name, "OK", test_time, []))
                    test_end = False
                elif (
                    len(test_results) > 0
                    and test_results[-1][1] == "FAIL"
                    and not test_end
                ):
                    test_results[-1][3].append(original_line)
                # Database printed after everything else in case of failures,
                # so this is a stop marker for capturing test output.
                #
                # And it is handled after everything else to include line with database into the report.
                if DATABASE_SIGN in line:
                    test_end = True

        test_results = [
            Result(
                name=test[0],
                status=test[1],
                start_time=None,
                duration=float(test[2]),
                info="".join(test[3])[:8192],
            )
            for test in test_results
        ]

        s = self.Summary(
            total=total,
            skipped=skipped,
            unknown=unknown,
            failed=failed,
            success=success,
            test_results=test_results,
            hung=hung,
            server_died=server_died,
            success_finish=success_finish,
            retries=retries,
        )

        return s

    def run(self):
        state = Result.Status.SUCCESS
        s = self._process_test_output()
        test_results = s.test_results

        # # Check test_results.tsv for sanitizer asserts, crashes and other critical errors.
        # # If the file is present, it's expected to be generated by stress_test.lib check for critical errors
        # # In the end this file will be fully regenerated, including both results from critical errors check and
        # # functional test results.
        # if test_results_path and os.path.exists(test_results_path):
        #     with open(test_results_path, "r", encoding="utf-8") as test_results_file:
        #         existing_test_results = list(
        #             csv.reader(test_results_file, delimiter="\t")
        #         )
        #         for test in existing_test_results:
        #             if len(test) < 2:
        #                 unknown += 1
        #             else:
        #                 test_results.append(test)
        #
        #                 if test[1] != "OK":
        #                     failed += 1
        #                 else:
        #                     success += 1

        # is_flaky_check = 1 < int(os.environ.get("NUM_TRIES", 1))
        # logging.info("Is flaky check: %s", is_flaky_check)
        # # If no tests were run (success == 0) it indicates an error (e.g. server did not start or crashed immediately)
        # # But it's Ok for "flaky checks" - they can contain just one test for check which is marked as skipped.
        # if failed != 0 or unknown != 0 or (success == 0 and (not is_flaky_check)):
        if s.failed != 0 or s.unknown != 0:
            state = Result.Status.FAILED

        if s.hung:
            state = Result.Status.FAILED
            test_results.append(
                Result("Some queries hung", "FAIL", info="Some queries hung")
            )
        elif s.server_died:
            state = Result.Status.FAILED
            # When ClickHouse server crashes, some tests are still running
            # and fail because they cannot connect to server
            for result in test_results:
                if result.status == "FAIL":
                    result.status = "SERVER_DIED"
            test_results.append(Result("Server died", "FAIL", info="Server died"))
        elif not s.success_finish:
            state = Result.Status.FAILED
            test_results.append(
                Result("Tests are not finished", "FAIL", info="Tests are not finished")
            )
        elif s.retries:
            test_results.append(
                Result("Some tests restarted", "SKIPPED", info="Some tests restarted")
            )
        else:
            pass

        # TODO: !!!
        # def test_result_comparator(item):
        #     # sort by status then by check name
        #     order = {
        #         "FAIL": 0,
        #         "SERVER_DIED": 1,
        #         "Timeout": 2,
        #         "NOT_FAILED": 3,
        #         "BROKEN": 4,
        #         "OK": 5,
        #         "SKIPPED": 6,
        #     }
        #     return order.get(item[1], 10), str(item[0]), item[1]
        #
        # test_results.sort(key=test_result_comparator)

        return Result.create_from(
            name=Environment.JOB_NAME,
            results=test_results,
            status=state,
            files=[self.tests_output_file],
            with_info_from_results=False,
        )


# if __name__ == "__main__":
#     logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
#     parser = argparse.ArgumentParser(
#         description="ClickHouse script for parsing results of functional tests"
#     )
#
#     parser.add_argument("--out-results-file", default="/test_output/test_results.tsv")
#     parser.add_argument("--out-status-file", default="/test_output/check_status.tsv")
#     args = parser.parse_args()
#
#     broken_tests = []
#     state, description, test_results = process_result(
#         args.in_results_dir,
#         broken_tests,
#         args.in_test_result_file,
#         args.in_results_file,
#     )
#     logging.info("Result parsed")
#     status = (state, description)
#
#
#
#     write_results(args.out_results_file, args.out_status_file, test_results, status)
#     logging.info("Result written")
