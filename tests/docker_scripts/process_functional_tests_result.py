#!/usr/bin/env python3

import argparse
import csv
import logging
import os

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


def process_test_log(log_path, broken_tests):
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
    with open(log_path, "r", encoding="utf-8") as test_file:
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
                    if test_name in broken_tests:
                        success += 1
                        test_results.append((test_name, "BROKEN", test_time, []))
                    else:
                        failed += 1
                        test_results.append((test_name, "Timeout", test_time, []))
                elif FAIL_SIGN in line:
                    if test_name in broken_tests:
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
                    if OK_SIGN in line and test_name in broken_tests:
                        skipped += 1
                        test_results.append(
                            (
                                test_name,
                                "NOT_FAILED",
                                test_time,
                                ["This test passed. Update analyzer_tech_debt.txt.\n"],
                            )
                        )
                    else:
                        success += int(OK_SIGN in line)
                        test_results.append((test_name, "OK", test_time, []))
                test_end = False
            elif (
                len(test_results) > 0 and test_results[-1][1] == "FAIL" and not test_end
            ):
                test_results[-1][3].append(original_line)
            # Database printed after everything else in case of failures,
            # so this is a stop marker for capturing test output.
            #
            # And it is handled after everything else to include line with database into the report.
            if DATABASE_SIGN in line:
                test_end = True

    # Python does not support TSV, so we have to escape '\t' and '\n' manually
    # and hope that complex escape sequences will not break anything
    test_results = [
        [
            test[0],
            test[1],
            test[2],
            "".join(test[3])[:8192].replace("\t", "\\t").replace("\n", "\\n"),
        ]
        for test in test_results
    ]

    return (
        total,
        skipped,
        unknown,
        failed,
        success,
        hung,
        server_died,
        success_finish,
        retries,
        test_results,
    )


def process_result(result_path, broken_tests, in_test_result_file, in_results_file):
    test_results = []
    state = "success"
    description = ""
    files = os.listdir(result_path)
    test_results_path = result_path
    if files:
        logging.info("Find files in result folder %s", ",".join(files))
        test_results_path = os.path.join(result_path, in_results_file)
        result_path = os.path.join(result_path, in_test_result_file)
    else:
        test_results_path = None
        result_path = None
        description = "No output log"
        state = "error"

    if result_path and os.path.exists(result_path):
        (
            _total,
            skipped,
            unknown,
            failed,
            success,
            hung,
            server_died,
            success_finish,
            retries,
            test_results,
        ) = process_test_log(result_path, broken_tests)

        # Check test_results.tsv for sanitizer asserts, crashes and other critical errors.
        # If the file is present, it's expected to be generated by stress_test.lib check for critical errors
        # In the end this file will be fully regenerated, including both results from critical errors check and
        # functional test results.
        if test_results_path and os.path.exists(test_results_path):
            with open(test_results_path, "r", encoding="utf-8") as test_results_file:
                existing_test_results = list(
                    csv.reader(test_results_file, delimiter="\t")
                )
                for test in existing_test_results:
                    if len(test) < 2:
                        unknown += 1
                    else:
                        test_results.append(test)

                        if test[1] != "OK":
                            failed += 1
                        else:
                            success += 1

        is_flaky_check = 1 < int(os.environ.get("NUM_TRIES", 1))
        logging.info("Is flaky check: %s", is_flaky_check)
        # If no tests were run (success == 0) it indicates an error (e.g. server did not start or crashed immediately)
        # But it's Ok for "flaky checks" - they can contain just one test for check which is marked as skipped.
        if failed != 0 or unknown != 0 or (success == 0 and (not is_flaky_check)):
            state = "failure"

        if hung:
            description = "Some queries hung, "
            state = "failure"
            test_results.append(["Some queries hung", "FAIL", "0", ""])
        elif server_died:
            description = "Server died, "
            state = "failure"
            # When ClickHouse server crashes, some tests are still running
            # and fail because they cannot connect to server
            for result in test_results:
                if result[1] == "FAIL":
                    result[1] = "SERVER_DIED"
            test_results.append(["Server died", "FAIL", "0", ""])
        elif not success_finish:
            description = "Tests are not finished, "
            state = "failure"
            test_results.append(["Tests are not finished", "FAIL", "0", ""])
        elif retries:
            description = "Some tests restarted, "
            test_results.append(["Some tests restarted", "SKIPPED", "0", ""])
        else:
            description = ""

        description += f"fail: {failed}, passed: {success}"
        if skipped != 0:
            description += f", skipped: {skipped}"
        if unknown != 0:
            description += f", unknown: {unknown}"
    else:
        state = "failure"
        description = "Output log doesn't exist"
        test_results = []

    return state, description, test_results


def write_results(results_file, status_file, results, status):
    with open(results_file, "w", encoding="utf-8") as f:
        out = csv.writer(f, delimiter="\t")
        out.writerows(results)
    with open(status_file, "w", encoding="utf-8") as f:
        out = csv.writer(f, delimiter="\t")
        out.writerow(status)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
    parser = argparse.ArgumentParser(
        description="ClickHouse script for parsing results of functional tests"
    )
    parser.add_argument("--in-results-dir", default="/test_output/")
    parser.add_argument("--in-test-result-file", default="test_result.txt")
    parser.add_argument("--in-results-file", default="test_results.tsv")
    parser.add_argument("--out-results-file", default="/test_output/test_results.tsv")
    parser.add_argument("--out-status-file", default="/test_output/check_status.tsv")
    parser.add_argument("--broken-tests", default="/repo/tests/analyzer_tech_debt.txt")
    args = parser.parse_args()

    broken_tests = []
    if os.path.exists(args.broken_tests):
        print(f"File {args.broken_tests} with broken tests found")
        with open(args.broken_tests, encoding="utf-8") as f:
            broken_tests = f.read().splitlines()
        print(f"Broken tests in the list: {len(broken_tests)}")

    state, description, test_results = process_result(
        args.in_results_dir,
        broken_tests,
        args.in_test_result_file,
        args.in_results_file,
    )
    logging.info("Result parsed")
    status = (state, description)

    def test_result_comparator(item):
        # sort by status then by check name
        order = {
            "FAIL": 0,
            "SERVER_DIED": 1,
            "Timeout": 2,
            "NOT_FAILED": 3,
            "BROKEN": 4,
            "OK": 5,
            "SKIPPED": 6,
        }
        return order.get(item[1], 10), str(item[0]), item[1]

    test_results.sort(key=test_result_comparator)

    write_results(args.out_results_file, args.out_status_file, test_results, status)
    logging.info("Result written")
