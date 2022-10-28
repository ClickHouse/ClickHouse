#!/usr/bin/env python3

import os
import logging
import argparse
import csv

OK_SIGN = "[ OK "
FAIL_SIGN = "[ FAIL "
TIMEOUT_SIGN = "[ Timeout! "
UNKNOWN_SIGN = "[ UNKNOWN "
SKIPPED_SIGN = "[ SKIPPED "
HUNG_SIGN = "Found hung queries in processlist"
DATABASE_SIGN = "Database: "

SUCCESS_FINISH_SIGNS = ["All tests have finished", "No tests were run"]

RETRIES_SIGN = "Some tests were restarted"


def process_test_log(log_path):
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
    with open(log_path, "r") as test_file:
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
                    failed += 1
                    test_results.append((test_name, "Timeout", test_time, []))
                elif FAIL_SIGN in line:
                    failed += 1
                    test_results.append((test_name, "FAIL", test_time, []))
                elif UNKNOWN_SIGN in line:
                    unknown += 1
                    test_results.append((test_name, "FAIL", test_time, []))
                elif SKIPPED_SIGN in line:
                    skipped += 1
                    test_results.append((test_name, "SKIPPED", test_time, []))
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

    test_results = [
        (test[0], test[1], test[2], "".join(test[3])[:4096]) for test in test_results
    ]

    return (
        total,
        skipped,
        unknown,
        failed,
        success,
        hung,
        success_finish,
        retries,
        test_results,
    )


def process_result(result_path):
    test_results = []
    state = "success"
    description = ""
    files = os.listdir(result_path)
    if files:
        logging.info("Find files in result folder %s", ",".join(files))
        result_path = os.path.join(result_path, "test_result.txt")
    else:
        result_path = None
        description = "No output log"
        state = "error"

    if result_path and os.path.exists(result_path):
        (
            total,
            skipped,
            unknown,
            failed,
            success,
            hung,
            success_finish,
            retries,
            test_results,
        ) = process_test_log(result_path)
        is_flacky_check = 1 < int(os.environ.get("NUM_TRIES", 1))
        logging.info("Is flaky check: %s", is_flacky_check)
        # If no tests were run (success == 0) it indicates an error (e.g. server did not start or crashed immediately)
        # But it's Ok for "flaky checks" - they can contain just one test for check which is marked as skipped.
        if failed != 0 or unknown != 0 or (success == 0 and (not is_flacky_check)):
            state = "failure"

        if hung:
            description = "Some queries hung, "
            state = "failure"
            test_results.append(("Some queries hung", "FAIL", "0", ""))
        elif not success_finish:
            description = "Tests are not finished, "
            state = "failure"
            test_results.append(("Tests are not finished", "FAIL", "0", ""))
        elif retries:
            description = "Some tests restarted, "
            test_results.append(("Some tests restarted", "SKIPPED", "0", ""))
        else:
            description = ""

        description += "fail: {}, passed: {}".format(failed, success)
        if skipped != 0:
            description += ", skipped: {}".format(skipped)
        if unknown != 0:
            description += ", unknown: {}".format(unknown)
    else:
        state = "failure"
        description = "Output log doesn't exist"
        test_results = []

    return state, description, test_results


def write_results(results_file, status_file, results, status):
    with open(results_file, "w") as f:
        out = csv.writer(f, delimiter="\t")
        out.writerows(results)
    with open(status_file, "w") as f:
        out = csv.writer(f, delimiter="\t")
        out.writerow(status)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
    parser = argparse.ArgumentParser(
        description="ClickHouse script for parsing results of functional tests"
    )
    parser.add_argument("--in-results-dir", default="/test_output/")
    parser.add_argument("--out-results-file", default="/test_output/test_results.tsv")
    parser.add_argument("--out-status-file", default="/test_output/check_status.tsv")
    args = parser.parse_args()

    state, description, test_results = process_result(args.in_results_dir)
    logging.info("Result parsed")
    status = (state, description)
    write_results(args.out_results_file, args.out_status_file, test_results, status)
    logging.info("Result written")
