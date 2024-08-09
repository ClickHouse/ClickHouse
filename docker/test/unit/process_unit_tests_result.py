#!/usr/bin/env python3

import os
import logging
import argparse
import csv

OK_SIGN = "OK ]"
FAILED_SIGN = "FAILED  ]"
SEGFAULT = "Segmentation fault"
SIGNAL = "received signal SIG"
PASSED = "PASSED"


def get_test_name(line):
    elements = reversed(line.split(" "))
    for element in elements:
        if "(" not in element and ")" not in element:
            return element
    raise Exception("No test name in line '{}'".format(line))


def process_result(result_folder):
    summary = []
    total_counter = 0
    failed_counter = 0
    result_log_path = "{}/test_result.txt".format(result_folder)
    if not os.path.exists(result_log_path):
        logging.info("No output log on path %s", result_log_path)
        return "exception", "No output log", []

    status = "success"
    description = ""
    passed = False
    with open(result_log_path, "r") as test_result:
        for line in test_result:
            if OK_SIGN in line:
                logging.info("Found ok line: '%s'", line)
                test_name = get_test_name(line.strip())
                logging.info("Test name: '%s'", test_name)
                summary.append((test_name, "OK"))
                total_counter += 1
            elif FAILED_SIGN in line and "listed below" not in line and "ms)" in line:
                logging.info("Found fail line: '%s'", line)
                test_name = get_test_name(line.strip())
                logging.info("Test name: '%s'", test_name)
                summary.append((test_name, "FAIL"))
                total_counter += 1
                failed_counter += 1
            elif SEGFAULT in line:
                logging.info("Found segfault line: '%s'", line)
                status = "failure"
                description += "Segmentation fault. "
                break
            elif SIGNAL in line:
                logging.info("Received signal line: '%s'", line)
                status = "failure"
                description += "Exit on signal. "
                break
            elif PASSED in line:
                logging.info("PASSED record found: '%s'", line)
                passed = True

    if not passed:
        status = "failure"
        description += "PASSED record not found. "

    if failed_counter != 0:
        status = "failure"

    if not description:
        description += "fail: {}, passed: {}".format(
            failed_counter, total_counter - failed_counter
        )

    return status, description, summary


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
        description="ClickHouse script for parsing results of unit tests"
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
