#!/usr/bin/env python3

import os
import logging
import argparse
import csv
import json


def process_result(result_folder):
    json_path = os.path.join(result_folder, "results.json")
    if not os.path.exists(json_path):
        return "success", "No testflows in branch", None, []

    test_binary_log = os.path.join(result_folder, "test.log")
    with open(json_path) as source:
        results = json.loads(source.read())

    total_tests = 0
    total_ok = 0
    total_fail = 0
    total_other = 0
    test_results = []
    for test in results["tests"]:
        test_name = test["test"]["test_name"]
        test_result = test["result"]["result_type"].upper()
        test_time = str(test["result"]["message_rtime"])
        total_tests += 1
        if test_result == "OK":
            total_ok += 1
        elif test_result == "FAIL" or test_result == "ERROR":
            total_fail += 1
        else:
            total_other += 1

        test_results.append((test_name, test_result, test_time))
    if total_fail != 0:
        status = "failure"
    else:
        status = "success"

    description = "failed: {}, passed: {}, other: {}".format(
        total_fail, total_ok, total_other
    )
    return status, description, test_results, [json_path, test_binary_log]


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
        description="ClickHouse script for parsing results of Testflows tests"
    )
    parser.add_argument("--in-results-dir", default="./")
    parser.add_argument("--out-results-file", default="./test_results.tsv")
    parser.add_argument("--out-status-file", default="./check_status.tsv")
    args = parser.parse_args()

    state, description, test_results, logs = process_result(args.in_results_dir)
    logging.info("Result parsed")
    status = (state, description)
    write_results(args.out_results_file, args.out_status_file, test_results, status)
    logging.info("Result written")
