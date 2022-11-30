#!/usr/bin/env python3

import os
import logging
import argparse
import csv


def process_result(result_folder):
    status = "success"
    summary = []
    paths = []
    tests = [
        "TLPWhere",
        "TLPGroupBy",
        "TLPHaving",
        "TLPWhereGroupBy",
        "TLPDistinct",
        "TLPAggregate",
    ]

    for test in tests:
        err_path = "{}/{}.err".format(result_folder, test)
        out_path = "{}/{}.out".format(result_folder, test)
        if not os.path.exists(err_path):
            logging.info("No output err on path %s", err_path)
            summary.append((test, "SKIPPED"))
        elif not os.path.exists(out_path):
            logging.info("No output log on path %s", out_path)
        else:
            paths.append(err_path)
            paths.append(out_path)
            with open(err_path, "r") as f:
                if "AssertionError" in f.read():
                    summary.append((test, "FAIL"))
                    status = "failure"
                else:
                    summary.append((test, "OK"))

    logs_path = "{}/logs.tar.gz".format(result_folder)
    if not os.path.exists(logs_path):
        logging.info("No logs tar on path %s", logs_path)
    else:
        paths.append(logs_path)
    stdout_path = "{}/stdout.log".format(result_folder)
    if not os.path.exists(stdout_path):
        logging.info("No stdout log on path %s", stdout_path)
    else:
        paths.append(stdout_path)
    stderr_path = "{}/stderr.log".format(result_folder)
    if not os.path.exists(stderr_path):
        logging.info("No stderr log on path %s", stderr_path)
    else:
        paths.append(stderr_path)

    description = "SQLancer test run. See report"

    return status, description, summary, paths


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
        description="ClickHouse script for parsing results of sqlancer test"
    )
    parser.add_argument("--in-results-dir", default="/test_output/")
    parser.add_argument("--out-results-file", default="/test_output/test_results.tsv")
    parser.add_argument("--out-status-file", default="/test_output/check_status.tsv")
    args = parser.parse_args()

    state, description, test_results, logs = process_result(args.in_results_dir)
    logging.info("Result parsed")
    status = (state, description)
    write_results(args.out_results_file, args.out_status_file, test_results, status)
    logging.info("Result written")
