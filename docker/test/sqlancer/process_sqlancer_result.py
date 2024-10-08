#!/usr/bin/env python3

import argparse
import csv
import logging
import os


def process_result(result_folder):
    status = "success"
    summary = []
    paths = []
    tests = [
        "TLPAggregate",
        "TLPDistinct",
        "TLPGroupBy",
        "TLPHaving",
        "TLPWhere",
        "NoREC",
    ]
    failed_tests = []

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
                    failed_tests.append(test)
                    status = "failure"
                else:
                    summary.append((test, "OK"))

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

    description = "SQLancer run successfully"
    if status == "failure":
        description = f"Failed oracles: {failed_tests}"

    return status, description, summary, paths


def write_results(
    results_file, status_file, description_file, results, status, description
):
    with open(results_file, "w") as f:
        out = csv.writer(f, delimiter="\t")
        out.writerows(results)
    with open(status_file, "w") as f:
        f.write(status + "\n")
    with open(description_file, "w") as f:
        f.write(description + "\n")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
    parser = argparse.ArgumentParser(
        description="ClickHouse script for parsing results of sqlancer test"
    )
    parser.add_argument("--in-results-dir", default="/workspace/")
    parser.add_argument("--out-results-file", default="/workspace/summary.tsv")
    parser.add_argument("--out-description-file", default="/workspace/description.txt")
    parser.add_argument("--out-status-file", default="/workspace/status.txt")
    args = parser.parse_args()

    status, description, summary, logs = process_result(args.in_results_dir)
    logging.info("Result parsed")
    write_results(
        args.out_results_file,
        args.out_status_file,
        args.out_description_file,
        summary,
        status,
        description,
    )
    logging.info("Result written")
