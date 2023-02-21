#!/usr/bin/env python3

import os
import logging
import argparse
import csv

RESULT_LOG_NAME = "run.log"


def process_result(result_folder):

    status = "success"
    description = "Server started and responded"
    summary = [("Smoke test", "OK")]
    with open(os.path.join(result_folder, RESULT_LOG_NAME), "r") as run_log:
        lines = run_log.read().split("\n")
        if not lines or lines[0].strip() != "OK":
            status = "failure"
            logging.info("Lines is not ok: %s", str("\n".join(lines)))
            summary = [("Smoke test", "FAIL")]
            description = "Server failed to respond, see result in logs"

    result_logs = []
    server_log_path = os.path.join(result_folder, "clickhouse-server.log")
    stderr_log_path = os.path.join(result_folder, "stderr.log")
    client_stderr_log_path = os.path.join(result_folder, "clientstderr.log")

    if os.path.exists(server_log_path):
        result_logs.append(server_log_path)

    if os.path.exists(stderr_log_path):
        result_logs.append(stderr_log_path)

    if os.path.exists(client_stderr_log_path):
        result_logs.append(client_stderr_log_path)

    return status, description, summary, result_logs


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
        description="ClickHouse script for parsing results of split build smoke test"
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
