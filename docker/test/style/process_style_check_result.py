#!/usr/bin/env python3

import os
import logging
import argparse
import csv


def process_result(result_folder):
    status = "success"
    description = ""
    test_results = []

    style_log_path = '{}/style_output.txt'.format(result_folder)
    if not os.path.exists(style_log_path):
        logging.info("No style check log on path %s", style_log_path)
        return "exception", "No style check log", []
    elif os.stat(style_log_path).st_size != 0:
        description += "Style check failed. "
        test_results.append(("Style check", "FAIL"))
        status = "failure"  # Disabled for now
    else:
        test_results.append(("Style check", "OK"))

    typos_log_path = '{}/typos_output.txt'.format(result_folder)
    if not os.path.exists(style_log_path):
        logging.info("No typos check log on path %s", style_log_path)
        return "exception", "No typos check log", []
    elif os.stat(typos_log_path).st_size != 0:
        description += "Typos check failed. "
        test_results.append(("Typos check", "FAIL"))
        status = "failure"
    else:
        test_results.append(("Typos check", "OK"))

    whitespaces_log_path = '{}/whitespaces_output.txt'.format(result_folder)
    if not os.path.exists(style_log_path):
        logging.info("No whitespaces check log on path %s", style_log_path)
        return "exception", "No whitespaces check log", []
    elif os.stat(whitespaces_log_path).st_size != 0:
        description += "Whitespaces check failed. "
        test_results.append(("Whitespaces check", "FAIL"))
        status = "failure"
    else:
        test_results.append(("Whitespaces check", "OK"))

    duplicate_log_path = '{}/duplicate_output.txt'.format(result_folder)
    if not os.path.exists(duplicate_log_path):
        logging.info("No header duplicates check log on path %s", duplicate_log_path)
        return "exception", "No header duplicates check log", []
    elif os.stat(duplicate_log_path).st_size != 0:
        description += " Header duplicates check failed. "
        test_results.append(("Header duplicates check", "FAIL"))
        status = "failure"
    else:
        test_results.append(("Header duplicates check", "OK"))

    shellcheck_log_path = '{}/shellcheck_output.txt'.format(result_folder)
    if not os.path.exists(shellcheck_log_path):
        logging.info("No shellcheck  log on path %s", shellcheck_log_path)
        return "exception", "No shellcheck log", []
    elif os.stat(shellcheck_log_path).st_size != 0:
        description += " Shellcheck check failed. "
        test_results.append(("Shellcheck ", "FAIL"))
        status = "failure"
    else:
        test_results.append(("Shellcheck", "OK"))

    if not description:
        description += "Style check success"

    return status, description, test_results


def write_results(results_file, status_file, results, status):
    with open(results_file, 'w') as f:
        out = csv.writer(f, delimiter='\t')
        out.writerows(results)
    with open(status_file, 'w') as f:
        out = csv.writer(f, delimiter='\t')
        out.writerow(status)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')
    parser = argparse.ArgumentParser(description="ClickHouse script for parsing results of style check")
    parser.add_argument("--in-results-dir", default='/test_output/')
    parser.add_argument("--out-results-file", default='/test_output/test_results.tsv')
    parser.add_argument("--out-status-file", default='/test_output/check_status.tsv')
    args = parser.parse_args()

    state, description, test_results = process_result(args.in_results_dir)
    logging.info("Result parsed")
    status = (state, description)
    write_results(args.out_results_file, args.out_status_file, test_results, status)
    logging.info("Result written")
