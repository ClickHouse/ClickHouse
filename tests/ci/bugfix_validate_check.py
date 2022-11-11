#!/usr/bin/env python3

import argparse
import csv
import itertools
import logging
import os

from github import Github

from s3_helper import S3Helper
from get_robot_token import get_best_robot_token
from pr_info import PRInfo
from upload_result_helper import upload_results
from commit_status_helper import post_commit_status


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("status", nargs="+", help="Path to status file")
    return parser.parse_args()


def post_commit_status_from_file(file_path):
    res = []
    with open(file_path, "r", encoding="utf-8") as f:
        fin = csv.reader(f, delimiter="\t")
        res = list(itertools.islice(fin, 1))
    if len(res) < 1:
        raise Exception(f'Can\'t read from "{file_path}"')
    if len(res[0]) != 3:
        raise Exception(f'Can\'t read from "{file_path}"')
    return res[0]


def process_result(file_path):
    test_results = []
    state, report_url, description = post_commit_status_from_file(file_path)
    prefix = os.path.basename(os.path.dirname(file_path))
    is_ok = state == "success"
    if is_ok and report_url == "null":
        return is_ok, None

    status = f'OK: Bug reproduced (<a href="{report_url}">Report</a>'
    if not is_ok:
        status = f'Bug is not reproduced (<a href="{report_url}">Report</a>)'
    test_results.append([f"{prefix}: {description}", status])
    return is_ok, test_results


def process_all_results(file_paths):
    any_ok = False
    all_results = []
    for status_path in file_paths:
        is_ok, test_results = process_result(status_path)
        any_ok = any_ok or is_ok
        if test_results is not None:
            all_results.extend(test_results)

    return any_ok, all_results


def main(args):
    logging.basicConfig(level=logging.INFO)

    check_name_with_group = "Bugfix validate check"

    is_ok, test_results = process_all_results(args.status)

    if not test_results:
        logging.info("No results to upload")
        return

    pr_info = PRInfo()
    report_url = upload_results(
        S3Helper(),
        pr_info.number,
        pr_info.sha,
        test_results,
        [],
        check_name_with_group,
    )

    gh = Github(get_best_robot_token(), per_page=100)
    post_commit_status(
        gh,
        pr_info.sha,
        check_name_with_group,
        "" if is_ok else "Changed tests doesn't reproduce the bug",
        "success" if is_ok else "error",
        report_url,
    )


if __name__ == "__main__":
    main(parse_args())
