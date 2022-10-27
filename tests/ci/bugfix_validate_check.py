#!/usr/bin/env python3

import argparse
import csv
import itertools
import os
import sys


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


def process_results(file_path):
    test_results = []
    state, report_url, description = post_commit_status_from_file(file_path)
    prefix = os.path.basename(os.path.dirname(file_path))
    is_ok = state == "success"

    test_results.append(
        [
            f"{prefix}: {description}",
            "Bug reproduced" if is_ok else "Bug is not reproduced",
            report_url,
        ]
    )
    return is_ok, test_results


def main(args):
    all_ok = False
    all_results = []
    for status_path in args.status:
        is_ok, test_results = process_results(status_path)
        all_ok = all_ok or is_ok
        all_results.extend(test_results)

    check_name_with_group = "Bugfix validate check"

    pr_info = PRInfo()
    report_url = upload_results(
        S3Helper(),
        pr_info.number,
        pr_info.sha,
        all_results,
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
