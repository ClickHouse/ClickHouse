#!/usr/bin/env python3

from pathlib import Path
from typing import List, Tuple
import argparse
import csv
import logging

from github import Github

from commit_status_helper import get_commit, post_commit_status
from get_robot_token import get_best_robot_token
from pr_info import PRInfo
from report import TestResults, TestResult
from s3_helper import S3Helper
from upload_result_helper import upload_results


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("files", nargs="+", type=Path, help="Path to status files")
    return parser.parse_args()


def post_commit_status_from_file(file_path: Path) -> List[str]:
    with open(file_path, "r", encoding="utf-8") as f:
        res = list(csv.reader(f, delimiter="\t"))
    if len(res) < 1:
        raise Exception(f'Can\'t read from "{file_path}"')
    if len(res[0]) != 3:
        raise Exception(f'Can\'t read from "{file_path}"')
    return res[0]


def process_result(file_path: Path) -> Tuple[bool, TestResults]:
    test_results = []  # type: TestResults
    state, report_url, description = post_commit_status_from_file(file_path)
    prefix = file_path.parent.name
    is_ok = state == "success"
    if is_ok and report_url == "null":
        return is_ok, test_results

    status = (
        f'OK: Bug reproduced (<a href="{report_url}">Report</a>)'
        if is_ok
        else f'Bug is not reproduced (<a href="{report_url}">Report</a>)'
    )
    test_results.append(TestResult(f"{prefix}: {description}", status))
    return is_ok, test_results


def process_all_results(file_paths: List[Path]) -> Tuple[bool, TestResults]:
    any_ok = False
    all_results = []
    for status_path in file_paths:
        is_ok, test_results = process_result(status_path)
        any_ok = any_ok or is_ok
        if test_results is not None:
            all_results.extend(test_results)

    return any_ok, all_results


def main():
    logging.basicConfig(level=logging.INFO)
    args = parse_args()
    status_files = args.files  # type: List[Path]

    check_name_with_group = "Bugfix validate check"

    is_ok, test_results = process_all_results(status_files)

    if not test_results:
        logging.info("No results to upload")
        return

    pr_info = PRInfo()
    report_url = upload_results(
        S3Helper(),
        pr_info.number,
        pr_info.sha,
        test_results,
        status_files,
        check_name_with_group,
    )

    gh = Github(get_best_robot_token(), per_page=100)
    commit = get_commit(gh, pr_info.sha)
    post_commit_status(
        commit,
        "success" if is_ok else "error",
        report_url,
        "" if is_ok else "Changed tests don't reproduce the bug",
        check_name_with_group,
        pr_info,
    )


if __name__ == "__main__":
    main()
