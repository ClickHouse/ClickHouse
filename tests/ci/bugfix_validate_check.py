#!/usr/bin/env python3

from pathlib import Path
from typing import List, Tuple, Optional
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


# Returns (is_ok, test_results, error_message)
def process_result(file_path: Path) -> Tuple[bool, TestResults, Optional[str]]:
    test_results = []  # type: TestResults
    state, report_url, description = post_commit_status_from_file(file_path)
    prefix = file_path.parent.name
    if description.strip() in [
        "Invalid check_status.tsv",
        "Not found test_results.tsv",
        "Empty test_results.tsv",
    ]:
        status = (
            f'Check failed (<a href="{report_url}">Report</a>)'
            if report_url != "null"
            else "Check failed"
        )
        return False, [TestResult(f"{prefix}: {description}", status)], "Check failed"

    is_ok = state == "success"
    if is_ok and report_url == "null":
        return is_ok, test_results, None

    status = (
        f'OK: Bug reproduced (<a href="{report_url}">Report</a>)'
        if is_ok
        else f'Bug is not reproduced (<a href="{report_url}">Report</a>)'
    )
    test_results.append(TestResult(f"{prefix}: {description}", status))
    return is_ok, test_results, None


def process_all_results(
    file_paths: List[Path],
) -> Tuple[bool, TestResults, Optional[str]]:
    any_ok = False
    all_results = []
    error = None
    for status_path in file_paths:
        is_ok, test_results, error = process_result(status_path)
        any_ok = any_ok or is_ok
        if test_results is not None:
            all_results.extend(test_results)

    return any_ok, all_results, error


def main():
    logging.basicConfig(level=logging.INFO)
    args = parse_args()
    status_files = args.files  # type: List[Path]

    check_name_with_group = "Bugfix validate check"

    is_ok, test_results, error = process_all_results(status_files)

    description = ""
    if error:
        description = error
    elif not is_ok:
        description = "Changed tests don't reproduce the bug"

    pr_info = PRInfo()
    if not test_results:
        description = "No results to upload"
        report_url = ""
        logging.info("No results to upload")
    else:
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
        description,
        check_name_with_group,
        pr_info,
        dump_to_file=True,
    )


if __name__ == "__main__":
    main()
