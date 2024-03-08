#!/usr/bin/env python3
import argparse
import csv
import logging
import os
import subprocess
import sys
from pathlib import Path
from typing import List, Tuple

from docker_images_helper import get_docker_image, pull_image
from env_helper import REPO_COPY, TEMP_PATH
from git_helper import GIT_PREFIX, git_runner
from pr_info import PRInfo
from report import ERROR, FAILURE, SUCCESS, JobReport, TestResults, read_test_results
from ssh import SSHKey
from stopwatch import Stopwatch


def process_result(
    result_directory: Path,
) -> Tuple[str, str, TestResults, List[Path]]:
    test_results = []  # type: TestResults
    additional_files = []
    # Just upload all files from result_directory.
    # If task provides processed results, then it's responsible
    # for content of result_directory.
    if result_directory.exists():
        additional_files = [p for p in result_directory.iterdir() if p.is_file()]

    status = []
    status_path = result_directory / "check_status.tsv"
    if status_path.exists():
        logging.info("Found check_status.tsv")
        with open(status_path, "r", encoding="utf-8") as status_file:
            status = list(csv.reader(status_file, delimiter="\t"))
    if len(status) != 1 or len(status[0]) != 2:
        logging.info("Files in result folder %s", os.listdir(result_directory))
        return ERROR, "Invalid check_status.tsv", test_results, additional_files
    state, description = status[0][0], status[0][1]

    try:
        results_path = result_directory / "test_results.tsv"
        test_results = read_test_results(results_path)
        if len(test_results) == 0:
            raise ValueError("Empty results")

        return state, description, test_results, additional_files
    except Exception:
        if state == SUCCESS:
            state, description = ERROR, "Failed to read test_results.tsv"
        return state, description, test_results, additional_files


def parse_args():
    parser = argparse.ArgumentParser("Check and report style issues in the repository")
    parser.add_argument("--push", default=True, help=argparse.SUPPRESS)
    parser.add_argument(
        "--no-push",
        action="store_false",
        dest="push",
        help="do not commit and push automatic fixes",
        default=argparse.SUPPRESS,
    )
    return parser.parse_args()


def checkout_head(pr_info: PRInfo) -> None:
    # It works ONLY for PRs, and only over ssh, so either
    # ROBOT_CLICKHOUSE_SSH_KEY should be set or ssh-agent should work
    assert pr_info.number
    if not pr_info.head_name == pr_info.base_name:
        # We can't push to forks, sorry folks
        return
    remote_url = pr_info.event["pull_request"]["base"]["repo"]["ssh_url"]
    fetch_cmd = (
        f"{GIT_PREFIX} fetch --depth=1 "
        f"{remote_url} {pr_info.head_ref}:head-{pr_info.head_ref}"
    )
    if os.getenv("ROBOT_CLICKHOUSE_SSH_KEY", ""):
        with SSHKey("ROBOT_CLICKHOUSE_SSH_KEY"):
            git_runner(fetch_cmd)
    else:
        git_runner(fetch_cmd)
    git_runner(f"git checkout -f head-{pr_info.head_ref}")


def commit_push_staged(pr_info: PRInfo) -> None:
    # It works ONLY for PRs, and only over ssh, so either
    # ROBOT_CLICKHOUSE_SSH_KEY should be set or ssh-agent should work
    assert pr_info.number
    if not pr_info.head_name == pr_info.base_name:
        # We can't push to forks, sorry folks
        return
    git_staged = git_runner("git diff --cached --name-only")
    if not git_staged:
        return
    remote_url = pr_info.event["pull_request"]["base"]["repo"]["ssh_url"]
    git_runner(f"{GIT_PREFIX} commit -m 'Automatic style fix'")
    push_cmd = (
        f"{GIT_PREFIX} push {remote_url} head-{pr_info.head_ref}:{pr_info.head_ref}"
    )
    if os.getenv("ROBOT_CLICKHOUSE_SSH_KEY", ""):
        with SSHKey("ROBOT_CLICKHOUSE_SSH_KEY"):
            git_runner(push_cmd)
    else:
        git_runner(push_cmd)


def checkout_last_ref(pr_info: PRInfo) -> None:
    # Checkout the merge commit back to avoid special effects
    assert pr_info.number
    if not pr_info.head_name == pr_info.base_name:
        # We can't push to forks, sorry folks
        return
    git_runner("git checkout -f -")


def main():
    logging.basicConfig(level=logging.INFO)
    logging.getLogger("git_helper").setLevel(logging.DEBUG)
    args = parse_args()

    stopwatch = Stopwatch()

    repo_path = Path(REPO_COPY)
    temp_path = Path(TEMP_PATH)
    temp_path.mkdir(parents=True, exist_ok=True)

    pr_info = PRInfo()

    IMAGE_NAME = "clickhouse/style-test"
    image = pull_image(get_docker_image(IMAGE_NAME))
    cmd = (
        f"docker run -u $(id -u ${{USER}}):$(id -g ${{USER}}) --cap-add=SYS_PTRACE "
        f"--volume={repo_path}:/ClickHouse --volume={temp_path}:/test_output "
        f"{image}"
    )

    if args.push:
        checkout_head(pr_info)

    logging.info("Is going to run the command: %s", cmd)
    subprocess.check_call(
        cmd,
        shell=True,
    )

    if args.push:
        commit_push_staged(pr_info)
        checkout_last_ref(pr_info)

    state, description, test_results, additional_files = process_result(temp_path)

    JobReport(
        description=description,
        test_results=test_results,
        status=state,
        start_time=stopwatch.start_time_str,
        duration=stopwatch.duration_seconds,
        additional_files=additional_files,
    ).dump()

    if state in [ERROR, FAILURE]:
        print(f"Style check failed: [{description}]")
        sys.exit(1)


if __name__ == "__main__":
    main()
