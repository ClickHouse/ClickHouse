#!/usr/bin/env python3
import argparse
import atexit
import csv
import logging
import os
import subprocess
import sys
from pathlib import Path
from typing import List, Tuple

from clickhouse_helper import (
    ClickHouseHelper,
    prepare_tests_results_for_clickhouse,
)
from commit_status_helper import (
    RerunHelper,
    get_commit,
    post_commit_status,
    update_mergeable_check,
)

from env_helper import REPO_COPY, TEMP_PATH
from get_robot_token import get_best_robot_token
from github_helper import GitHub
from git_helper import git_runner
from pr_info import PRInfo
from report import TestResults, read_test_results
from s3_helper import S3Helper
from ssh import SSHKey
from stopwatch import Stopwatch
from docker_images_helper import get_docker_image, pull_image
from upload_result_helper import upload_results

NAME = "Style Check"

GIT_PREFIX = (  # All commits to remote are done as robot-clickhouse
    "git -c user.email=robot-clickhouse@users.noreply.github.com "
    "-c user.name=robot-clickhouse -c commit.gpgsign=false "
    "-c core.sshCommand="
    "'ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no'"
)


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
        return "error", "Invalid check_status.tsv", test_results, additional_files
    state, description = status[0][0], status[0][1]

    try:
        results_path = result_directory / "test_results.tsv"
        test_results = read_test_results(results_path)
        if len(test_results) == 0:
            raise Exception("Empty results")

        return state, description, test_results, additional_files
    except Exception:
        if state == "success":
            state, description = "error", "Failed to read test_results.tsv"
        return state, description, test_results, additional_files


def parse_args():
    parser = argparse.ArgumentParser("Check and report style issues in the repository")
    parser.add_argument("--push", default=True, help=argparse.SUPPRESS)
    parser.add_argument(
        "--tag",
        required=False,
        default="",
        help="tag for docker image",
    )
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


def main():
    logging.basicConfig(level=logging.INFO)
    logging.getLogger("git_helper").setLevel(logging.DEBUG)
    args = parse_args()

    stopwatch = Stopwatch()

    repo_path = Path(REPO_COPY)
    temp_path = Path(TEMP_PATH)
    temp_path.mkdir(parents=True, exist_ok=True)

    pr_info = PRInfo()
    gh = GitHub(get_best_robot_token(), create_cache_dir=False)
    commit = get_commit(gh, pr_info.sha)
    if args.push:
        checkout_head(pr_info)

    atexit.register(update_mergeable_check, gh, pr_info, NAME)

    rerun_helper = RerunHelper(commit, NAME)
    if rerun_helper.is_already_finished_by_status():
        logging.info("Check is already finished according to github status, exiting")
        # Finish with the same code as previous
        state = rerun_helper.get_finished_status().state  # type: ignore
        # state == "success" -> code = 0
        code = int(state != "success")
        sys.exit(code)

    s3_helper = S3Helper()

    IMAGE_NAME = "clickhouse/style-test"
    image = pull_image(get_docker_image(IMAGE_NAME))
    cmd = (
        f"docker run -u $(id -u ${{USER}}):$(id -g ${{USER}}) --cap-add=SYS_PTRACE "
        f"--volume={repo_path}:/ClickHouse --volume={temp_path}:/test_output "
        f"{image}"
    )

    logging.info("Is going to run the command: %s", cmd)
    subprocess.check_call(
        cmd,
        shell=True,
    )

    if args.push:
        commit_push_staged(pr_info)

    state, description, test_results, additional_files = process_result(temp_path)
    ch_helper = ClickHouseHelper()

    report_url = upload_results(
        s3_helper, pr_info.number, pr_info.sha, test_results, additional_files, NAME
    )
    print(f"::notice ::Report url: {report_url}")
    post_commit_status(commit, state, report_url, description, NAME, pr_info)

    prepared_events = prepare_tests_results_for_clickhouse(
        pr_info,
        test_results,
        state,
        stopwatch.duration_seconds,
        stopwatch.start_time_str,
        report_url,
        NAME,
    )
    ch_helper.insert_events_into(db="default", table="checks", events=prepared_events)

    if state in ["error", "failure"]:
        sys.exit(1)


if __name__ == "__main__":
    main()
