#!/usr/bin/env python3
import argparse
import csv
import logging
import os
import subprocess
import sys


from clickhouse_helper import (
    ClickHouseHelper,
    mark_flaky_tests,
    prepare_tests_results_for_clickhouse,
)
from commit_status_helper import post_commit_status, get_commit
from docker_pull_helper import get_image_with_version
from env_helper import (
    RUNNER_TEMP,
    GITHUB_WORKSPACE,
    GITHUB_REPOSITORY,
    GITHUB_SERVER_URL,
)
from get_robot_token import get_best_robot_token
from github_helper import GitHub
from git_helper import git_runner
from pr_info import PRInfo, SKIP_SIMPLE_CHECK_LABEL
from rerun_helper import RerunHelper
from s3_helper import S3Helper
from ssh import SSHKey
from stopwatch import Stopwatch
from upload_result_helper import upload_results

NAME = "Style Check (actions)"


def process_result(result_folder):
    test_results = []
    additional_files = []
    # Just upload all files from result_folder.
    # If task provides processed results, then it's responsible
    # for content of result_folder.
    if os.path.exists(result_folder):
        test_files = [
            f
            for f in os.listdir(result_folder)
            if os.path.isfile(os.path.join(result_folder, f))
        ]
        additional_files = [os.path.join(result_folder, f) for f in test_files]

    status = []
    status_path = os.path.join(result_folder, "check_status.tsv")
    if os.path.exists(status_path):
        logging.info("Found test_results.tsv")
        with open(status_path, "r", encoding="utf-8") as status_file:
            status = list(csv.reader(status_file, delimiter="\t"))
    if len(status) != 1 or len(status[0]) != 2:
        logging.info("Files in result folder %s", os.listdir(result_folder))
        return "error", "Invalid check_status.tsv", test_results, additional_files
    state, description = status[0][0], status[0][1]

    try:
        results_path = os.path.join(result_folder, "test_results.tsv")
        with open(results_path, "r", encoding="utf-8") as fd:
            test_results = list(csv.reader(fd, delimiter="\t"))
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
        "--no-push",
        action="store_false",
        dest="push",
        help="do not commit and push automatic fixes",
        default=argparse.SUPPRESS,
    )
    return parser.parse_args()


def checkout_head(pr_info: PRInfo):
    # It works ONLY for PRs, and only over ssh, so either
    # ROBOT_CLICKHOUSE_SSH_KEY should be set or ssh-agent should work
    assert pr_info.number
    remote_url = f"git@github.com:{pr_info.head_name}"
    git_prefix = (  # All commits to remote are done as robot-clickhouse
        "git -c user.email=robot-clickhouse@clickhouse.com "
        "-c user.name=robot-clickhouse -c commit.gpgsign=false "
                  "-c core.sshCommand="

                  "'ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no'"
    )
    fetch_cmd = (
        f"{git_prefix} fetch --depth=1 "
        f"{remote_url} {pr_info.head_ref}:head-{pr_info.head_ref}"
    )
    if os.getenv("ROBOT_CLICKHOUSE_SSH_KEY", ""):
        with SSHKey("ROBOT_CLICKHOUSE_SSH_KEY"):
            git_runner(fetch_cmd)
    else:
        git_runner(fetch_cmd)
    git_runner(f"git checkout -f head-{pr_info.head_ref}")


def commit_push_staged(pr_info: PRInfo):
    # It works ONLY for PRs, and only over ssh, so either
    # ROBOT_CLICKHOUSE_SSH_KEY should be set or ssh-agent should work
    assert pr_info.number
    git_staged = git_runner("git diff --cached --name-only")
    if not git_staged:
        return
    remote_url = f"git@github.com:{pr_info.head_name}"
    git_prefix = (  # All commits to remote are done as robot-clickhouse
        "git -c user.email=robot-clickhouse@clickhouse.com "
        "-c user.name=robot-clickhouse -c commit.gpgsign=false "
                  "-c core.sshCommand="

                  "'ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no'"
    )
    git_runner(f"{git_prefix} commit -m 'Automatic style fix'")
    push_cmd = (
        f"{git_prefix} push {remote_url} head-{pr_info.head_ref}:{pr_info.head_ref}"
    )
    if os.getenv("ROBOT_CLICKHOUSE_SSH_KEY", ""):
        with SSHKey("ROBOT_CLICKHOUSE_SSH_KEY"):
            git_runner(push_cmd)
    else:
        git_runner(push_cmd)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    logging.getLogger("git_helper").setLevel(logging.DEBUG)
    args = parse_args()

    stopwatch = Stopwatch()

    repo_path = GITHUB_WORKSPACE
    temp_path = os.path.join(RUNNER_TEMP, "style_check")

    pr_info = PRInfo()
    if args.push:
        checkout_head(pr_info)

    gh = GitHub(get_best_robot_token())

    rerun_helper = RerunHelper(gh, pr_info, NAME)
    if rerun_helper.is_already_finished_by_status():
        logging.info("Check is already finished according to github status, exiting")
        sys.exit(0)

    if not os.path.exists(temp_path):
        os.makedirs(temp_path)

    docker_image = get_image_with_version(temp_path, "clickhouse/style-test")
    s3_helper = S3Helper("https://s3.amazonaws.com")

    cmd = (
        f"docker run -u $(id -u ${{USER}}):$(id -g ${{USER}}) --cap-add=SYS_PTRACE "
        f"--volume={repo_path}:/ClickHouse --volume={temp_path}:/test_output "
        f"{docker_image}"
    )

    logging.info("Is going to run the command: %s", cmd)
    subprocess.check_call(
        cmd,
        shell=True,
    )

    if args.push:
        commit_push_staged(
            pr_info
        )

    state, description, test_results, additional_files = process_result(temp_path)
    ch_helper = ClickHouseHelper()
    mark_flaky_tests(ch_helper, NAME, test_results)

    report_url = upload_results(
        s3_helper, pr_info.number, pr_info.sha, test_results, additional_files, NAME
    )
    print(f"::notice ::Report url: {report_url}")
    post_commit_status(gh, pr_info.sha, NAME, description, state, report_url)

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

    if state == "error":
        if SKIP_SIMPLE_CHECK_LABEL not in pr_info.labels:
            url = (
                f"{GITHUB_SERVER_URL}/{GITHUB_REPOSITORY}/"
                "blob/master/.github/PULL_REQUEST_TEMPLATE.md?plain=1"
            )
            commit = get_commit(gh, pr_info.sha)
            commit.create_status(
                context="Simple Check",
                description=f"{NAME} failed",
                state="failed",
                target_url=url,
            )
        sys.exit(1)
