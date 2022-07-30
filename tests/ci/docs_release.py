#!/usr/bin/env python3
import argparse
import logging
import subprocess
import os
import sys

from github import Github

from env_helper import TEMP_PATH, REPO_COPY, CLOUDFLARE_TOKEN
from s3_helper import S3Helper
from pr_info import PRInfo
from get_robot_token import get_best_robot_token
from ssh import SSHKey
from upload_result_helper import upload_results
from docker_pull_helper import get_image_with_version
from commit_status_helper import get_commit
from rerun_helper import RerunHelper
from tee_popen import TeePopen

NAME = "Docs Release"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        description="ClickHouse building script using prebuilt Docker image",
    )
    parser.add_argument(
        "--as-root", action="store_true", help="if the container should run as root"
    )
    return parser.parse_args()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    args = parse_args()

    temp_path = TEMP_PATH
    repo_path = REPO_COPY

    gh = Github(get_best_robot_token())
    pr_info = PRInfo()
    rerun_helper = RerunHelper(gh, pr_info, NAME)
    if rerun_helper.is_already_finished_by_status():
        logging.info("Check is already finished according to github status, exiting")
        sys.exit(0)

    if not os.path.exists(temp_path):
        os.makedirs(temp_path)

    docker_image = get_image_with_version(temp_path, "clickhouse/docs-release")

    test_output = os.path.join(temp_path, "docs_release_log")
    if not os.path.exists(test_output):
        os.makedirs(test_output)

    if args.as_root:
        user = "0:0"
    else:
        user = f"{os.geteuid()}:{os.getegid()}"

    run_log_path = os.path.join(test_output, "runlog.log")

    with SSHKey("ROBOT_CLICKHOUSE_SSH_KEY"):
        cmd = (
            f"docker run --cap-add=SYS_PTRACE --user={user} "
            f"--volume='{os.getenv('SSH_AUTH_SOCK', '')}:/ssh-agent' "
            f"--volume={repo_path}:/repo_path --volume={test_output}:/output_path "
            f"-e SSH_AUTH_SOCK=/ssh-agent -e EXTRA_BUILD_ARGS='--verbose' "
            f"-e CLOUDFLARE_TOKEN={CLOUDFLARE_TOKEN} {docker_image}"
        )
        logging.info("Running command: %s", cmd)
        with TeePopen(cmd, run_log_path) as process:
            retcode = process.wait()
            if retcode == 0:
                logging.info("Run successfully")
                status = "success"
                description = "Released successfuly"
            else:
                description = "Release failed (non zero exit code)"
                status = "failure"
                logging.info("Run failed")

    subprocess.check_call(f"sudo chown -R ubuntu:ubuntu {temp_path}", shell=True)
    files = os.listdir(test_output)
    lines = []
    additional_files = []
    if not files:
        logging.error("No output files after docs release")
        description = "No output files after docs release"
        status = "failure"
    else:
        for f in files:
            path = os.path.join(test_output, f)
            additional_files.append(path)
            with open(path, "r", encoding="utf-8") as check_file:
                for line in check_file:
                    if "ERROR" in line:
                        lines.append((line.split(":")[-1], "FAIL"))
        if lines:
            status = "failure"
            description = "Found errors in docs"
        elif status != "failure":
            lines.append(("No errors found", "OK"))
        else:
            lines.append(("Non zero exit code", "FAIL"))

    s3_helper = S3Helper("https://s3.amazonaws.com")

    report_url = upload_results(
        s3_helper, pr_info.number, pr_info.sha, lines, additional_files, NAME
    )
    print("::notice ::Report url: {report_url}")
    commit = get_commit(gh, pr_info.sha)
    commit.create_status(
        context=NAME, description=description, state=status, target_url=report_url
    )

    if status == "failure":
        sys.exit(1)
