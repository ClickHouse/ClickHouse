#!/usr/bin/env python3


import os
import logging
import sys
import json
import subprocess
import traceback
import re

from github import Github

from env_helper import GITHUB_RUN_URL
from pr_info import PRInfo
from s3_helper import S3Helper
from get_robot_token import get_best_robot_token
from docker_pull_helper import get_image_with_version
from commit_status_helper import get_commit, post_commit_status
from tee_popen import TeePopen
from rerun_helper import RerunHelper

IMAGE_NAME = "clickhouse/performance-comparison"


def get_run_command(
    workspace,
    result_path,
    repo_tests_path,
    pr_to_test,
    sha_to_test,
    additional_env,
    image,
):
    return (
        f"docker run --privileged --volume={workspace}:/workspace --volume={result_path}:/output "
        f"--volume={repo_tests_path}:/usr/share/clickhouse-test "
        f"--cap-add syslog --cap-add sys_admin --cap-add sys_rawio "
        f"-e PR_TO_TEST={pr_to_test} -e SHA_TO_TEST={sha_to_test} {additional_env} "
        f"{image}"
    )


class RamDrive:
    def __init__(self, path, size):
        self.path = path
        self.size = size

    def __enter__(self):
        if not os.path.exists(self.path):
            os.makedirs(self.path)

        subprocess.check_call(
            f"sudo mount -t tmpfs -o rw,size={self.size} tmpfs {self.path}", shell=True
        )

    def __exit__(self, exc_type, exc_val, exc_tb):
        subprocess.check_call(f"sudo umount {self.path}", shell=True)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    temp_path = os.getenv("TEMP_PATH", os.path.abspath("."))
    repo_path = os.getenv("REPO_COPY", os.path.abspath("../../"))
    repo_tests_path = os.path.join(repo_path, "tests")
    ramdrive_path = os.getenv("RAMDRIVE_PATH", os.path.join(temp_path, "ramdrive"))
    # currently unused, doesn't make tests more stable
    ramdrive_size = os.getenv("RAMDRIVE_SIZE", "0G")
    reports_path = os.getenv("REPORTS_PATH", "./reports")

    check_name = sys.argv[1]

    if not os.path.exists(temp_path):
        os.makedirs(temp_path)

    with open(os.getenv("GITHUB_EVENT_PATH"), "r", encoding="utf-8") as event_file:
        event = json.load(event_file)

    gh = Github(get_best_robot_token())
    pr_info = PRInfo(event)
    commit = get_commit(gh, pr_info.sha)

    docker_env = ""

    docker_env += " -e S3_URL=https://s3.amazonaws.com/clickhouse-builds"

    if pr_info.number == 0:
        pr_link = commit.html_url
    else:
        pr_link = f"https://github.com/ClickHouse/ClickHouse/pull/{pr_info.number}"

    docker_env += (
        f' -e CHPC_ADD_REPORT_LINKS="<a href={GITHUB_RUN_URL}>'
        f'Job (actions)</a> <a href={pr_link}>Tested commit</a>"'
    )

    if "RUN_BY_HASH_TOTAL" in os.environ:
        run_by_hash_total = int(os.getenv("RUN_BY_HASH_TOTAL"))
        run_by_hash_num = int(os.getenv("RUN_BY_HASH_NUM"))
        docker_env += f" -e CHPC_TEST_RUN_BY_HASH_TOTAL={run_by_hash_total} -e CHPC_TEST_RUN_BY_HASH_NUM={run_by_hash_num}"
        check_name_with_group = (
            check_name + f" [{run_by_hash_num + 1}/{run_by_hash_total}]"
        )
    else:
        check_name_with_group = check_name

    rerun_helper = RerunHelper(gh, pr_info, check_name_with_group)
    if rerun_helper.is_already_finished_by_status():
        logging.info("Check is already finished according to github status, exiting")
        sys.exit(0)

    docker_image = get_image_with_version(reports_path, IMAGE_NAME)

    # with RamDrive(ramdrive_path, ramdrive_size):
    result_path = ramdrive_path
    if not os.path.exists(result_path):
        os.makedirs(result_path)

    run_command = get_run_command(
        result_path,
        result_path,
        repo_tests_path,
        pr_info.number,
        pr_info.sha,
        docker_env,
        docker_image,
    )
    logging.info("Going to run command %s", run_command)
    run_log_path = os.path.join(temp_path, "runlog.log")
    with TeePopen(run_command, run_log_path) as process:
        retcode = process.wait()
        if retcode == 0:
            logging.info("Run successfully")
        else:
            logging.info("Run failed")

    subprocess.check_call(f"sudo chown -R ubuntu:ubuntu {temp_path}", shell=True)

    paths = {
        "compare.log": os.path.join(result_path, "compare.log"),
        "output.7z": os.path.join(result_path, "output.7z"),
        "report.html": os.path.join(result_path, "report.html"),
        "all-queries.html": os.path.join(result_path, "all-queries.html"),
        "queries.rep": os.path.join(result_path, "queries.rep"),
        "all-query-metrics.tsv": os.path.join(
            result_path, "report/all-query-metrics.tsv"
        ),
        "runlog.log": run_log_path,
    }

    check_name_prefix = (
        check_name_with_group.lower()
        .replace(" ", "_")
        .replace("(", "_")
        .replace(")", "_")
        .replace(",", "_")
    )
    s3_prefix = f"{pr_info.number}/{pr_info.sha}/{check_name_prefix}/"
    s3_helper = S3Helper("https://s3.amazonaws.com")
    for file in paths:
        try:
            paths[file] = s3_helper.upload_test_report_to_s3(
                paths[file], s3_prefix + file
            )
        except Exception:
            paths[file] = ""
            traceback.print_exc()

    # Upload all images and flamegraphs to S3
    try:
        s3_helper.upload_test_folder_to_s3(
            os.path.join(result_path, "images"), s3_prefix + "images"
        )
    except Exception:
        traceback.print_exc()

    # Try to fetch status from the report.
    status = ""
    message = ""
    try:
        report_text = open(os.path.join(result_path, "report.html"), "r").read()
        status_match = re.search("<!--[ ]*status:(.*)-->", report_text)
        message_match = re.search("<!--[ ]*message:(.*)-->", report_text)
        if status_match:
            status = status_match.group(1).strip()
        if message_match:
            message = message_match.group(1).strip()

        # TODO: Remove me, always green mode for the first time
        status = "success"
    except Exception:
        traceback.print_exc()
        status = "failure"
        message = "Failed to parse the report."

    if not status:
        status = "failure"
        message = "No status in report."
    elif not message:
        status = "failure"
        message = "No message in report."

    report_url = GITHUB_RUN_URL

    if paths["runlog.log"]:
        report_url = paths["runlog.log"]

    if paths["compare.log"]:
        report_url = paths["compare.log"]

    if paths["output.7z"]:
        report_url = paths["output.7z"]

    if paths["report.html"]:
        report_url = paths["report.html"]

    post_commit_status(
        gh, pr_info.sha, check_name_with_group, message, status, report_url
    )
