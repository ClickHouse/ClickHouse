#!/usr/bin/env python3


import os
import logging
import sys
import json
import subprocess
import traceback
import re
from typing import Dict

from github import Github

from commit_status_helper import get_commit, post_commit_status
from ci_config import CI_CONFIG
from docker_pull_helper import get_image_with_version
from env_helper import GITHUB_EVENT_PATH, GITHUB_RUN_URL, S3_BUILDS_BUCKET, S3_URL
from get_robot_token import get_best_robot_token, get_parameter_from_ssm
from pr_info import PRInfo
from rerun_helper import RerunHelper
from s3_helper import S3Helper
from tee_popen import TeePopen

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
        f"docker run --privileged --volume={workspace}:/workspace "
        f"--volume={result_path}:/output "
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
    required_build = CI_CONFIG["tests_config"][check_name]["required_build"]

    if not os.path.exists(temp_path):
        os.makedirs(temp_path)

    with open(GITHUB_EVENT_PATH, "r", encoding="utf-8") as event_file:
        event = json.load(event_file)

    gh = Github(get_best_robot_token(), per_page=100)
    pr_info = PRInfo(event)
    commit = get_commit(gh, pr_info.sha)

    docker_env = ""

    docker_env += f" -e S3_URL={S3_URL}/{S3_BUILDS_BUCKET}"
    docker_env += f" -e BUILD_NAME={required_build}"

    if pr_info.number == 0:
        pr_link = commit.html_url
    else:
        pr_link = f"https://github.com/ClickHouse/ClickHouse/pull/{pr_info.number}"

    docker_env += (
        f' -e CHPC_ADD_REPORT_LINKS="<a href={GITHUB_RUN_URL}>'
        f'Job (actions)</a> <a href={pr_link}>Tested commit</a>"'
    )

    if "RUN_BY_HASH_TOTAL" in os.environ:
        run_by_hash_total = int(os.getenv("RUN_BY_HASH_TOTAL", "1"))
        run_by_hash_num = int(os.getenv("RUN_BY_HASH_NUM", "1"))
        docker_env += (
            f" -e CHPC_TEST_RUN_BY_HASH_TOTAL={run_by_hash_total}"
            f" -e CHPC_TEST_RUN_BY_HASH_NUM={run_by_hash_num}"
        )
        check_name_with_group = (
            check_name + f" [{run_by_hash_num + 1}/{run_by_hash_total}]"
        )
    else:
        check_name_with_group = check_name

    test_grep_exclude_filter = CI_CONFIG["tests_config"][check_name][
        "test_grep_exclude_filter"
    ]
    if test_grep_exclude_filter:
        docker_env += f" -e CHPC_TEST_GREP_EXCLUDE={test_grep_exclude_filter}"
        logging.info(
            "Fill fliter our performance tests by grep -v %s", test_grep_exclude_filter
        )

    rerun_helper = RerunHelper(gh, pr_info, check_name_with_group)
    if rerun_helper.is_already_finished_by_status():
        logging.info("Check is already finished according to github status, exiting")
        sys.exit(0)

    check_name_prefix = (
        check_name_with_group.lower()
        .replace(" ", "_")
        .replace("(", "_")
        .replace(")", "_")
        .replace(",", "_")
    )

    docker_image = get_image_with_version(reports_path, IMAGE_NAME)

    # with RamDrive(ramdrive_path, ramdrive_size):
    result_path = ramdrive_path
    if not os.path.exists(result_path):
        os.makedirs(result_path)

    docker_env += (
        " -e CLICKHOUSE_PERFORMANCE_COMPARISON_DATABASE_URL"
        " -e CLICKHOUSE_PERFORMANCE_COMPARISON_DATABASE_USER"
        " -e CLICKHOUSE_PERFORMANCE_COMPARISON_DATABASE_USER_PASSWORD"
    )

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

    popen_env = os.environ.copy()

    database_url = get_parameter_from_ssm("clickhouse-test-stat-url")
    database_username = get_parameter_from_ssm("clickhouse-test-stat-login")
    database_password = get_parameter_from_ssm("clickhouse-test-stat-password")

    popen_env.update(
        {
            "CLICKHOUSE_PERFORMANCE_COMPARISON_DATABASE_URL": f"{database_url}:9440",
            "CLICKHOUSE_PERFORMANCE_COMPARISON_DATABASE_USER": database_username,
            "CLICKHOUSE_PERFORMANCE_COMPARISON_DATABASE_USER_PASSWORD": database_password,
            "CLICKHOUSE_PERFORMANCE_COMPARISON_CHECK_NAME": check_name_with_group,
            "CLICKHOUSE_PERFORMANCE_COMPARISON_CHECK_NAME_PREFIX": check_name_prefix,
        }
    )

    run_log_path = os.path.join(temp_path, "runlog.log")
    with TeePopen(run_command, run_log_path, env=popen_env) as process:
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

    s3_prefix = f"{pr_info.number}/{pr_info.sha}/{check_name_prefix}/"
    s3_helper = S3Helper(S3_URL)
    uploaded = {}  # type: Dict[str, str]
    for name, path in paths.items():
        try:
            uploaded[name] = s3_helper.upload_test_report_to_s3(path, s3_prefix + name)
        except Exception:
            uploaded[name] = ""
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
        with open(
            os.path.join(result_path, "report.html"), "r", encoding="utf-8"
        ) as report_fd:
            report_text = report_fd.read()
            status_match = re.search("<!--[ ]*status:(.*)-->", report_text)
            message_match = re.search("<!--[ ]*message:(.*)-->", report_text)
        if status_match:
            status = status_match.group(1).strip()
        if message_match:
            message = message_match.group(1).strip()

        # TODO: Remove me, always green mode for the first time, unless errors
        status = "success"
        if "errors" in message:
            status = "failure"
        # TODO: Remove until here
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

    if uploaded["runlog.log"]:
        report_url = uploaded["runlog.log"]

    if uploaded["compare.log"]:
        report_url = uploaded["compare.log"]

    if uploaded["output.7z"]:
        report_url = uploaded["output.7z"]

    if uploaded["report.html"]:
        report_url = uploaded["report.html"]

    post_commit_status(
        gh, pr_info.sha, check_name_with_group, message, status, report_url
    )

    if status == "error":
        sys.exit(1)
