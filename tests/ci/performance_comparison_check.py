#!/usr/bin/env python3

import json
import logging
import os
import re
import subprocess
import sys
import traceback
from pathlib import Path

from github import Github

from build_download_helper import download_builds_filter
from ci_config import CI
from clickhouse_helper import get_instance_id, get_instance_type
from commit_status_helper import get_commit
from docker_images_helper import get_docker_image, pull_image
from env_helper import (
    GITHUB_EVENT_PATH,
    GITHUB_RUN_URL,
    REPO_COPY,
    REPORT_PATH,
    S3_BUILDS_BUCKET,
    S3_DOWNLOAD,
    TEMP_PATH,
)
from get_robot_token import get_best_robot_token, get_parameter_from_ssm
from pr_info import PRInfo
from report import FAILURE, SUCCESS, JobReport
from stopwatch import Stopwatch
from tee_popen import TeePopen

IMAGE_NAME = "clickhouse/performance-comparison"


def get_run_command(
    check_start_time,
    check_name,
    workspace,
    result_path,
    repo_tests_path,
    pr_to_test,
    sha_to_test,
    additional_env,
    image,
):
    instance_type = get_instance_type()
    instance_id = get_instance_id()

    envs = [
        f"-e CHECK_START_TIME='{check_start_time}'",
        f"-e CHECK_NAME='{check_name}'",
        f"-e INSTANCE_TYPE='{instance_type}'",
        f"-e INSTANCE_ID='{instance_id}'",
        f"-e PR_TO_TEST={pr_to_test}",
        f"-e SHA_TO_TEST={sha_to_test}",
    ]

    env_str = " ".join(envs)

    return (
        f"docker run --privileged --volume={workspace}:/workspace "
        f"--volume={result_path}:/output "
        f"--volume={repo_tests_path}:/usr/share/clickhouse-test "
        f"--volume={TEMP_PATH}:/artifacts "
        f"--cap-add syslog --cap-add sys_admin --cap-add sys_rawio "
        f"{env_str} {additional_env} "
        f"{image}"
    )


def main():
    logging.basicConfig(level=logging.INFO)

    stopwatch = Stopwatch()

    temp_path = Path(TEMP_PATH)
    temp_path.mkdir(parents=True, exist_ok=True)
    repo_tests_path = Path(REPO_COPY, "tests")

    check_name = sys.argv[1] if len(sys.argv) > 1 else os.getenv("CHECK_NAME")
    assert (
        check_name
    ), "Check name must be provided as an input arg or in CHECK_NAME env"
    required_build = CI.JOB_CONFIGS[check_name].get_required_build()

    with open(GITHUB_EVENT_PATH, "r", encoding="utf-8") as event_file:
        event = json.load(event_file)

    gh = Github(get_best_robot_token(), per_page=100)
    pr_info = PRInfo(event)
    commit = get_commit(gh, pr_info.sha)

    docker_env = ""

    docker_env += f" -e S3_URL={S3_DOWNLOAD}/{S3_BUILDS_BUCKET}"
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

    is_aarch64 = "aarch64" in os.getenv("CHECK_NAME", "Performance Comparison").lower()
    if pr_info.number != 0 and is_aarch64 and "pr-performance" not in pr_info.labels:
        print("Skipped, not labeled with 'pr-performance'")
        sys.exit(0)

    check_name_prefix = (
        check_name_with_group.lower()
        .replace(" ", "_")
        .replace("(", "_")
        .replace(")", "_")
        .replace(",", "_")
        .replace("/", "_")
    )

    docker_image = pull_image(get_docker_image(IMAGE_NAME))

    result_path = temp_path / "result"
    result_path.mkdir(parents=True, exist_ok=True)

    database_url = get_parameter_from_ssm("clickhouse-test-stat-url")
    database_username = get_parameter_from_ssm("clickhouse-test-stat-login")
    database_password = get_parameter_from_ssm("clickhouse-test-stat-password")

    env_extra = {
        "CLICKHOUSE_PERFORMANCE_COMPARISON_DATABASE_URL": f"{database_url}:9440",
        "CLICKHOUSE_PERFORMANCE_COMPARISON_DATABASE_USER": database_username,
        "CLICKHOUSE_PERFORMANCE_COMPARISON_DATABASE_USER_PASSWORD": database_password,
        "CLICKHOUSE_PERFORMANCE_COMPARISON_CHECK_NAME": check_name_with_group,
        "CLICKHOUSE_PERFORMANCE_COMPARISON_CHECK_NAME_PREFIX": check_name_prefix,
    }

    download_builds_filter(
        check_name, REPORT_PATH, temp_path, lambda url: "performance.tar.zst" in url
    )
    assert os.path.exists(f"{TEMP_PATH}/performance.tar.zst"), "Perf artifact not found"

    docker_env += "".join([f" -e {name}" for name in env_extra])

    run_command = get_run_command(
        stopwatch.start_time_str,
        check_name,
        result_path,
        result_path,
        repo_tests_path,
        pr_info.number,
        pr_info.sha,
        docker_env,
        docker_image,
    )
    logging.info("Going to run command %s", run_command)

    run_log_path = temp_path / "run.log"
    compare_log_path = result_path / "compare.log"

    popen_env = os.environ.copy()
    popen_env.update(env_extra)
    with TeePopen(run_command, run_log_path, env=popen_env) as process:
        retcode = process.wait()
        if retcode == 0:
            logging.info("Run successfully")
        else:
            logging.info("Run failed")

    subprocess.check_call(f"sudo chown -R ubuntu:ubuntu {temp_path}", shell=True)

    def too_many_slow(msg):
        match = re.search(r"(|.* )(\d+) slower.*", msg)
        # This threshold should be synchronized with the value in
        # https://github.com/ClickHouse/ClickHouse/blob/master/docker/test/performance-comparison/report.py#L629
        threshold = 5
        return int(match.group(2).strip()) > threshold if match else False

    paths = {
        "compare.log": compare_log_path,
        "output.7z": result_path / "output.7z",
        "report.html": result_path / "report.html",
        "all-queries.html": result_path / "all-queries.html",
        "queries.rep": result_path / "queries.rep",
        "all-query-metrics.tsv": result_path / "report/all-query-metrics.tsv",
        "run.log": run_log_path,
    }

    # FIXME: where images come from? dir does not exist atm.
    image_files = (
        list((Path(result_path) / "images").iterdir())
        if (Path(result_path) / "images").exists()
        else []
    )

    # Try to fetch status from the report.
    status = ""
    message = ""
    try:
        with open(result_path / "report.html", "r", encoding="utf-8") as report_fd:
            report_text = report_fd.read()
            status_match = re.search("<!--[ ]*status:(.*)-->", report_text)
            message_match = re.search("<!--[ ]*message:(.*)-->", report_text)
        if status_match:
            status = status_match.group(1).strip()
        if message_match:
            message = message_match.group(1).strip()

        # TODO: Remove me, always green mode for the first time, unless errors
        status = SUCCESS
        if "errors" in message.lower() or too_many_slow(message.lower()):
            status = FAILURE
        # TODO: Remove until here
    except Exception:
        traceback.print_exc()
        status = FAILURE
        message = "Failed to parse the report."

    if not status:
        status = FAILURE
        message = "No status in report."
    elif not message:
        status = FAILURE
        message = "No message in report."

    JobReport(
        description=message,
        test_results=[],
        status=status,
        start_time=stopwatch.start_time_str,
        duration=stopwatch.duration_seconds,
        additional_files=[v for _, v in paths.items()] + image_files,
        check_name=check_name_with_group,
    ).dump()

    if status != SUCCESS:
        sys.exit(1)


if __name__ == "__main__":
    main()
