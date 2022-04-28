#!/usr/bin/env python3


import os
import subprocess
import logging

from github import Github

from env_helper import IMAGES_PATH, REPO_COPY
from stopwatch import Stopwatch
from upload_result_helper import upload_results
from s3_helper import S3Helper
from get_robot_token import get_best_robot_token
from commit_status_helper import post_commit_status
from docker_pull_helper import get_image_with_version
from tee_popen import TeePopen

NAME = "Woboq Build (actions)"


def get_run_command(repo_path, output_path, image):
    cmd = (
        "docker run " + f"--volume={repo_path}:/repo_folder "
        f"--volume={output_path}:/test_output "
        f"-e 'DATA=https://s3.amazonaws.com/clickhouse-test-reports/codebrowser/data' {image}"
    )
    return cmd


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    stopwatch = Stopwatch()

    temp_path = os.getenv("TEMP_PATH", os.path.abspath("."))

    gh = Github(get_best_robot_token())

    if not os.path.exists(temp_path):
        os.makedirs(temp_path)

    docker_image = get_image_with_version(IMAGES_PATH, "altinityinfra/codebrowser")
    s3_helper = S3Helper("https://s3.amazonaws.com")

    result_path = os.path.join(temp_path, "result_path")
    if not os.path.exists(result_path):
        os.makedirs(result_path)

    run_command = get_run_command(REPO_COPY, result_path, docker_image)

    logging.info("Going to run codebrowser: %s", run_command)

    run_log_path = os.path.join(temp_path, "runlog.log")

    with TeePopen(run_command, run_log_path) as process:
        retcode = process.wait()
        if retcode == 0:
            logging.info("Run successfully")
        else:
            logging.info("Run failed")

    subprocess.check_call(f"sudo chown -R ubuntu:ubuntu {temp_path}", shell=True)

    report_path = os.path.join(result_path, "html_report")
    logging.info("Report path %s", report_path)
    s3_path_prefix = "codebrowser"
    html_urls = s3_helper.fast_parallel_upload_dir(
        report_path, s3_path_prefix, "clickhouse-test-reports"
    )

    index_html = '<a href="https://s3.amazonaws.com/clickhouse-test-reports/codebrowser/index.html">HTML report</a>'

    test_results = [(index_html, "Look at the report")]

    report_url = upload_results(
        s3_helper, 0, os.getenv("GITHUB_SHA"), test_results, [], NAME
    )

    print(f"::notice ::Report url: {report_url}")

    post_commit_status(
        gh, os.getenv("GITHUB_SHA"), NAME, "Report built", "success", report_url
    )
