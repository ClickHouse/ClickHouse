#!/usr/bin/env python3

import os
import logging
import json
import sys
import subprocess
import time

from github import Github

from s3_helper import S3Helper
from get_robot_token import get_best_robot_token
from pr_info import PRInfo
from build_download_helper import download_shared_build
from upload_result_helper import upload_results
from docker_pull_helper import get_image_with_version

DOCKER_IMAGE = "clickhouse/split-build-smoke-test"
DOWNLOAD_RETRIES_COUNT = 5
RESULT_LOG_NAME = "run.log"
CHECK_NAME = 'Split build smoke test (actions)'

def process_result(result_folder, server_log_folder):
    status = "success"
    description = 'Server started and responded'
    summary = [("Smoke test", "OK")]
    with open(os.path.join(result_folder, RESULT_LOG_NAME), 'r') as run_log:
        lines = run_log.read().split('\n')
        if not lines or lines[0].strip() != 'OK':
            status = "failure"
            logging.info("Lines is not ok: %s", str('\n'.join(lines)))
            summary = [("Smoke test", "FAIL")]
            description = 'Server failed to respond, see result in logs'

    result_logs = []
    server_log_path = os.path.join(server_log_folder, "clickhouse-server.log")
    stderr_log_path = os.path.join(result_folder, "stderr.log")
    client_stderr_log_path = os.path.join(result_folder, "clientstderr.log")
    run_log_path = os.path.join(result_folder, RESULT_LOG_NAME)

    for path in [server_log_path, stderr_log_path, client_stderr_log_path, run_log_path]:
        if os.path.exists(path):
            result_logs.append(path)

    return status, description, summary, result_logs

def get_run_command(build_path, result_folder, server_log_folder, docker_image):
    return f"docker run --network=host --volume={build_path}:/package_folder" \
           f" --volume={server_log_folder}:/var/log/clickhouse-server" \
           f" --volume={result_folder}:/test_output" \
           f" {docker_image} >{result_folder}/{RESULT_LOG_NAME}"


def get_commit(gh, commit_sha):
    repo = gh.get_repo(os.getenv("GITHUB_REPOSITORY", "ClickHouse/ClickHouse"))
    commit = repo.get_commit(commit_sha)
    return commit

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    temp_path = os.getenv("TEMP_PATH", os.path.abspath("."))
    repo_path = os.getenv("REPO_COPY", os.path.abspath("../../"))
    reports_path = os.getenv("REPORTS_PATH", "./reports")

    build_number = int(sys.argv[1])

    with open(os.getenv('GITHUB_EVENT_PATH'), 'r', encoding='utf-8') as event_file:
        event = json.load(event_file)

    pr_info = PRInfo(event)

    gh = Github(get_best_robot_token())

    for root, _, files in os.walk(reports_path):
        for f in files:
            if f == 'changed_images.json':
                images_path = os.path.join(root, 'changed_images.json')
                break

    docker_image = get_image_with_version(reports_path, DOCKER_IMAGE)

    packages_path = os.path.join(temp_path, "packages")
    if not os.path.exists(packages_path):
        os.makedirs(packages_path)

    download_shared_build(CHECK_NAME, reports_path, packages_path)

    server_log_path = os.path.join(temp_path, "server_log")
    if not os.path.exists(server_log_path):
        os.makedirs(server_log_path)

    result_path = os.path.join(temp_path, "result_path")
    if not os.path.exists(result_path):
        os.makedirs(result_path)

    run_command = get_run_command(packages_path, result_path, server_log_path, docker_image)

    logging.info("Going to run command %s", run_command)
    with subprocess.Popen(run_command, shell=True) as process:
        retcode = process.wait()
        if retcode == 0:
            logging.info("Run successfully")
        else:
            logging.info("Run failed")

    subprocess.check_call(f"sudo chown -R ubuntu:ubuntu {temp_path}", shell=True)
    print("Result path", os.listdir(result_path))
    print("Server log path", os.listdir(server_log_path))

    state, description, test_results, additional_logs = process_result(result_path, server_log_path)

    s3_helper = S3Helper('https://s3.amazonaws.com')
    report_url = upload_results(s3_helper, pr_info.number, pr_info.sha, test_results, additional_logs, CHECK_NAME)
    print(f"::notice ::Report url: {report_url}")
    commit = get_commit(gh, pr_info.sha)
    commit.create_status(context=CHECK_NAME, description=description, state=state, target_url=report_url)
