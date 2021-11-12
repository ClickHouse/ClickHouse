#!/usr/bin/env python3

import logging
import os
import sys
import subprocess
import json

from github import Github

from s3_helper import S3Helper
from get_robot_token import get_best_robot_token
from pr_info import PRInfo
from build_download_helper import download_unit_tests
from upload_result_helper import upload_results
from docker_pull_helper import get_image_with_version
from commit_status_helper import post_commit_status

IMAGE_NAME = 'clickhouse/unit-test'

def get_test_name(line):
    elements = reversed(line.split(' '))
    for element in elements:
        if '(' not in element and ')' not in element:
            return element
    raise Exception(f"No test name in line '{line}'")

def process_result(result_folder):
    OK_SIGN = 'OK ]'
    FAILED_SIGN = 'FAILED  ]'
    SEGFAULT = 'Segmentation fault'
    SIGNAL = 'received signal SIG'
    PASSED = 'PASSED'

    summary = []
    total_counter = 0
    failed_counter = 0
    result_log_path = f'{result_folder}/test_result.txt'
    if not os.path.exists(result_log_path):
        logging.info("No output log on path %s", result_log_path)
        return "error", "No output log", summary, []

    status = "success"
    description = ""
    passed = False
    with open(result_log_path, 'r', encoding='utf-8') as test_result:
        for line in test_result:
            if OK_SIGN in line:
                logging.info("Found ok line: '%s'", line)
                test_name = get_test_name(line.strip())
                logging.info("Test name: '%s'", test_name)
                summary.append((test_name, "OK"))
                total_counter += 1
            elif FAILED_SIGN in line and 'listed below' not in line and 'ms)' in line:
                logging.info("Found fail line: '%s'", line)
                test_name = get_test_name(line.strip())
                logging.info("Test name: '%s'", test_name)
                summary.append((test_name, "FAIL"))
                total_counter += 1
                failed_counter += 1
            elif SEGFAULT in line:
                logging.info("Found segfault line: '%s'", line)
                status = "failure"
                description += "Segmentation fault. "
                break
            elif SIGNAL in line:
                logging.info("Received signal line: '%s'", line)
                status = "failure"
                description += "Exit on signal. "
                break
            elif PASSED in line:
                logging.info("PASSED record found: '%s'", line)
                passed = True

    if not passed:
        status = "failure"
        description += "PASSED record not found. "

    if failed_counter != 0:
        status = "failure"

    if not description:
        description += f"fail: {failed_counter}, passed: {total_counter - failed_counter}"

    return status, description, summary, [result_log_path]


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    temp_path = os.getenv("TEMP_PATH", os.path.abspath("."))
    repo_path = os.getenv("REPO_COPY", os.path.abspath("../../"))
    reports_path = os.getenv("REPORTS_PATH", "./reports")

    check_name = sys.argv[1]

    if not os.path.exists(temp_path):
        os.makedirs(temp_path)

    with open(os.getenv('GITHUB_EVENT_PATH'), 'r', encoding='utf-8') as event_file:
        event = json.load(event_file)

    pr_info = PRInfo(event)

    gh = Github(get_best_robot_token())

    docker_image = get_image_with_version(reports_path, IMAGE_NAME)

    download_unit_tests(check_name, reports_path, temp_path)
    tests_binary_path = os.path.join(temp_path, "unit_tests_dbms")
    os.chmod(tests_binary_path, 0o777)

    test_output = os.path.join(temp_path, "test_output")
    if not os.path.exists(test_output):
        os.makedirs(test_output)

    run_command = f"docker run --cap-add=SYS_PTRACE --volume={tests_binary_path}:/unit_tests_dbms --volume={test_output}:/test_output {docker_image}"

    run_log_path = os.path.join(test_output, "runlog.log")

    logging.info("Going to run func tests: %s", run_command)

    with open(run_log_path, 'w', encoding='utf-8') as log:
        with subprocess.Popen(run_command, shell=True, stderr=log, stdout=log) as process:
            retcode = process.wait()
            if retcode == 0:
                logging.info("Run successfully")
            else:
                logging.info("Run failed")

    subprocess.check_call(f"sudo chown -R ubuntu:ubuntu {temp_path}", shell=True)

    s3_helper = S3Helper('https://s3.amazonaws.com')
    state, description, test_results, additional_logs = process_result(test_output)
    report_url = upload_results(s3_helper, pr_info.number, pr_info.sha, test_results, [run_log_path] + additional_logs, check_name)
    print(f"::notice ::Report url: {report_url}")
    post_commit_status(gh, pr_info.sha, check_name, description, state, report_url)
