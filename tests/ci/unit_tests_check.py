#!/usr/bin/env python3

import logging
import os
import sys
import time
import subprocess
import json

from github import Github

from report import create_test_html_report
from s3_helper import S3Helper
from get_robot_token import get_best_robot_token
from pr_info import PRInfo
from build_download_helper import download_unit_tests

IMAGE_NAME = 'clickhouse/unit-test'

DOWNLOAD_RETRIES_COUNT = 5

def process_logs(s3_client, additional_logs, s3_path_prefix):
    additional_urls = []
    for log_path in additional_logs:
        if log_path:
            additional_urls.append(
                s3_client.upload_test_report_to_s3(
                    log_path,
                    s3_path_prefix + "/" + os.path.basename(log_path)))

    return additional_urls

def upload_results(s3_client, pr_number, commit_sha, test_results, raw_log, additional_files, check_name):
    additional_files = [raw_log] + additional_files
    s3_path_prefix = f"{pr_number}/{commit_sha}/" + check_name.lower().replace(' ', '_').replace('(', '_').replace(')', '_').replace(',', '_')
    additional_urls = process_logs(s3_client, additional_files, s3_path_prefix)

    branch_url = "https://github.com/ClickHouse/ClickHouse/commits/master"
    branch_name = "master"
    if pr_number != 0:
        branch_name = f"PR #{pr_number}"
        branch_url = f"https://github.com/ClickHouse/ClickHouse/pull/{pr_number}"
    commit_url = f"https://github.com/ClickHouse/ClickHouse/commit/{commit_sha}"

    task_url = f"https://github.com/ClickHouse/ClickHouse/actions/runs/{os.getenv('GITHUB_RUN_ID')}"

    raw_log_url = additional_urls[0]
    additional_urls.pop(0)

    html_report = create_test_html_report(check_name, test_results, raw_log_url, task_url, branch_url, branch_name, commit_url, additional_urls, True)
    with open('report.html', 'w', encoding='utf-8') as f:
        f.write(html_report)

    url = s3_client.upload_test_report_to_s3('report.html', s3_path_prefix + ".html")
    logging.info("Search result in url %s", url)
    return url

def get_commit(gh, commit_sha):
    repo = gh.get_repo(os.getenv("GITHUB_REPOSITORY", "ClickHouse/ClickHouse"))
    commit = repo.get_commit(commit_sha)
    return commit

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

    for root, _, files in os.walk(reports_path):
        for f in files:
            if f == 'changed_images.json':
                images_path = os.path.join(root, 'changed_images.json')
                break

    docker_image = IMAGE_NAME
    if images_path and os.path.exists(images_path):
        logging.info("Images file exists")
        with open(images_path, 'r', encoding='utf-8') as images_fd:
            images = json.load(images_fd)
            logging.info("Got images %s", images)
            if IMAGE_NAME in images:
                docker_image += ':' + images[IMAGE_NAME]
    else:
        logging.info("Images file not found")

    for i in range(10):
        try:
            logging.info("Pulling image %s", docker_image)
            subprocess.check_output(f"docker pull {docker_image}", stderr=subprocess.STDOUT, shell=True)
            break
        except Exception as ex:
            time.sleep(i * 3)
            logging.info("Got execption pulling docker %s", ex)
    else:
        raise Exception(f"Cannot pull dockerhub for image docker pull {docker_image}")

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
    report_url = upload_results(s3_helper, pr_info.number, pr_info.sha, test_results, run_log_path, additional_logs, check_name)
    print(f"::notice ::Report url: {report_url}")
    commit = get_commit(gh, pr_info.sha)
    commit.create_status(context=check_name, description=description, state=state, target_url=report_url)
