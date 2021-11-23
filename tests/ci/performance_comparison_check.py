#!/usr/bin/env python3


import os
import logging
import sys
import json
import subprocess
import traceback
import re

from github import Github

from pr_info import PRInfo
from s3_helper import S3Helper
from ci_config import build_config_to_string
from get_robot_token import get_best_robot_token
from docker_pull_helper import get_image_with_version
from commit_status_helper import get_commit, post_commit_status
from build_download_helper import get_build_config_for_check, get_build_urls

IMAGE_NAME = 'clickhouse/performance-comparison'

def get_run_command(workspace, result_path, pr_to_test, sha_to_test, additional_env, image):
    return f"docker run --privileged --volume={workspace}:/workspace --volume={result_path}:/output " \
        f"--cap-add syslog --cap-add sys_admin --cap-add sys_rawio " \
        f"-e PR_TO_TEST={pr_to_test} -e SHA_TO_TEST={sha_to_test} {additional_env} " \
        f"{image}"

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

    gh = Github(get_best_robot_token())
    pr_info = PRInfo(event)
    commit = get_commit(gh, pr_info.sha)

    build_config = get_build_config_for_check(check_name)
    print(build_config)
    build_config_str = build_config_to_string(build_config)
    print(build_config_str)
    urls = get_build_urls(build_config_str, reports_path)
    if not urls:
        raise Exception("No build URLs found")

    for url in urls:
        if url.endswith('/performance.tgz'):
            build_url = url
            break
    else:
        raise Exception("Cannot binary clickhouse among build results")

    docker_env = ''
    if pr_info.number != 0 and 'force tests' in pr_info.labels:
        # Run all perf tests if labeled 'force tests'.
        docker_env += ' -e CHPC_MAX_QUERIES=0 '

    docker_env += " -e S3_URL=https://s3.amazonaws.com/clickhouse-builds"

    if pr_info.number == 0:
        pr_link = commit.html_url
    else:
        pr_link = f"https://github.com/ClickHouse/ClickHouse/pull/{pr_info.number}"

    task_url = f"https://github.com/ClickHouse/ClickHouse/actions/runs/{os.getenv('GITHUB_RUN_ID')}"
    docker_env += ' -e CHPC_ADD_REPORT_LINKS="<a href={}>Job (actions)</a> <a href={}>Tested commit</a>"'.format(
        task_url, pr_link)

    docker_image = get_image_with_version(reports_path, IMAGE_NAME)

    result_path = os.path.join(temp_path, 'result_path')
    if not os.path.exists(result_path):
        os.makedirs(result_path)

    run_command = get_run_command(result_path, result_path, pr_info.number, pr_info.sha, docker_env, docker_image)
    logging.info("Going to run command %s", run_command)
    run_log_path = os.path.join(temp_path, "runlog.log")
    with open(run_log_path, 'w', encoding='utf-8') as log:
        with subprocess.Popen(run_command, shell=True, stderr=log, stdout=log) as process:
            retcode = process.wait()
            if retcode == 0:
                logging.info("Run successfully")
            else:
                logging.info("Run failed")

    subprocess.check_call(f"sudo chown -R ubuntu:ubuntu {temp_path}", shell=True)

    s3_prefix = f'{pr_info.number}/{pr_info.sha}/performance_comparison/'
    paths = {
        'compare.log': 'compare.log',
        'output.7z': 'output.7z',
        'report.html': 'report.html',
        'all-queries.html': 'all-queries.html',
        'queries.rep': 'queries.rep',
        'all-query-metrics.tsv': 'report/all-query-metrics.tsv',
    }

    s3_helper = S3Helper('https://s3.amazonaws.com')
    for file in paths:
        try:
            paths[file] = s3_helper.upload_test_report_to_s3(
                os.path.join(result_path, paths[file]),
                s3_prefix + file)
        except Exception:
            paths[file] = ''
            traceback.print_exc()

    # Upload all images and flamegraphs to S3
    try:
        s3_helper.upload_test_folder_to_s3(
            os.path.join(result_path, 'images'),
            s3_prefix + 'images'
        )
    except Exception:
        traceback.print_exc()

    # Try to fetch status from the report.
    status = ''
    message = ''
    try:
        report_text = open(os.path.join(result_path, 'report.html'), 'r').read()
        status_match = re.search('<!--[ ]*status:(.*)-->', report_text)
        message_match = re.search('<!--[ ]*message:(.*)-->', report_text)
        if status_match:
            status = status_match.group(1).strip()
        if message_match:
            message = message_match.group(1).strip()
    except Exception:
        traceback.print_exc()
        status = 'failure'
        message = 'Failed to parse the report.'

    if not status:
        status = 'failure'
        message = 'No status in report.'
    elif not message:
        status = 'failure'
        message = 'No message in report.'

    report_url = task_url

    if paths['compare.log']:
        report_url = paths['compare.log']

    if paths['output.7z']:
        report_url = paths['output.7z']

    if paths['report.html']:
        report_url = paths['report.html']

    post_commit_status(gh, pr_info.sha, check_name, message, status, report_url)
