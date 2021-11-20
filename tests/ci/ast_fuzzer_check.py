#!/usr/bin/env python3

import logging
import subprocess
import os
import json
import sys

from github import Github

from s3_helper import S3Helper
from get_robot_token import get_best_robot_token
from pr_info import PRInfo
from ci_config import build_config_to_string
from build_download_helper import get_build_config_for_check, get_build_urls
from docker_pull_helper import get_image_with_version
from commit_status_helper import post_commit_status

IMAGE_NAME = 'clickhouse/fuzzer'

def get_run_command(pr_number, sha, download_url, workspace_path, image):
    return f'docker run --network=host --volume={workspace_path}:/workspace ' \
          '--cap-add syslog --cap-add sys_admin ' \
          f'-e PR_TO_TEST={pr_number} -e SHA_TO_TEST={sha} -e BINARY_URL_TO_DOWNLOAD="{download_url}" '\
          f'{image}'

def get_commit(gh, commit_sha):
    repo = gh.get_repo(os.getenv("GITHUB_REPOSITORY", "ClickHouse/ClickHouse"))
    commit = repo.get_commit(commit_sha)
    return commit

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

    docker_image = get_image_with_version(temp_path, IMAGE_NAME)

    build_config = get_build_config_for_check(check_name)
    print(build_config)
    build_config_str = build_config_to_string(build_config)
    print(build_config_str)
    urls = get_build_urls(build_config_str, reports_path)
    if not urls:
        raise Exception("No build URLs found")

    for url in urls:
        if url.endswith('/clickhouse'):
            build_url = url
            break
    else:
        raise Exception("Cannot binary clickhouse among build results")

    logging.info("Got build url %s", build_url)

    workspace_path = os.path.join(temp_path, 'workspace')
    if not os.path.exists(workspace_path):
        os.makedirs(workspace_path)

    run_command = get_run_command(pr_info.number, pr_info.sha, build_url, workspace_path, docker_image)
    logging.info("Going to run %s", run_command)

    run_log_path = os.path.join(temp_path, "runlog.log")
    with open(run_log_path, 'w', encoding='utf-8') as log:
        with subprocess.Popen(run_command, shell=True, stderr=log, stdout=log) as process:
            retcode = process.wait()
            if retcode == 0:
                logging.info("Run successfully")
            else:
                logging.info("Run failed")

    subprocess.check_call(f"sudo chown -R ubuntu:ubuntu {temp_path}", shell=True)

    check_name_lower = check_name.lower().replace('(', '').replace(')', '').replace(' ', '')
    s3_prefix = f'{pr_info.number}/{pr_info.sha}/fuzzer_{check_name_lower}/'
    paths = {
        'runlog.log': run_log_path,
        'main.log': os.path.join(workspace_path, 'main.log'),
        'server.log': os.path.join(workspace_path, 'server.log'),
        'fuzzer.log': os.path.join(workspace_path, 'fuzzer.log'),
        'report.html': os.path.join(workspace_path, 'report.html'),
    }

    s3_helper = S3Helper('https://s3.amazonaws.com')
    for f in paths:
        try:
            paths[f] = s3_helper.upload_test_report_to_s3(paths[f], s3_prefix + '/' + f)
        except Exception as ex:
            logging.info("Exception uploading file %s text %s", f, ex)
            paths[f] = ''

    report_url = f"{os.getenv('GITHUB_SERVER_URL')}/{os.getenv('GITHUB_REPOSITORY')}/actions/runs/{os.getenv('GITHUB_RUN_ID')}"
    if paths['runlog.log']:
        report_url = paths['runlog.log']
    if paths['main.log']:
        report_url = paths['main.log']
    if paths['server.log']:
        report_url = paths['server.log']
    if paths['fuzzer.log']:
        report_url = paths['fuzzer.log']
    if paths['report.html']:
        report_url = paths['report.html']

    # Try to get status message saved by the fuzzer
    try:
        with open(os.path.join(workspace_path, 'status.txt'), 'r', encoding='utf-8') as status_f:
            status = status_f.readline().rstrip('\n')

        with open(os.path.join(workspace_path, 'description.txt'), 'r', encoding='utf-8') as desc_f:
            description = desc_f.readline().rstrip('\n')[:140]
    except:
        status = 'failure'
        description = 'Task failed: $?=' + str(retcode)

    logging.info("Result: '%s', '%s', '%s'", status, description, report_url)
    print(f"::notice ::Report url: {report_url}")
    post_commit_status(gh, pr_info.sha, check_name, description, status, report_url)
