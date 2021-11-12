#!/usr/bin/env python3

import csv
import logging
import subprocess
import os
import json
import time
import sys

from github import Github

from s3_helper import S3Helper
from get_robot_token import get_best_robot_token
from pr_info import PRInfo
from build_download_helper import download_all_deb_packages
from upload_result_helper import upload_results
from docker_pull_helper import get_image_with_version


def get_commit(gh, commit_sha):
    repo = gh.get_repo(os.getenv("GITHUB_REPOSITORY", "ClickHouse/ClickHouse"))
    commit = repo.get_commit(commit_sha)
    return commit

def get_image_name(check_name):
    if 'stateless' in check_name.lower():
        return 'clickhouse/stateless-test'
    if 'stateful' in  check_name.lower():
        return 'clickhouse/stateful-test'
    else:
        raise Exception(f"Cannot deduce image name based on check name {check_name}")

def get_run_command(builds_path, result_path, server_log_path, kill_timeout, additional_envs, image, flaky_check, tests_to_run):
    additional_options = ['--hung-check']
    additional_options.append('--print-time')

    if tests_to_run:
        additional_options += tests_to_run

    additional_options_str = '-e ADDITIONAL_OPTIONS="' + ' '.join(additional_options) + '"'

    envs = [f'-e MAX_RUN_TIME={int(0.9 * kill_timeout)}', '-e S3_URL="https://clickhouse-datasets.s3.amazonaws.com"']

    if flaky_check:
        envs += ['-e NUM_TRIES=100', '-e MAX_RUN_TIME=1800']

    envs += [f'-e {e}' for e in additional_envs]

    env_str = ' '.join(envs)

    return f"docker run --volume={builds_path}:/package_folder " \
        f"--volume={result_path}:/test_output --volume={server_log_path}:/var/log/clickhouse-server " \
        f"--cap-add=SYS_PTRACE {env_str} {additional_options_str} {image}"


def get_tests_to_run(pr_info):
    result = set([])

    if pr_info.changed_files is None:
        return []

    for fpath in pr_info.changed_files:
        if 'tests/queries/0_stateless/0' in fpath:
            logging.info('File %s changed and seems like stateless test', fpath)
            fname = fpath.split('/')[3]
            fname_without_ext = os.path.splitext(fname)[0]
            result.add(fname_without_ext + '.')
    return list(result)

def process_results(result_folder, server_log_path):
    test_results = []
    additional_files = []
    # Just upload all files from result_folder.
    # If task provides processed results, then it's responsible for content of result_folder.
    if os.path.exists(result_folder):
        test_files = [f for f in os.listdir(result_folder) if os.path.isfile(os.path.join(result_folder, f))]
        additional_files = [os.path.join(result_folder, f) for f in test_files]

    if os.path.exists(server_log_path):
        server_log_files = [f for f in os.listdir(server_log_path) if os.path.isfile(os.path.join(server_log_path, f))]
        additional_files = additional_files + [os.path.join(server_log_path, f) for f in server_log_files]

    status_path = os.path.join(result_folder, "check_status.tsv")
    logging.info("Found test_results.tsv")
    with open(status_path, 'r', encoding='utf-8') as status_file:
        status = list(csv.reader(status_file, delimiter='\t'))

    if len(status) != 1 or len(status[0]) != 2:
        return "error", "Invalid check_status.tsv", test_results, additional_files
    state, description = status[0][0], status[0][1]

    results_path = os.path.join(result_folder, "test_results.tsv")
    with open(results_path, 'r', encoding='utf-8') as results_file:
        test_results = list(csv.reader(results_file, delimiter='\t'))
    if len(test_results) == 0:
        raise Exception("Empty results")

    return state, description, test_results, additional_files


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    temp_path = os.getenv("TEMP_PATH", os.path.abspath("."))
    repo_path = os.getenv("REPO_COPY", os.path.abspath("../../"))
    reports_path = os.getenv("REPORTS_PATH", "./reports")

    check_name = sys.argv[1]
    kill_timeout = int(sys.argv[2])
    flaky_check = 'flaky' in check_name.lower()

    if not os.path.exists(temp_path):
        os.makedirs(temp_path)

    with open(os.getenv('GITHUB_EVENT_PATH'), 'r', encoding='utf-8') as event_file:
        event = json.load(event_file)

    gh = Github(get_best_robot_token())
    pr_info = PRInfo(event, need_changed_files=flaky_check)
    tests_to_run = []
    if flaky_check:
        tests_to_run = get_tests_to_run(pr_info)
        if not tests_to_run:
            commit = get_commit(gh, pr_info.sha)
            commit.create_status(context=check_name, description='Not found changed stateless tests', state='success')
            sys.exit(0)


    image_name = get_image_name(check_name)
    docker_image = get_image_with_version(reports_path, image_name)

    packages_path = os.path.join(temp_path, "packages")
    if not os.path.exists(packages_path):
        os.makedirs(packages_path)

    download_all_deb_packages(check_name, reports_path, packages_path)

    server_log_path = os.path.join(temp_path, "server_log")
    if not os.path.exists(server_log_path):
        os.makedirs(server_log_path)

    result_path = os.path.join(temp_path, "result_path")
    if not os.path.exists(result_path):
        os.makedirs(result_path)

    run_log_path = os.path.join(result_path, "runlog.log")

    run_command = get_run_command(packages_path, result_path, server_log_path, kill_timeout, [], docker_image, flaky_check, tests_to_run)
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
    state, description, test_results, additional_logs = process_results(result_path, server_log_path)
    report_url = upload_results(s3_helper, pr_info.number, pr_info.sha, test_results, [run_log_path] + additional_logs, check_name)
    print(f"::notice ::Report url: {report_url}")
    commit = get_commit(gh, pr_info.sha)
    commit.create_status(context=check_name, description=description, state=state, target_url=report_url)
