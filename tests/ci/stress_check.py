#!/usr/bin/env python3

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

def get_run_command(build_path, result_folder, server_log_folder, image):
    cmd = "docker run -e S3_URL='https://clickhouse-datasets.s3.amazonaws.com' " + \
          f"--volume={build_path}:/package_folder "  \
          f"--volume={result_folder}:/test_output " \
          f"--volume={server_log_folder}:/var/log/clickhouse-server {image}"

    return cmd

def get_commit(gh, commit_sha):
    repo = gh.get_repo(os.getenv("GITHUB_REPOSITORY", "ClickHouse/ClickHouse"))
    commit = repo.get_commit(commit_sha)
    return commit

def process_results(result_folder, server_log_path, run_log_path):
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

    additional_files.append(run_log_path)

    status_path = os.path.join(result_folder, "check_status.tsv")
    if not os.path.exists(status_path):
        return "failure", "check_status.tsv doesn't exists", test_results, additional_files

    logging.info("Found check_status.tsv")
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

    image_name = "clickhouse/stress-test"

    docker_image = image_name
    if images_path and os.path.exists(images_path):
        logging.info("Images file exists")
        with open(images_path, 'r', encoding='utf-8') as images_fd:
            images = json.load(images_fd)
            logging.info("Got images %s", images)
            if image_name in images:
                docker_image += ':' + images[image_name]
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

    run_log_path = os.path.join(temp_path, "runlog.log")

    run_command = get_run_command(packages_path, result_path, server_log_path, docker_image)
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
    state, description, test_results, additional_logs = process_results(result_path, server_log_path, run_log_path)
    report_url = upload_results(s3_helper, pr_info.number, pr_info.sha, test_results, [run_log_path] + additional_logs, check_name)
    print(f"::notice ::Report url: {report_url}")
    commit = get_commit(gh, pr_info.sha)
    commit.create_status(context=check_name, description=description, state=state, target_url=report_url)
