#!/usr/bin/env python3

import os
import logging
import sys
import json
import time
import subprocess
import csv

from github import Github

from s3_helper import S3Helper
from get_robot_token import get_best_robot_token
from pr_info import PRInfo
from build_download_helper import download_all_deb_packages
from upload_result_helper import upload_results

DOWNLOAD_RETRIES_COUNT = 5

IMAGES = [
    "yandex/clickhouse-integration-tests-runner",
    "yandex/clickhouse-mysql-golang-client",
    "yandex/clickhouse-mysql-java-client",
    "yandex/clickhouse-mysql-js-client",
    "yandex/clickhouse-mysql-php-client",
    "yandex/clickhouse-postgresql-java-client",
    "yandex/clickhouse-integration-test",
    "yandex/clickhouse-kerberos-kdc",
    "yandex/clickhouse-integration-helper",
]

def get_json_params_dict(check_name, commit_sha, pr_number, docker_images):
    return {
        'context_name': check_name,
        'commit': commit_sha,
        'pull_request': pr_number,
        'pr_info': None,
        'docker_images_with_versions': docker_images,
        'shuffle_test_groups': False,
    }

def get_env_for_runner(build_path, repo_path, result_path, work_path):
    binary_path = os.path.join(build_path, 'clickhouse')
    odbc_bridge_path = os.path.join(build_path, 'clickhouse-odbc-bridge')
    library_bridge_path = os.path.join(build_path, 'clickhouse-library-bridge')

    my_env = os.environ.copy()
    my_env["CLICKHOUSE_TESTS_BUILD_PATH"] = build_path
    my_env["CLICKHOUSE_TESTS_SERVER_BIN_PATH"] = binary_path
    my_env["CLICKHOUSE_TESTS_CLIENT_BIN_PATH"] = binary_path
    my_env["CLICKHOUSE_TESTS_ODBC_BRIDGE_BIN_PATH"] = odbc_bridge_path
    my_env["CLICKHOUSE_TESTS_LIBRARY_BRIDGE_BIN_PATH"] = library_bridge_path
    my_env["CLICKHOUSE_TESTS_REPO_PATH"] = repo_path
    my_env["CLICKHOUSE_TESTS_RESULT_PATH"] = result_path
    my_env["CLICKHOUSE_TESTS_BASE_CONFIG_DIR"] = f"{repo_path}/programs/server"
    my_env["CLICKHOUSE_TESTS_JSON_PARAMS_PATH"] = os.path.join(work_path, "params.json")
    my_env["CLICKHOUSE_TESTS_RUNNER_RESTART_DOCKER"] = '0'

    return my_env

def get_images_with_versions(images_path):
    if os.path.exists(images_path):
        result = {}
        logging.info("Images file exists")
        with open(images_path, 'r', encoding='utf-8') as images_fd:
            images = json.load(images_fd)
            logging.info("Got images %s", images)
            for required_image in IMAGES:
                if required_image in images:
                    result[required_image] = images[required_image]
                else:
                    result[required_image] = 'latest'
        return result
    else:
        return {image: 'latest' for image in IMAGES}

def get_commit(gh, commit_sha):
    repo = gh.get_repo(os.getenv("GITHUB_REPOSITORY", "ClickHouse/ClickHouse"))
    commit = repo.get_commit(commit_sha)
    return commit

def process_results(result_folder):
    test_results = []
    additional_files = []
    # Just upload all files from result_folder.
    # If task provides processed results, then it's responsible for content of result_folder.
    if os.path.exists(result_folder):
        test_files = [f for f in os.listdir(result_folder) if os.path.isfile(os.path.join(result_folder, f))]
        additional_files = [os.path.join(result_folder, f) for f in test_files]

    status_path = os.path.join(result_folder, "check_status.tsv")
    if os.path.exists(status_path):
        logging.info("Found test_results.tsv")
        with open(status_path, 'r', encoding='utf-8') as status_file:
            status = list(csv.reader(status_file, delimiter='\t'))
    else:
        status = []

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

    images_path = os.path.join(temp_path, 'changed_images.json')
    images_with_version = get_images_with_versions(images_path)
    for image, version in images_with_version.items():
        docker_image = image + ':' + version
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

    result_path = os.path.join(temp_path, "output_dir")
    if not os.path.exists(result_path):
        os.makedirs(result_path)

    work_path = os.path.join(temp_path, "workdir")
    if not os.path.exists(work_path):
        os.makedirs(work_path)

    build_path = os.path.join(temp_path, "build")
    if not os.path.exists(build_path):
        os.makedirs(build_path)

    download_all_deb_packages(check_name, reports_path, build_path)

    my_env = get_env_for_runner(build_path, repo_path, result_path, work_path)

    json_path = os.path.join(work_path, 'params.json')
    with open(json_path, 'w', encoding='utf-8') as json_params:
        json_params.write(json.dumps(get_json_params_dict(check_name, pr_info.sha, pr_info.number, images_with_version)))

    output_path_log = os.path.join(result_path, "main_script_log.txt")

    runner_path = os.path.join(repo_path, "tests/integration", "ci-runner.py")
    run_command = f"sudo -E {runner_path} | tee {output_path_log}"

    with open(output_path_log, 'w', encoding='utf-8') as log:
        with subprocess.Popen(run_command, shell=True, stderr=log, stdout=log, env=my_env) as process:
            retcode = process.wait()
            if retcode == 0:
                logging.info("Run tests successfully")
            else:
                logging.info("Some tests failed")


    subprocess.check_call(f"sudo chown -R ubuntu:ubuntu {temp_path}", shell=True)

    state, description, test_results, additional_logs = process_results(result_path)

    s3_helper = S3Helper('https://s3.amazonaws.com')
    report_url = upload_results(s3_helper, pr_info.number, pr_info.sha, test_results, [output_path_log] + additional_logs, check_name, False)
    print(f"::notice ::Report url: {report_url}")
    commit = get_commit(gh, pr_info.sha)
    commit.create_status(context=check_name, description=description, state=state, target_url=report_url)
