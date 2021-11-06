#!/usr/bin/env python3

import os
import logging
import sys
import json
import time
import subprocess
import csv

from github import Github
import requests

from report import create_test_html_report
from s3_helper import S3Helper
from get_robot_token import get_best_robot_token
from pr_info import PRInfo

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

    return my_env

def dowload_build_with_progress(url, path):
    logging.info("Downloading from %s to temp path %s", url, path)
    for i in range(DOWNLOAD_RETRIES_COUNT):
        try:
            with open(path, 'wb') as f:
                response = requests.get(url, stream=True)
                response.raise_for_status()
                total_length = response.headers.get('content-length')
                if total_length is None or int(total_length) == 0:
                    logging.info("No content-length, will download file without progress")
                    f.write(response.content)
                else:
                    dl = 0
                    total_length = int(total_length)
                    logging.info("Content length is %ld bytes", total_length)
                    for data in response.iter_content(chunk_size=4096):
                        dl += len(data)
                        f.write(data)
                        if sys.stdout.isatty():
                            done = int(50 * dl / total_length)
                            percent = int(100 * float(dl) / total_length)
                            eq_str = '=' * done
                            space_str = ' ' * (50 - done)
                            sys.stdout.write(f"\r[{eq_str}{space_str}] {percent}%")
                            sys.stdout.flush()
            break
        except Exception as ex:
            sys.stdout.write("\n")
            time.sleep(3)
            logging.info("Exception while downloading %s, retry %s", ex, i + 1)
            if os.path.exists(path):
                os.remove(path)
    else:
        raise Exception(f"Cannot download dataset from {url}, all retries exceeded")

    sys.stdout.write("\n")
    logging.info("Downloading finished")

def get_build_urls(build_config_str, reports_path):
    for root, _, files in os.walk(reports_path):
        for f in files:
            if build_config_str in f :
                logging.info("Found build report json %s", f)
                with open(os.path.join(root, f), 'r', encoding='utf-8') as file_handler:
                    build_report = json.load(file_handler)
                    return build_report['build_urls']
    return []

def get_build_config(build_number, repo_path):
    ci_config_path = os.path.join(repo_path, "tests/ci/ci_config.json")
    with open(ci_config_path, 'r', encoding='utf-8') as ci_config:
        config_dict = json.load(ci_config)
        return config_dict['build_config'][build_number]

def build_config_to_string(build_config):
    if build_config["package-type"] == "performance":
        return "performance"

    return "_".join([
        build_config['compiler'],
        build_config['build-type'] if build_config['build-type'] else "relwithdebuginfo",
        build_config['sanitizer'] if build_config['sanitizer'] else "none",
        build_config['bundled'],
        build_config['splitted'],
        "tidy" if build_config['tidy'] == "enable" else "notidy",
        "with_coverage" if build_config['with_coverage'] else "without_coverage",
        build_config['package-type'],
    ])

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

def download_builds(result_path, build_urls):
    for url in build_urls:
        if url.endswith('.deb'):
            fname = os.path.basename(url.replace('%2B', '+').replace('%20', ' '))
            logging.info("Will download %s to %s", fname, result_path)
            dowload_build_with_progress(url, os.path.join(result_path, fname))


def get_commit(gh, commit_sha):
    repo = gh.get_repo(os.getenv("GITHUB_REPOSITORY", "ClickHouse/ClickHouse"))
    commit = repo.get_commit(commit_sha)
    return commit

def process_logs(s3_client, additional_logs, s3_path_prefix):
    additional_urls = []
    for log_path in additional_logs:
        if log_path:
            additional_urls.append(
                s3_client.upload_test_report_to_s3(
                    log_path,
                    s3_path_prefix + "/" + os.path.basename(log_path)))

    return additional_urls

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

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    temp_path = os.getenv("TEMP_PATH", os.path.abspath("."))
    repo_path = os.getenv("REPO_COPY", os.path.abspath("../../"))
    reports_path = os.getenv("REPORTS_PATH", "./reports")

    check_name = sys.argv[1]
    build_number = int(sys.argv[2])

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

    build_config = get_build_config(build_number, repo_path)
    build_config_str = build_config_to_string(build_config)
    urls = get_build_urls(build_config_str, reports_path)
    if not urls:
        raise Exception("No build URLs found")

    build_path = os.path.join(temp_path, "build")
    if not os.path.exists(build_path):
        os.makedirs(build_path)

    download_builds(build_path, urls)

    my_env = get_env_for_runner(build_path, repo_path, result_path, work_path)

    json_path = os.path.join(work_path, 'params.json')
    with open(json_path, 'w', encoding='utf-8') as json_params:
        json_params.write(json.dumps(get_json_params_dict(check_name, pr_info.sha, pr_info.number, images_with_version)))

    output_path_log = os.path.join(result_path, "main_script_log.txt")

    runner_path = os.path.join(repo_path, "tests/integration", "ci-runner.py")
    run_command = f"sudo {runner_path} | tee {output_path_log}"

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
    report_url = upload_results(s3_helper, pr_info.number, pr_info.sha, test_results, output_path_log, additional_logs, check_name)
    print(f"::notice ::Report url: {report_url}")
    commit = get_commit(gh, pr_info.sha)
    commit.create_status(context=check_name, description=description, state=state, target_url=report_url)
