#!/usr/bin/env python3

import os
import logging
import json
import sys
import subprocess
import time

from github import Github
import requests

from report import create_test_html_report
from s3_helper import S3Helper
from get_robot_token import get_best_robot_token
from pr_info import PRInfo

DOCKER_IMAGE = "clickhouse/split-build-smoke-test"
DOWNLOAD_RETRIES_COUNT = 5
RESULT_LOG_NAME = "result.log"
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
    stderr_log_path = os.path.join(server_log_folder, "stderr.log")
    client_stderr_log_path = os.path.join(server_log_folder, "clientstderr.log")

    if os.path.exists(server_log_path):
        result_logs.append(server_log_path)

    if os.path.exists(stderr_log_path):
        result_logs.append(stderr_log_path)

    if os.path.exists(client_stderr_log_path):
        result_logs.append(client_stderr_log_path)

    return status, description, summary, result_logs

def process_(log_path):
    name = os.path.basename(log_path)
    with open(log_path, 'r') as log:
        line = log.read().split('\n')[0].strip()
        if line != 'OK':
            return (name, "FAIL")
        else:
            return (name, "OK")



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


def download_builds(result_path, build_urls):
    for url in build_urls:
        if url.endswith('.tgz'):
            fname = os.path.basename(url)
            logging.info("Will download %s to %s", fname, result_path)
            dowload_build_with_progress(url, os.path.join(result_path, fname))

def get_build_config(build_number, repo_path):
    ci_config_path = os.path.join(repo_path, "tests/ci/ci_config.json")
    with open(ci_config_path, 'r', encoding='utf-8') as ci_config:
        config_dict = json.load(ci_config)
        return config_dict['special_build_config'][build_number]

def get_build_urls(build_config_str, reports_path):
    for root, _, files in os.walk(reports_path):
        for f in files:
            if build_config_str in f :
                logging.info("Found build report json %s", f)
                with open(os.path.join(root, f), 'r', encoding='utf-8') as file_handler:
                    build_report = json.load(file_handler)
                    return build_report['build_urls']
    return []

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

def get_run_command(build_path, result_folder, server_log_folder, docker_image):
    return f"docker run --network=host --volume={build_path}:/package_folder" \
           f" --volume={server_log_folder}:/var/log/clickhouse-server" \
           f" --volume={result_folder}:/test_output" \
           f" {docker_image} >{result_folder}/{RESULT_LOG_NAME}"

def process_logs(s3_client, additional_logs, s3_path_prefix):
    additional_urls = []
    for log_path in additional_logs:
        if log_path:
            additional_urls.append(
                s3_client.upload_test_report_to_s3(
                    log_path,
                    s3_path_prefix + "/" + os.path.basename(log_path)))

    return additional_urls

def upload_results(s3_client, pr_number, commit_sha, test_results, additional_files, check_name):
    s3_path_prefix = f"{pr_number}/{commit_sha}/" + check_name.lower().replace(' ', '_').replace('(', '_').replace(')', '_').replace(',', '_')
    additional_urls = process_logs(s3_client, additional_files, s3_path_prefix)

    branch_url = "https://github.com/ClickHouse/ClickHouse/commits/master"
    branch_name = "master"
    if pr_number != 0:
        branch_name = f"PR #{pr_number}"
        branch_url = f"https://github.com/ClickHouse/ClickHouse/pull/{pr_number}"
    commit_url = f"https://github.com/ClickHouse/ClickHouse/commit/{commit_sha}"

    task_url = f"https://github.com/ClickHouse/ClickHouse/actions/runs/{os.getenv('GITHUB_RUN_ID')}"

    if additional_urls:
        raw_log_url = additional_urls[0]
        additional_urls.pop(0)
    else:
        raw_log_url = task_url

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

    docker_image = DOCKER_IMAGE
    if images_path and os.path.exists(images_path):
        logging.info("Images file exists")
        with open(images_path, 'r', encoding='utf-8') as images_fd:
            images = json.load(images_fd)
            logging.info("Got images %s", images)
            if docker_image in images:
                docker_image += ':' + images[docker_image]
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

    build_config = get_build_config(build_number, repo_path)
    build_config_str = build_config_to_string(build_config)
    urls = get_build_urls(build_config_str, reports_path)
    if not urls:
        raise Exception("No build URLs found")

    packages_path = os.path.join(temp_path, "packages")
    if not os.path.exists(packages_path):
        os.makedirs(packages_path)

    download_builds(packages_path, urls)

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
