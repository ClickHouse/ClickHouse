#!/usr/bin/env python3

import logging
import subprocess
import os
import json
import time
import sys

from github import Github
import requests

from s3_helper import S3Helper
from get_robot_token import get_best_robot_token
from pr_info import PRInfo


DOWNLOAD_RETRIES_COUNT = 5
IMAGE_NAME = 'clickhouse/fuzzer'

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
    build_number = int(sys.argv[2])

    if not os.path.exists(temp_path):
        os.makedirs(temp_path)

    with open(os.getenv('GITHUB_EVENT_PATH'), 'r', encoding='utf-8') as event_file:
        event = json.load(event_file)

    pr_info = PRInfo(event)

    gh = Github(get_best_robot_token())

    images_path = os.path.join(temp_path, 'changed_images.json')

    docker_image = IMAGE_NAME
    if os.path.exists(images_path):
        logging.info("Images file exists")
        with open(images_path, 'r', encoding='utf-8') as images_fd:
            images = json.load(images_fd)
            logging.info("Got images %s", images)
            if IMAGE_NAME in images:
                docker_image += ':' + images[IMAGE_NAME]

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

    report_url = f"https://github.com/ClickHouse/ClickHouse/actions/runs/{os.getenv('GITHUB_RUN_ID')}"
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
    commit = get_commit(gh, pr_info.sha)
    commit.create_status(context=check_name, description=description, state=status, target_url=report_url)
