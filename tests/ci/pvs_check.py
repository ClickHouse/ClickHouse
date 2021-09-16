#!/usr/bin/env python3
import subprocess
import os
import json
import logging
from github import Github
from report import create_test_html_report
from s3_helper import S3Helper
from pr_info import PRInfo
import shutil
import sys

NAME = 'PVS Studio (actions)'
LICENSE_NAME = 'Free license: ClickHouse, Yandex'
HTML_REPORT_FOLDER = 'pvs-studio-html-report'
TXT_REPORT_NAME = 'pvs-studio-task-report.txt'

def process_logs(s3_client, additional_logs, s3_path_prefix):
    additional_urls = []
    for log_path in additional_logs:
        if log_path:
            additional_urls.append(
                s3_client.upload_test_report_to_s3(
                    log_path,
                    s3_path_prefix + "/" + os.path.basename(log_path)))

    return additional_urls

def _process_txt_report(self, path):
    warnings = []
    errors = []
    with open(path, 'r') as report_file:
        for line in report_file:
            if 'viva64' in line:
                continue
            elif 'warn' in line:
                warnings.append(':'.join(line.split('\t')[0:2]))
            elif 'err' in line:
                errors.append(':'.join(line.split('\t')[0:2]))
    return warnings, errors

def get_commit(gh, commit_sha):
    repo = gh.get_repo(os.getenv("GITHUB_REPOSITORY", "ClickHouse/ClickHouse"))
    commit = repo.get_commit(commit_sha)
    return commit

def upload_results(s3_client, pr_number, commit_sha, test_results, additional_files):
    s3_path_prefix = str(pr_number) + "/" + commit_sha + "/" + NAME.lower().replace(' ', '_')
    additional_urls = process_logs(s3_client, additional_files, s3_path_prefix)

    branch_url = "https://github.com/ClickHouse/ClickHouse/commits/master"
    branch_name = "master"
    if pr_number != 0:
        branch_name = "PR #{}".format(pr_number)
        branch_url = "https://github.com/ClickHouse/ClickHouse/pull/" + str(pr_number)
    commit_url = f"https://github.com/ClickHouse/ClickHouse/commit/{commit_sha}"

    task_url = f"https://github.com/ClickHouse/ClickHouse/actions/runs/{os.getenv('GITHUB_RUN_ID')}"

    raw_log_url = additional_urls[0]
    additional_urls.pop(0)

    html_report = create_test_html_report(NAME, test_results, raw_log_url, task_url, branch_url, branch_name, commit_url, additional_urls)
    with open('report.html', 'w') as f:
        f.write(html_report)

    url = s3_client.upload_test_report_to_s3('report.html', s3_path_prefix + ".html")
    logging.info("Search result in url %s", url)
    return url


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    repo_path = os.getenv("GITHUB_WORKSPACE", os.path.abspath("../../"))
    temp_path = os.path.join(os.getenv("RUNNER_TEMP", os.path.abspath("./temp")), 'pvs_check')

    with open(os.getenv('GITHUB_EVENT_PATH'), 'r') as event_file:
        event = json.load(event_file)
    pr_info = PRInfo(event)

    if not os.path.exists(temp_path):
        os.makedirs(temp_path)

    aws_secret_key_id = os.getenv("YANDEX_S3_ACCESS_KEY_ID", "")
    aws_secret_key = os.getenv("YANDEX_S3_ACCESS_SECRET_KEY", "")

    gh = Github(os.getenv("GITHUB_TOKEN"))

    images_path = os.path.join(temp_path, 'changed_images.json')
    docker_image = 'clickhouse/pvs-test'
    if os.path.exists(images_path):
        logging.info("Images file exists")
        with open(images_path, 'r') as images_fd:
            images = json.load(images_fd)
            logging.info("Got images %s", images)
            if 'clickhouse/pvs-test' in images:
                docker_image += ':' + images['clickhouse/pvs-test']

    logging.info("Got docker image %s", docker_image)

    if not aws_secret_key_id  or not aws_secret_key:
        logging.info("No secrets, will not upload anything to S3")

    s3_helper = S3Helper('https://storage.yandexcloud.net', aws_access_key_id=aws_secret_key_id, aws_secret_access_key=aws_secret_key)

    licence_key = os.getenv('PVS_STUDIO_KEY')
    cmd = f"docker run --volume={repo_path}:/repo_folder --volume={temp_path}:/test_output -e LICENSE_NAME='{LICENSE_NAME}' -e LICENCE_KEY='{licence_key}' -e CC=clang-11 -e CXX=clang++-11 {docker_image}"

    subprocess.check_output(cmd, shell=True)

    s3_path_prefix = str(pr_info.number) + "/" + pr_info.sha + "/" + NAME.lower().replace(' ', '_')
    html_urls = self.s3_client.upload_test_folder_to_s3(os.path.join(temp_path, HTML_REPORT_FOLDER), s3_path_prefix)
    index_html = None

    commit = get_commit(gh, pr_info.sha)
    for url in html_urls:
        if 'index.html' in url:
            index_html = '<a href="{}">HTML report</a>'.format(url)
            break

        if not index_html:
            commit.create_status(context=NAME, description='PVS report failed to build', state='failure', target_url=f"https://github.com/ClickHouse/ClickHouse/actions/runs/{os.getenv('GITHUB_RUN_ID')}")
            sys.exit(1)

    txt_report = os.path.join(temp_path, TXT_REPORT_NAME)
    warnings, errors = _process_txt_report(txt_report)
    errors = errors + warnings

    status = 'success'
    test_results = [(index_html, "Look at the report"), ("Errors count not checked", "OK")]
    description = "Total errors {}".format(len(errors))
    additional_logs = [txt_report, os.path.join(temp_path, 'pvs-studio.log')]
    report_url = upload_results(s3_helper, pr_info.number, pr_info.sha, test_results, additional_logs)

    print("::notice ::Report url: {}".format(report_url))
    commit = get_commit(gh, pr_info.sha)
    commit.create_status(context=NAME, description=description, state=status, target_url=report_url)
