#!/usr/bin/env python3
import logging
import subprocess
import os
import time
import json
import sys
from github import Github
from report import create_test_html_report
from s3_helper import S3Helper
from pr_info import PRInfo
from get_robot_token import get_best_robot_token

NAME = "Docs Check (actions)"

def process_logs(s3_client, additional_logs, s3_path_prefix):
    additional_urls = []
    for log_path in additional_logs:
        if log_path:
            additional_urls.append(
                s3_client.upload_test_report_to_s3(
                    log_path,
                    s3_path_prefix + "/" + os.path.basename(log_path)))

    return additional_urls

def upload_results(s3_client, pr_number, commit_sha, test_results, additional_files):
    s3_path_prefix = f"{pr_number}/{commit_sha}/docs_check"
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

    html_report = create_test_html_report(NAME, test_results, raw_log_url, task_url, branch_url, branch_name, commit_url, additional_urls)
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

    temp_path = os.path.join(os.getenv("TEMP_PATH"))
    repo_path = os.path.join(os.getenv("REPO_COPY"))

    with open(os.getenv('GITHUB_EVENT_PATH'), 'r', encoding='utf-8') as event_file:
        event = json.load(event_file)

    pr_info = PRInfo(event, need_changed_files=True)

    gh = Github(get_best_robot_token())
    if not pr_info.has_changes_in_documentation():
        logging.info ("No changes in documentation")
        commit = get_commit(gh, pr_info.sha)
        commit.create_status(context=NAME, description="No changes in docs", state="success")
        sys.exit(0)

    logging.info("Has changes in docs")

    if not os.path.exists(temp_path):
        os.makedirs(temp_path)

    images_path = os.path.join(temp_path, 'changed_images.json')

    docker_image = 'clickhouse/docs-check'
    if os.path.exists(images_path):
        logging.info("Images file exists")
        with open(images_path, 'r', encoding='utf-8') as images_fd:
            images = json.load(images_fd)
            logging.info("Got images %s", images)
            if 'clickhouse/docs-check' in images:
                docker_image += ':' + images['clickhouse/docs-check']

    logging.info("Got docker image %s", docker_image)
    for i in range(10):
        try:
            subprocess.check_output(f"docker pull {docker_image}", shell=True)
            break
        except Exception as ex:
            time.sleep(i * 3)
            logging.info("Got execption pulling docker %s", ex)
    else:
        raise Exception(f"Cannot pull dockerhub for image {docker_image}")

    test_output = os.path.join(temp_path, 'docs_check_log')
    if not os.path.exists(test_output):
        os.makedirs(test_output)

    cmd = f"docker run --cap-add=SYS_PTRACE --volume={repo_path}:/repo_path --volume={test_output}:/output_path {docker_image}"

    run_log_path = os.path.join(test_output, 'runlog.log')

    with open(run_log_path, 'w', encoding='utf-8') as log:
        with subprocess.Popen(cmd, shell=True, stderr=log, stdout=log) as process:
            retcode = process.wait()
            if retcode == 0:
                logging.info("Run successfully")
                status = "success"
                description = "Docs check passed"
            else:
                description = "Docs check failed (non zero exit code)"
                status = "failure"
                logging.info("Run failed")

    subprocess.check_call(f"sudo chown -R ubuntu:ubuntu {temp_path}", shell=True)
    files = os.listdir(test_output)
    lines = []
    additional_files = []
    if not files:
        logging.error("No output files after docs check")
        description = "No output files after docs check"
        status = "failure"
    else:
        for f in files:
            path = os.path.join(test_output, f)
            additional_files.append(path)
            with open(path, 'r', encoding='utf-8') as check_file:
                for line in check_file:
                    if "ERROR" in line:
                        lines.append((line.split(':')[-1], "FAIL"))
        if lines:
            status = "failure"
            description = "Found errors in docs"
        elif status != "failure":
            lines.append(("No errors found", "OK"))
        else:
            lines.append(("Non zero exit code", "FAIL"))

    s3_helper = S3Helper('https://s3.amazonaws.com')

    report_url = upload_results(s3_helper, pr_info.number, pr_info.sha, lines, additional_files)
    print("::notice ::Report url: {report_url}")
    commit = get_commit(gh, pr_info.sha)
    commit.create_status(context=NAME, description=description, state=status, target_url=report_url)
