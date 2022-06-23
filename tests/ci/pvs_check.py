#!/usr/bin/env python3

# pylint: disable=line-too-long

import os
import json
import logging
import sys
from github import Github
from s3_helper import S3Helper
from pr_info import PRInfo, get_event
from get_robot_token import get_best_robot_token, get_parameter_from_ssm
from upload_result_helper import upload_results
from commit_status_helper import get_commit
from clickhouse_helper import ClickHouseHelper, prepare_tests_results_for_clickhouse
from stopwatch import Stopwatch
from rerun_helper import RerunHelper
from tee_popen import TeePopen

NAME = 'PVS Studio (actions)'
LICENCE_NAME = 'Free license: ClickHouse, Yandex'
HTML_REPORT_FOLDER = 'pvs-studio-html-report'
TXT_REPORT_NAME = 'pvs-studio-task-report.txt'

def _process_txt_report(path):
    warnings = []
    errors = []
    with open(path, 'r') as report_file:
        for line in report_file:
            if 'viva64' in line:
                continue

            if 'warn' in line:
                warnings.append(':'.join(line.split('\t')[0:2]))
            elif 'err' in line:
                errors.append(':'.join(line.split('\t')[0:2]))

    return warnings, errors

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    stopwatch = Stopwatch()

    repo_path = os.path.join(os.getenv("REPO_COPY", os.path.abspath("../../")))
    temp_path = os.path.join(os.getenv("TEMP_PATH"))

    pr_info = PRInfo(get_event())
    # this check modify repository so copy it to the temp directory
    logging.info("Repo copy path %s", repo_path)

    gh = Github(get_best_robot_token())
    rerun_helper = RerunHelper(gh, pr_info, NAME)
    if rerun_helper.is_already_finished_by_status():
        logging.info("Check is already finished according to github status, exiting")
        sys.exit(0)

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

    s3_helper = S3Helper('https://s3.amazonaws.com')

    licence_key = get_parameter_from_ssm('pvs_studio_key')
    cmd = f"docker run -u $(id -u ${{USER}}):$(id -g ${{USER}}) --volume={repo_path}:/repo_folder --volume={temp_path}:/test_output -e LICENCE_NAME='{LICENCE_NAME}' -e LICENCE_KEY='{licence_key}' {docker_image}"
    commit = get_commit(gh, pr_info.sha)

    run_log_path = os.path.join(temp_path, 'run_log.log')

    with TeePopen(cmd, run_log_path) as process:
        retcode = process.wait()
        if retcode != 0:
            logging.info("Run failed")
        else:
            logging.info("Run Ok")

    if retcode != 0:
        commit.create_status(context=NAME, description='PVS report failed to build', state='failure', target_url=f"https://github.com/ClickHouse/ClickHouse/actions/runs/{os.getenv('GITHUB_RUN_ID')}")
        sys.exit(1)

    try:
        s3_path_prefix = str(pr_info.number) + "/" + pr_info.sha + "/" + NAME.lower().replace(' ', '_')
        html_urls = s3_helper.upload_test_folder_to_s3(os.path.join(temp_path, HTML_REPORT_FOLDER), s3_path_prefix)
        index_html = None

        for url in html_urls:
            if 'index.html' in url:
                index_html = '<a href="{}">HTML report</a>'.format(url)
                break

        if not index_html:
            commit.create_status(context=NAME, description='PVS report failed to build', state='failure',
                                 target_url=f"{os.getenv('GITHUB_SERVER_URL')}/{os.getenv('GITHUB_REPOSITORY')}/actions/runs/{os.getenv('GITHUB_RUN_ID')}")
            sys.exit(1)

        txt_report = os.path.join(temp_path, TXT_REPORT_NAME)
        warnings, errors = _process_txt_report(txt_report)
        errors = errors + warnings

        status = 'success'
        test_results = [(index_html, "Look at the report"), ("Errors count not checked", "OK")]
        description = "Total errors {}".format(len(errors))
        additional_logs = [txt_report, os.path.join(temp_path, 'pvs-studio.log')]
        report_url = upload_results(s3_helper, pr_info.number, pr_info.sha, test_results, additional_logs, NAME)

        print("::notice ::Report url: {}".format(report_url))
        commit = get_commit(gh, pr_info.sha)
        commit.create_status(context=NAME, description=description, state=status, target_url=report_url)

        ch_helper = ClickHouseHelper()
        prepared_events = prepare_tests_results_for_clickhouse(pr_info, test_results, status, stopwatch.duration_seconds, stopwatch.start_time_str, report_url, NAME)
        ch_helper.insert_events_into(db="gh-data", table="checks", events=prepared_events)
    except Exception as ex:
        print("Got an exception", ex)
        sys.exit(1)
