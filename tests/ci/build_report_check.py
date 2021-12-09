#!/usr/bin/env python3

import json
import logging
import os
import sys
from github import Github
from report import create_build_html_report
from s3_helper import S3Helper
from get_robot_token import get_best_robot_token
from pr_info import PRInfo, get_event
from commit_status_helper import  get_commit
from ci_config import CI_CONFIG
from rerun_helper import RerunHelper

class BuildResult():
    def __init__(self, compiler, build_type, sanitizer, bundled, splitted, status, elapsed_seconds, with_coverage):
        self.compiler = compiler
        self.build_type = build_type
        self.sanitizer = sanitizer
        self.bundled = bundled
        self.splitted = splitted
        self.status = status
        self.elapsed_seconds = elapsed_seconds
        self.with_coverage = with_coverage

def group_by_artifacts(build_urls):
    groups = {'deb': [], 'binary': [], 'tgz': [], 'rpm': [], 'preformance': []}
    for url in build_urls:
        if url.endswith('performance.tgz'):
            groups['performance'].append(url)
        elif url.endswith('.deb') or url.endswith('.buildinfo') or url.endswith('.changes') or url.endswith('.tar.gz'):
            groups['deb'].append(url)
        elif url.endswith('.rpm'):
            groups['rpm'].append(url)
        elif url.endswith('.tgz'):
            groups['tgz'].append(url)
        else:
            groups['binary'].append(url)
    return groups

def process_report(build_report):
    build_config = build_report['build_config']
    build_result = BuildResult(
        compiler=build_config['compiler'],
        build_type=build_config['build_type'],
        sanitizer=build_config['sanitizer'],
        bundled=build_config['bundled'],
        splitted=build_config['splitted'],
        status="success" if build_report['status'] else "failure",
        elapsed_seconds=build_report['elapsed_seconds'],
        with_coverage=False
    )
    build_results = []
    build_urls = []
    build_logs_urls = []
    urls_groups = group_by_artifacts(build_report['build_urls'])
    found_group = False
    for _, group_urls in urls_groups.items():
        if group_urls:
            build_results.append(build_result)
            build_urls.append(group_urls)
            build_logs_urls.append(build_report['log_url'])
            found_group = True

    if not found_group:
        build_results.append(build_result)
        build_urls.append([""])
        build_logs_urls.append(build_report['log_url'])

    return build_results, build_urls, build_logs_urls

def get_build_name_from_file_name(file_name):
    return file_name.replace('build_urls_', '').replace('.json', '')

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    reports_path = os.getenv("REPORTS_PATH", "./reports")
    temp_path = os.path.join(os.getenv("TEMP_PATH", "."))
    logging.info("Reports path %s", reports_path)

    if not os.path.exists(temp_path):
        os.makedirs(temp_path)

    build_check_name = sys.argv[1]

    gh = Github(get_best_robot_token())
    pr_info = PRInfo(get_event())
    rerun_helper = RerunHelper(gh, pr_info, build_check_name)
    if rerun_helper.is_already_finished_by_status():
        logging.info("Check is already finished according to github status, exiting")
        sys.exit(0)

    reports_order = CI_CONFIG["builds_report_config"][build_check_name]
    logging.info("My reports list %s", reports_order)

    build_reports_map = {}
    for root, dirs, files in os.walk(reports_path):
        for f in files:
            if f.startswith("build_urls_") and f.endswith('.json'):
                logging.info("Found build report json %s", f)
                build_name = get_build_name_from_file_name(f)
                if build_name in reports_order:
                    with open(os.path.join(root, f), 'r') as file_handler:
                        build_report = json.load(file_handler)
                        build_reports_map[build_name] = build_report
                else:
                    logging.info("Skipping report %s for build %s, it's not in our reports list", f, build_name)

    build_reports = [build_reports_map[build_name] for build_name in reports_order if build_name in build_reports_map]

    build_results = []
    build_artifacts = []
    build_logs = []

    for build_report in build_reports:
        build_result, build_artifacts_url, build_logs_url = process_report(build_report)
        logging.info("Got %s result for report", len(build_result))
        build_results += build_result
        build_artifacts += build_artifacts_url
        build_logs += build_logs_url

    logging.info("Totally got %s results", len(build_results))
    if len(build_results) == 0:
        logging.info("No builds, failing check")
        sys.exit(1)

    s3_helper = S3Helper('https://s3.amazonaws.com')

    pr_info = PRInfo(get_event())

    branch_url = f"{os.getenv('GITHUB_SERVER_URL')}/{os.getenv('GITHUB_REPOSITORY')}/commits/master"
    branch_name = "master"
    if pr_info.number != 0:
        branch_name = "PR #{}".format(pr_info.number)
        branch_url = f"{os.getenv('GITHUB_SERVER_URL')}/{os.getenv('GITHUB_REPOSITORY')}/pull/{pr_info.number}"
    commit_url = f"{os.getenv('GITHUB_SERVER_URL')}/{os.getenv('GITHUB_REPOSITORY')}/commit/{pr_info.sha}"
    task_url = f"{os.getenv('GITHUB_SERVER_URL')}/{os.getenv('GITHUB_REPOSITORY')}/actions/runs/{os.getenv('GITHUB_RUN_ID', '0')}"
    report = create_build_html_report(
        build_check_name,
        build_results,
        build_logs,
        build_artifacts,
        task_url,
        branch_url,
        branch_name,
        commit_url
    )

    report_path = os.path.join(temp_path, 'report.html')
    with open(report_path, 'w') as f:
        f.write(report)

    logging.info("Going to upload prepared report")
    context_name_for_path = build_check_name.lower().replace(' ', '_')
    s3_path_prefix = str(pr_info.number) + "/" + pr_info.sha + "/" + context_name_for_path

    url = s3_helper.upload_build_file_to_s3(report_path, s3_path_prefix + "/report.html")
    logging.info("Report url %s", url)

    total_builds = len(build_results)
    ok_builds = 0
    summary_status = "success"
    for build_result in build_results:
        if build_result.status == "failure" and summary_status != "error":
            summary_status = "failure"
        if build_result.status == "error" or not build_result.status:
            summary_status = "error"

        if build_result.status == "success":
            ok_builds += 1

    if ok_builds == 0:
        summary_status = "error"

    description = "{}/{} builds are OK".format(ok_builds, total_builds)

    print("::notice ::Report url: {}".format(url))

    commit = get_commit(gh, pr_info.sha)
    commit.create_status(context=build_check_name, description=description, state=summary_status, target_url=url)
