#!/usr/bin/env python3

import json
import logging
import os
import sys
import atexit
from typing import Dict, List, Tuple

from github import Github

from env_helper import (
    GITHUB_REPOSITORY,
    GITHUB_RUN_URL,
    GITHUB_SERVER_URL,
    REPORTS_PATH,
    TEMP_PATH,
)
from report import create_build_html_report
from s3_helper import S3Helper
from get_robot_token import get_best_robot_token
from pr_info import PRInfo
from commit_status_helper import (
    get_commit,
    update_mergeable_check,
)
from ci_config import CI_CONFIG
from rerun_helper import RerunHelper


NEEDS_DATA_PATH = os.getenv("NEEDS_DATA_PATH")


class BuildResult:
    def __init__(
        self,
        compiler,
        build_type,
        sanitizer,
        bundled,
        splitted,
        status,
        elapsed_seconds,
        with_coverage,
    ):
        self.compiler = compiler
        self.build_type = build_type
        self.sanitizer = sanitizer
        self.bundled = bundled
        self.splitted = splitted
        self.status = status
        self.elapsed_seconds = elapsed_seconds
        self.with_coverage = with_coverage


def group_by_artifacts(build_urls: List[str]) -> Dict[str, List[str]]:
    groups = {
        "apk": [],
        "deb": [],
        "binary": [],
        "tgz": [],
        "rpm": [],
        "performance": [],
    }  # type: Dict[str, List[str]]
    for url in build_urls:
        if url.endswith("performance.tgz"):
            groups["performance"].append(url)
        elif (
            url.endswith(".deb")
            or url.endswith(".buildinfo")
            or url.endswith(".changes")
            or url.endswith(".tar.gz")
        ):
            groups["deb"].append(url)
        elif url.endswith(".apk"):
            groups["apk"].append(url)
        elif url.endswith(".rpm"):
            groups["rpm"].append(url)
        elif url.endswith(".tgz"):
            groups["tgz"].append(url)
        else:
            groups["binary"].append(url)
    return groups


def get_failed_report(
    job_name: str,
) -> Tuple[List[BuildResult], List[List[str]], List[str]]:
    message = f"{job_name} failed"
    build_result = BuildResult(
        compiler="unknown",
        build_type="unknown",
        sanitizer="unknown",
        bundled="unknown",
        splitted="unknown",
        status=message,
        elapsed_seconds=0,
        with_coverage=False,
    )
    return [build_result], [[""]], [GITHUB_RUN_URL]


def process_report(
    build_report,
) -> Tuple[List[BuildResult], List[List[str]], List[str]]:
    build_config = build_report["build_config"]
    build_result = BuildResult(
        compiler=build_config["compiler"],
        build_type=build_config["build_type"],
        sanitizer=build_config["sanitizer"],
        bundled=build_config["bundled"],
        splitted=build_config["splitted"],
        status="success" if build_report["status"] else "failure",
        elapsed_seconds=build_report["elapsed_seconds"],
        with_coverage=False,
    )
    build_results = []
    build_urls = []
    build_logs_urls = []
    urls_groups = group_by_artifacts(build_report["build_urls"])
    found_group = False
    for _, group_urls in urls_groups.items():
        if group_urls:
            build_results.append(build_result)
            build_urls.append(group_urls)
            build_logs_urls.append(build_report["log_url"])
            found_group = True

    # No one group of urls is found, a failed report
    if not found_group:
        build_results.append(build_result)
        build_urls.append([""])
        build_logs_urls.append(build_report["log_url"])

    return build_results, build_urls, build_logs_urls


def get_build_name_from_file_name(file_name):
    return file_name.replace("build_urls_", "").replace(".json", "")


def main():
    logging.basicConfig(level=logging.INFO)
    temp_path = TEMP_PATH
    logging.info("Reports path %s", REPORTS_PATH)

    if not os.path.exists(temp_path):
        os.makedirs(temp_path)

    build_check_name = sys.argv[1]
    needs_data = None
    required_builds = 0
    if os.path.exists(NEEDS_DATA_PATH):
        with open(NEEDS_DATA_PATH, "rb") as file_handler:
            needs_data = json.load(file_handler)
            required_builds = len(needs_data)

    if needs_data is not None and all(
        i["result"] == "skipped" for i in needs_data.values()
    ):
        logging.info("All builds are skipped, exiting")
        sys.exit(0)

    logging.info("The next builds are required: %s", ", ".join(needs_data))

    gh = Github(get_best_robot_token(), per_page=100)
    pr_info = PRInfo()

    atexit.register(update_mergeable_check, gh, pr_info, build_check_name)

    rerun_helper = RerunHelper(gh, pr_info, build_check_name)
    if rerun_helper.is_already_finished_by_status():
        logging.info("Check is already finished according to github status, exiting")
        sys.exit(0)

    builds_for_check = CI_CONFIG["builds_report_config"][build_check_name]
    required_builds = required_builds or len(builds_for_check)

    # Collect reports from json artifacts
    builds_report_map = {}
    for root, _, files in os.walk(REPORTS_PATH):
        for f in files:
            if f.startswith("build_urls_") and f.endswith(".json"):
                logging.info("Found build report json %s", f)
                build_name = get_build_name_from_file_name(f)
                if build_name in builds_for_check:
                    with open(os.path.join(root, f), "rb") as file_handler:
                        builds_report_map[build_name] = json.load(file_handler)
                else:
                    logging.info(
                        "Skipping report %s for build %s, it's not in our reports list",
                        f,
                        build_name,
                    )

    # Sort reports by config order
    build_reports = [
        builds_report_map[build_name]
        for build_name in builds_for_check
        if build_name in builds_report_map
    ]

    some_builds_are_missing = len(build_reports) < required_builds
    missing_build_names = []
    if some_builds_are_missing:
        logging.warning(
            "Expected to get %s build results, got only %s",
            required_builds,
            len(build_reports),
        )
        missing_build_names = [
            name
            for name in needs_data
            if not any(rep for rep in build_reports if rep["job_name"] == name)
        ]
    else:
        logging.info("Got exactly %s builds", len(builds_report_map))

    # Group build artifacts by groups
    build_results = []  # type: List[BuildResult]
    build_artifacts = []  #
    build_logs = []

    for build_report in build_reports:
        build_result, build_artifacts_url, build_logs_url = process_report(build_report)
        logging.info(
            "Got %s artifact groups for build report report", len(build_result)
        )
        build_results.extend(build_result)
        build_artifacts.extend(build_artifacts_url)
        build_logs.extend(build_logs_url)

    for failed_job in missing_build_names:
        build_result, build_artifacts_url, build_logs_url = get_failed_report(
            failed_job
        )
        build_results.extend(build_result)
        build_artifacts.extend(build_artifacts_url)
        build_logs.extend(build_logs_url)

    total_groups = len(build_results)
    logging.info("Totally got %s artifact groups", total_groups)
    if total_groups == 0:
        logging.error("No success builds, failing check")
        sys.exit(1)

    s3_helper = S3Helper("https://s3.amazonaws.com")

    branch_url = f"{GITHUB_SERVER_URL}/{GITHUB_REPOSITORY}/commits/master"
    branch_name = "master"
    if pr_info.number != 0:
        branch_name = f"PR #{pr_info.number}"
        branch_url = f"{GITHUB_SERVER_URL}/{GITHUB_REPOSITORY}/pull/{pr_info.number}"
    commit_url = f"{GITHUB_SERVER_URL}/{GITHUB_REPOSITORY}/commit/{pr_info.sha}"
    task_url = GITHUB_RUN_URL
    report = create_build_html_report(
        build_check_name,
        build_results,
        build_logs,
        build_artifacts,
        task_url,
        branch_url,
        branch_name,
        commit_url,
    )

    report_path = os.path.join(temp_path, "report.html")
    with open(report_path, "w", encoding="utf-8") as fd:
        fd.write(report)

    logging.info("Going to upload prepared report")
    context_name_for_path = build_check_name.lower().replace(" ", "_")
    s3_path_prefix = (
        str(pr_info.number) + "/" + pr_info.sha + "/" + context_name_for_path
    )

    url = s3_helper.upload_build_file_to_s3(
        report_path, s3_path_prefix + "/report.html"
    )
    logging.info("Report url %s", url)
    print(f"::notice ::Report url: {url}")

    # Prepare a commit status
    ok_groups = 0
    summary_status = "success"
    for build_result in build_results:
        if build_result.status == "failure" and summary_status != "error":
            summary_status = "failure"
        if build_result.status == "error" or not build_result.status:
            summary_status = "error"

        if build_result.status == "success":
            ok_groups += 1

    if ok_groups == 0 or some_builds_are_missing:
        summary_status = "error"

    addition = ""
    if some_builds_are_missing:
        addition = f"({len(build_reports)} of {required_builds} builds are OK)"

    description = f"{ok_groups}/{total_groups} artifact groups are OK {addition}"

    commit = get_commit(gh, pr_info.sha)
    commit.create_status(
        context=build_check_name,
        description=description,
        state=summary_status,
        target_url=url,
    )

    if summary_status == "error":
        sys.exit(1)


if __name__ == "__main__":
    main()
