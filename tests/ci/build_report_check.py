#!/usr/bin/env python3

import logging
import sys
import atexit
from pathlib import Path

from github import Github

from env_helper import (
    GITHUB_JOB_URL,
    GITHUB_REPOSITORY,
    GITHUB_SERVER_URL,
    TEMP_PATH,
)
from report import (
    BuildResult,
    ERROR,
    PENDING,
    SUCCESS,
    create_build_html_report,
    get_worst_status,
)
from s3_helper import S3Helper
from get_robot_token import get_best_robot_token
from pr_info import PRInfo
from commit_status_helper import (
    RerunHelper,
    format_description,
    get_commit,
    post_commit_status,
    update_mergeable_check,
)
from ci_config import CI_CONFIG


def main():
    logging.basicConfig(level=logging.INFO)
    temp_path = Path(TEMP_PATH)
    temp_path.mkdir(parents=True, exist_ok=True)

    logging.info("Reports path %s", TEMP_PATH)
    reports_path = Path(TEMP_PATH)
    logging.info(
        "Reports found:\n %s",
        "\n ".join(p.as_posix() for p in reports_path.glob("*.json")),
    )

    build_check_name = sys.argv[1]

    gh = Github(get_best_robot_token(), per_page=100)
    pr_info = PRInfo()
    commit = get_commit(gh, pr_info.sha)

    atexit.register(update_mergeable_check, gh, pr_info, build_check_name)

    rerun_helper = RerunHelper(commit, build_check_name)
    if rerun_helper.is_already_finished_by_status():
        logging.info("Check is already finished according to github status, exiting")
        sys.exit(0)

    builds_for_check = CI_CONFIG.builds_report_config[build_check_name]
    required_builds = len(builds_for_check)

    # Collect reports from json artifacts
    build_results = []
    missing_results = []
    for build_name in builds_for_check:
        build_result = BuildResult.read_json(reports_path, build_name)
        if build_result.is_missing:
            logging.warning("Build results for %s are missing", build_name)
            missing_results.append(build_result)
            continue
        build_results.append(build_result)

    build_results = missing_results + build_results

    # Calculate artifact groups like packages and binaries
    total_groups = sum(len(br.grouped_urls) for br in build_results)
    ok_groups = sum(
        len(br.grouped_urls) for br in build_results if br.status == SUCCESS
    )
    logging.info("Totally got %s artifact groups", total_groups)
    if total_groups == 0:
        logging.error("No success builds, failing check without creating a status")
        sys.exit(1)

    s3_helper = S3Helper()

    branch_url = f"{GITHUB_SERVER_URL}/{GITHUB_REPOSITORY}/commits/master"
    branch_name = "master"
    if pr_info.number != 0:
        branch_name = f"PR #{pr_info.number}"
        branch_url = f"{GITHUB_SERVER_URL}/{GITHUB_REPOSITORY}/pull/{pr_info.number}"
    commit_url = f"{GITHUB_SERVER_URL}/{GITHUB_REPOSITORY}/commit/{pr_info.sha}"
    task_url = GITHUB_JOB_URL()
    report = create_build_html_report(
        build_check_name,
        build_results,
        task_url,
        branch_url,
        branch_name,
        commit_url,
    )

    report_path = temp_path / "report.html"
    report_path.write_text(report, encoding="utf-8")

    logging.info("Going to upload prepared report")
    context_name_for_path = build_check_name.lower().replace(" ", "_")
    s3_path_prefix = (
        str(pr_info.number) + "/" + pr_info.sha + "/" + context_name_for_path
    )

    url = s3_helper.upload_test_report_to_s3(
        report_path, s3_path_prefix + "/report.html"
    )
    logging.info("Report url %s", url)
    print(f"::notice ::Report url: {url}")

    # Prepare a commit status
    summary_status = get_worst_status(br.status for br in build_results)

    # Check if there are no builds at all, do not override bad status
    if summary_status == SUCCESS:
        if missing_results:
            summary_status = PENDING
        elif ok_groups == 0:
            summary_status = ERROR

    addition = ""
    if missing_results:
        addition = f" ({required_builds - len(missing_results)} of {required_builds} builds are OK)"

    description = format_description(
        f"{ok_groups}/{total_groups} artifact groups are OK{addition}"
    )

    post_commit_status(
        commit, summary_status, url, description, build_check_name, pr_info
    )

    if summary_status == ERROR:
        sys.exit(1)


if __name__ == "__main__":
    main()
