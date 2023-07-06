from pathlib import Path
from typing import Dict, List, Optional
import os
import logging

from env_helper import (
    GITHUB_JOB_URL,
    GITHUB_REPOSITORY,
    GITHUB_RUN_URL,
    GITHUB_SERVER_URL,
)
from report import ReportColorTheme, TestResults, create_test_html_report
from s3_helper import S3Helper


def process_logs(
    s3_client: S3Helper,
    additional_logs: List[str],
    s3_path_prefix: str,
    test_results: TestResults,
) -> List[str]:
    logging.info("Upload files to s3 %s", additional_logs)

    processed_logs = {}  # type: Dict[Path, str]
    # Firstly convert paths of logs from test_results to urls to s3.
    for test_result in test_results:
        if test_result.log_files is None:
            continue

        # Convert from string repr of list to list.
        test_result.log_urls = []
        for path in test_result.log_files:
            if path in processed_logs:
                test_result.log_urls.append(processed_logs[path])
            elif path:
                url = s3_client.upload_test_report_to_s3(
                    path.as_posix(), s3_path_prefix + "/" + path.name
                )
                test_result.log_urls.append(url)
                processed_logs[path] = url

    additional_urls = []
    for log_path in additional_logs:
        if log_path:
            additional_urls.append(
                s3_client.upload_test_report_to_s3(
                    log_path, s3_path_prefix + "/" + os.path.basename(log_path)
                )
            )

    return additional_urls


def upload_results(
    s3_client: S3Helper,
    pr_number: int,
    commit_sha: str,
    test_results: TestResults,
    additional_files: List[str],
    check_name: str,
    additional_urls: Optional[List[str]] = None,
) -> str:
    normalized_check_name = check_name.lower()
    for r in ((" ", "_"), ("(", "_"), (")", "_"), (",", "_"), ("/", "_")):
        normalized_check_name = normalized_check_name.replace(*r)

    # Preserve additional_urls to not modify the original one
    original_additional_urls = additional_urls or []
    s3_path_prefix = f"{pr_number}/{commit_sha}/{normalized_check_name}"
    additional_urls = process_logs(
        s3_client, additional_files, s3_path_prefix, test_results
    )
    additional_urls.extend(original_additional_urls)

    branch_url = f"{GITHUB_SERVER_URL}/{GITHUB_REPOSITORY}/commits/master"
    branch_name = "master"
    if pr_number != 0:
        branch_name = f"PR #{pr_number}"
        branch_url = f"{GITHUB_SERVER_URL}/{GITHUB_REPOSITORY}/pull/{pr_number}"
    commit_url = f"{GITHUB_SERVER_URL}/{GITHUB_REPOSITORY}/commit/{commit_sha}"

    if additional_urls:
        raw_log_url = additional_urls.pop(0)
    else:
        raw_log_url = GITHUB_JOB_URL()

    statuscolors = (
        ReportColorTheme.bugfixcheck if "bugfix validate check" in check_name else None
    )

    html_report = create_test_html_report(
        check_name,
        test_results,
        raw_log_url,
        GITHUB_RUN_URL,
        GITHUB_JOB_URL(),
        branch_url,
        branch_name,
        commit_url,
        additional_urls,
        statuscolors=statuscolors,
    )
    with open("report.html", "w", encoding="utf-8") as f:
        f.write(html_report)

    url = s3_client.upload_test_report_to_s3("report.html", s3_path_prefix + ".html")
    logging.info("Search result in url %s", url)
    return url
