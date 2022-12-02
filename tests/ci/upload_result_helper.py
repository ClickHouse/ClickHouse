import os
import logging

from typing import List, Tuple

from env_helper import (
    GITHUB_JOB_URL,
    GITHUB_REPOSITORY,
    GITHUB_RUN_URL,
    GITHUB_SERVER_URL,
)
from report import ReportColorTheme, create_test_html_report
from s3_helper import S3Helper


def process_logs(
    s3_client: S3Helper, additional_logs: List[str], s3_path_prefix: str
) -> List[str]:
    logging.info("Upload files to s3 %s", additional_logs)

    additional_urls = []  # type: List[str]
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
    test_results: List[Tuple[str, str]],
    additional_files: List[str],
    check_name: str,
    with_raw_logs: bool = True,
) -> str:
    s3_path_prefix = f"{pr_number}/{commit_sha}/" + check_name.lower().replace(
        " ", "_"
    ).replace("(", "_").replace(")", "_").replace(",", "_")
    additional_urls = process_logs(s3_client, additional_files, s3_path_prefix)

    branch_url = f"{GITHUB_SERVER_URL}/{GITHUB_REPOSITORY}/commits/master"
    branch_name = "master"
    if pr_number != 0:
        branch_name = f"PR #{pr_number}"
        branch_url = f"{GITHUB_SERVER_URL}/{GITHUB_REPOSITORY}/pull/{pr_number}"
    commit_url = f"{GITHUB_SERVER_URL}/{GITHUB_REPOSITORY}/commit/{commit_sha}"

    if additional_urls:
        raw_log_url = additional_urls[0]
        additional_urls.pop(0)
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
        with_raw_logs,
        statuscolors=statuscolors,
    )
    with open("report.html", "w", encoding="utf-8") as f:
        f.write(html_report)

    url = s3_client.upload_test_report_to_s3("report.html", s3_path_prefix + ".html")
    logging.info("Search result in url %s", url)
    return url
