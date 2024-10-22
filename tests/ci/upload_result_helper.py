import logging
import os
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Union

from env_helper import GITHUB_REPOSITORY, GITHUB_RUN_URL, GITHUB_SERVER_URL
from report import GITHUB_JOB_URL, TestResults, create_test_html_report
from s3_helper import S3Helper


def process_logs(
    s3_client: S3Helper,
    additional_logs: Union[Sequence[str], Sequence[Path]],
    s3_path_prefix: str,
    test_results: TestResults,
) -> List[str]:
    logging.info("Upload files to s3 %s", additional_logs)

    processed_logs = {}  # type: Dict[str, str]
    # Firstly convert paths of logs from test_results to urls to s3.
    for test_result in test_results:
        if test_result.log_files is None:
            continue

        # Convert from string repr of list to list.
        test_result.log_urls = []
        for path in test_result.log_files:
            if path in processed_logs:
                test_result.log_urls.append(processed_logs[str(path)])
            elif path:
                url = s3_client.upload_test_report_to_s3(
                    Path(path), s3_path_prefix + "/" + str(path)
                )
                test_result.log_urls.append(url)
                processed_logs[str(path)] = url

    additional_urls = []
    for log_path in additional_logs:
        if Path(log_path).is_file():
            additional_urls.append(
                s3_client.upload_test_report_to_s3(
                    Path(log_path), s3_path_prefix + "/" + os.path.basename(log_path)
                )
            )
        else:
            logging.error("File %s is missing - skip", log_path)

    return additional_urls


def upload_results(
    s3_client: S3Helper,
    pr_number: int,
    commit_sha: str,
    test_results: TestResults,
    additional_files: Union[Sequence[Path], Sequence[str]],
    check_name: str,
    additional_urls: Optional[List[str]] = None,
) -> str:
    normalized_check_name = check_name.lower()
    for r in ((" ", "_"), ("(", "_"), (")", "_"), (",", "_"), ("/", "_")):
        normalized_check_name = normalized_check_name.replace(*r)

    # Preserve additional_urls to not modify the original one
    additional_urls = additional_urls or []
    s3_path_prefix = f"{pr_number}/{commit_sha}/{normalized_check_name}"
    additional_urls.extend(
        process_logs(s3_client, additional_files, s3_path_prefix, test_results)
    )

    branch_url = f"{GITHUB_SERVER_URL}/{GITHUB_REPOSITORY}/commits/master"
    branch_name = "master"
    if pr_number != 0:
        branch_name = f"PR #{pr_number}"
        branch_url = f"{GITHUB_SERVER_URL}/{GITHUB_REPOSITORY}/pull/{pr_number}"
    commit_url = f"{GITHUB_SERVER_URL}/{GITHUB_REPOSITORY}/commit/{commit_sha}"

    ready_report_url = None
    for url in additional_urls:
        if "report.html" in url:
            ready_report_url = url
            additional_urls.remove(ready_report_url)
            break

    if additional_urls:
        raw_log_url = additional_urls.pop(0)
    else:
        raw_log_url = GITHUB_JOB_URL()

    try:
        job_url = GITHUB_JOB_URL()
    except Exception:
        print(
            "ERROR: Failed to get job URL from GH API, job report will use run URL instead."
        )
        job_url = GITHUB_RUN_URL

    if test_results or not ready_report_url:
        html_report = create_test_html_report(
            check_name,
            test_results,
            raw_log_url,
            GITHUB_RUN_URL,
            job_url,
            branch_url,
            branch_name,
            commit_url,
            additional_urls,
        )
        report_path = Path("report.html")
        report_path.write_text(html_report, encoding="utf-8")
        url = s3_client.upload_test_report_to_s3(report_path, s3_path_prefix + ".html")
    else:
        logging.info("report.html was prepared by test job itself")
        url = ready_report_url

    logging.info("Search result in url %s", url)
    return url
