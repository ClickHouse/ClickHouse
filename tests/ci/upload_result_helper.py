import os
import logging
import ast

from env_helper import GITHUB_SERVER_URL, GITHUB_REPOSITORY, GITHUB_RUN_URL
from report import ReportColorTheme, create_test_html_report


def process_logs(
    s3_client, additional_logs, s3_path_prefix, test_results, with_raw_logs
):
    processed_logs = {}
    # Firstly convert paths of logs from test_results to urls to s3.
    for test_result in test_results:
        if len(test_result) <= 3 or with_raw_logs:
            continue

        # Convert from string repr of list to list.
        test_log_paths = ast.literal_eval(test_result[3])
        test_log_urls = []
        for log_path in test_log_paths:
            if log_path in processed_logs:
                test_log_urls.append(processed_logs[log_path])
            elif log_path:
                url = s3_client.upload_test_report_to_s3(
                    log_path, s3_path_prefix + "/" + os.path.basename(log_path)
                )
                test_log_urls.append(url)
                processed_logs[log_path] = url

        test_result[3] = test_log_urls

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
    s3_client,
    pr_number,
    commit_sha,
    test_results,
    additional_files,
    check_name,
    with_raw_logs=True,
    statuscolors=None,
):
    s3_path_prefix = f"{pr_number}/{commit_sha}/" + check_name.lower().replace(
        " ", "_"
    ).replace("(", "_").replace(")", "_").replace(",", "_")
    additional_urls = process_logs(
        s3_client, additional_files, s3_path_prefix, test_results, with_raw_logs
    )

    branch_url = f"{GITHUB_SERVER_URL}/{GITHUB_REPOSITORY}/commits/master"
    branch_name = "master"
    if pr_number != 0:
        branch_name = f"PR #{pr_number}"
        branch_url = f"{GITHUB_SERVER_URL}/{GITHUB_REPOSITORY}/pull/{pr_number}"
    commit_url = f"{GITHUB_SERVER_URL}/{GITHUB_REPOSITORY}/commit/{commit_sha}"

    task_url = GITHUB_RUN_URL

    if additional_urls:
        raw_log_url = additional_urls[0]
        additional_urls.pop(0)
    else:
        raw_log_url = task_url

    statuscolors = (
        ReportColorTheme.bugfixcheck if "bugfix validate check" in check_name else None
    )

    html_report = create_test_html_report(
        check_name,
        test_results,
        raw_log_url,
        task_url,
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
