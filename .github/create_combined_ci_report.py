#!/usr/bin/env python3
import argparse
import os
from pathlib import Path
from itertools import combinations

import requests
from clickhouse_driver import Client
import boto3
from botocore.exceptions import NoCredentialsError

DATABASE_HOST_VAR = "CHECKS_DATABASE_HOST"
DATABASE_USER_VAR = "CHECKS_DATABASE_USER"
DATABASE_PASSWORD_VAR = "CHECKS_DATABASE_PASSWORD"
S3_BUCKET = "altinity-build-artifacts"


def get_checks_fails(client: Client, job_url: str):
    """
    Get tests that did not succeed for the given job URL.
    Exclude checks that have status 'error' as they are counted in get_checks_errors.
    """
    columns = (
        "check_status, check_name, test_status, test_name, report_url as results_link"
    )
    query = f"""SELECT {columns} FROM `gh-data`.checks
                WHERE task_url='{job_url}'
                AND test_status in ('FAIL', 'ERROR')
                AND check_status!='error'
                ORDER BY check_name, test_name
                """
    return client.query_dataframe(query)


def get_checks_errors(client: Client, job_url: str):
    """
    Get checks that have status 'error' for the given job URL.
    """
    columns = (
        "check_status, check_name, test_status, test_name, report_url as results_link"
    )
    query = f"""SELECT {columns} FROM `gh-data`.checks
                WHERE task_url='{job_url}'
                AND check_status=='error'
                ORDER BY check_name, test_name
                """
    return client.query_dataframe(query)


def drop_prefix_rows(df, column_to_clean):
    """
    Drop rows from the dataframe if:
    - the row matches another row completely except for the specified column
    - the specified column of that row is a prefix of the same column in another row
    """
    to_drop = set()
    reference_columns = [col for col in df.columns if col != column_to_clean]
    for (i, row_1), (j, row_2) in combinations(df.iterrows(), 2):
        if all(row_1[col] == row_2[col] for col in reference_columns):
            if row_2[column_to_clean].startswith(row_1[column_to_clean]):
                to_drop.add(i)
            elif row_1[column_to_clean].startswith(row_2[column_to_clean]):
                to_drop.add(j)
    return df.drop(to_drop)


def get_regression_fails(client: Client, job_url: str):
    """
    Get regression tests that did not succeed for the given job URL.
    """
    # If you rename the alias for report_url, also update the formatters in format_results_as_html_table
    query = f"""SELECT arch, status, test_name, results_link
            FROM (
               SELECT
                    architecture as arch,
                    test_name,
                    argMax(result, start_time) AS status,
                    job_url,
                    report_url as results_link
               FROM `gh-data`.clickhouse_regression_results
               GROUP BY architecture, test_name, job_url, report_url, start_time
               ORDER BY start_time DESC, length(test_name) DESC
            )
            WHERE job_url='{job_url}'
            AND status IN ('Fail', 'Error')
            """
    df = client.query_dataframe(query)
    df = drop_prefix_rows(df, "test_name")
    return df


def url_to_html_link(url: str) -> str:
    if not url:
        return ""
    text = url.split("/")[-1]
    if not text:
        text = "results"
    return f'<a href="{url}">{text}</a>'


def format_test_name_for_linewrap(text: str) -> str:
    """Tweak the test name to improve line wrapping."""
    return text.replace(".py::", "/")


def format_results_as_html_table(results) -> str:
    if results.empty:
        return ""
    results.columns = [col.replace("_", " ").title() for col in results.columns]
    html = (
        results.to_html(
            index=False,
            formatters={
                "Results Link": url_to_html_link,
                "Test Name": format_test_name_for_linewrap,
            },
            escape=False,
        )  # tbody/thead tags interfere with the table sorting script
        .replace("<tbody>\n", "")
        .replace("</tbody>\n", "")
        .replace("<thead>\n", "")
        .replace("</thead>\n", "")
        .replace('<table border="1"', '<table style="min-width: min(900px, 98vw);"')
    )
    return html


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Create a combined CI report.")
    parser.add_argument(
        "--actions-run-url", required=True, help="URL of the actions run"
    )
    parser.add_argument(
        "--pr-number", required=True, help="Pull request number for the S3 path"
    )
    parser.add_argument(
        "--commit-sha", required=True, help="Commit SHA for the S3 path"
    )
    parser.add_argument(
        "--no-upload", action="store_true", help="Do not upload the report"
    )
    parser.add_argument(
        "--mark-preview", action="store_true", help="Mark the report as a preview"
    )
    return parser.parse_args()


def main():
    args = parse_args()

    db_client = Client(
        host=os.getenv(DATABASE_HOST_VAR),
        user=os.getenv(DATABASE_USER_VAR),
        password=os.getenv(DATABASE_PASSWORD_VAR),
        port=9440,
        secure="y",
        verify=False,
        settings={"use_numpy": True},
    )

    s3_path = (
        f"https://s3.amazonaws.com/{S3_BUCKET}/{args.pr_number}/{args.commit_sha}/"
    )
    report_destination_url = s3_path + "combined_report.html"
    ci_running_report_url = s3_path + "ci_running.html"

    response = requests.get(ci_running_report_url)
    if response.status_code == 200:
        ci_running_report: str = response.text
    else:
        print(
            f"Failed to download CI running report. Status code: {response.status_code}, Response: {response.text}"
        )
        exit(1)

    fail_results = {
        "checks_fails": get_checks_fails(db_client, args.actions_run_url),
        "checks_errors": get_checks_errors(db_client, args.actions_run_url),
        "regression_fails": get_regression_fails(db_client, args.actions_run_url),
    }

    combined_report = (
        ci_running_report.replace("ClickHouse CI running for", "Combined CI Report for")
        .replace(
            "<table>",
            f"""<h2>Table of Contents</h2>
{'<p style="font-weight: bold;color: #F00;">This is a preview. FinishCheck has not completed.</p>' if args.mark_preview else ""}
<ul>
    <li><a href="#ci-jobs-status">CI Jobs Status</a></li>
    <li><a href="#checks-fails">Checks Fails</a> ({len(fail_results['checks_fails'])})</li>
    <li><a href="#checks-errors">Checks Errors</a> ({len(fail_results['checks_errors'])})</li>
    <li><a href="#regression-fails">Regression Fails</a> ({len(fail_results['regression_fails'])})</li>
</ul>

<h2 id="ci-jobs-status">CI Jobs Status</h2>
<table>""",
        )
        .replace(
            "</table>",
            f"""</table>

<h2 id="checks-fails">Checks Fails</h2>
{format_results_as_html_table(fail_results['checks_fails'])}

<h2 id="checks-errors">Checks Errors</h2>
{format_results_as_html_table(fail_results['checks_errors'])}

<h2 id="regression-fails">Regression Fails</h2>
{format_results_as_html_table(fail_results['regression_fails'])}
""",
        )
    )
    report_path = Path("combined_report.html")
    report_path.write_text(combined_report, encoding="utf-8")

    if args.no_upload:
        print(f"Report saved to {report_path}")
        exit(0)

    # Upload the report to S3
    s3_client = boto3.client("s3")

    try:
        s3_client.put_object(
            Bucket=S3_BUCKET,
            Key=f"{args.pr_number}/{args.commit_sha}/combined_report.html",
            Body=combined_report,
            ContentType="text/html; charset=utf-8",
        )
    except NoCredentialsError:
        print("Credentials not available for S3 upload.")

    print(report_destination_url)


if __name__ == "__main__":
    main()
