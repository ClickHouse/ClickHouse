#!/usr/bin/env python3
import argparse
import os
from pathlib import Path
from itertools import combinations
import json
from datetime import datetime
from functools import lru_cache
from glob import glob

import pandas as pd
from jinja2 import Environment, FileSystemLoader
import requests
from clickhouse_driver import Client
import boto3
from botocore.exceptions import NoCredentialsError

DATABASE_HOST_VAR = "CHECKS_DATABASE_HOST"
DATABASE_USER_VAR = "CLICKHOUSE_TEST_STAT_LOGIN"
DATABASE_PASSWORD_VAR = "CLICKHOUSE_TEST_STAT_PASSWORD"
S3_BUCKET = "altinity-build-artifacts"
GITHUB_REPO = "Altinity/ClickHouse"
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN") or os.getenv("GH_TOKEN")

def get_commit_statuses(sha: str) -> pd.DataFrame:
    """
    Fetch commit statuses for a given SHA and return as a pandas DataFrame.
    Handles pagination to get all statuses.

    Args:
        sha (str): Commit SHA to fetch statuses for.

    Returns:
        pd.DataFrame: DataFrame containing all statuses.
    """
    headers = {
        "Authorization": f"token {GITHUB_TOKEN}",
        "Accept": "application/vnd.github.v3+json",
    }

    url = f"https://api.github.com/repos/{GITHUB_REPO}/commits/{sha}/statuses"

    all_data = []

    while url:
        response = requests.get(url, headers=headers)

        if response.status_code != 200:
            raise Exception(
                f"Failed to fetch statuses: {response.status_code} {response.text}"
            )

        data = response.json()
        all_data.extend(data)

        # Check for pagination links in the response headers
        if "Link" in response.headers:
            links = response.headers["Link"].split(",")
            next_url = None

            for link in links:
                parts = link.strip().split(";")
                if len(parts) == 2 and 'rel="next"' in parts[1]:
                    next_url = parts[0].strip("<>")
                    break

            url = next_url
        else:
            url = None

    # Parse relevant fields
    parsed = [
        {
            "job_name": item["context"],
            "job_status": item["state"],
            "message": item["description"],
            "results_link": item["target_url"],
        }
        for item in all_data
    ]

    # Create DataFrame
    df = pd.DataFrame(parsed)

    # Drop duplicates keeping the first occurrence (newest status for each context)
    # GitHub returns statuses in reverse chronological order
    df = df.drop_duplicates(subset=["job_name"], keep="first")

    # Sort by status and job name
    return df.sort_values(
        by=["job_status", "job_name"], ascending=[True, True]
    ).reset_index(drop=True)


def get_pr_info_from_number(pr_number: str) -> dict:
    """
    Fetch pull request information for a given PR number.

    Args:
        pr_number (str): Pull request number to fetch information for.

    Returns:
        dict: Dictionary containing PR information.
    """
    headers = {
        "Authorization": f"token {GITHUB_TOKEN}",
        "Accept": "application/vnd.github.v3+json",
    }

    url = f"https://api.github.com/repos/{GITHUB_REPO}/pulls/{pr_number}"
    response = requests.get(url, headers=headers)

    if response.status_code != 200:
        raise Exception(
            f"Failed to fetch pull request info: {response.status_code} {response.text}"
        )

    return response.json()


@lru_cache
def get_run_details(run_url: str) -> dict:
    """
    Fetch run details for a given run URL.
    """
    run_id = run_url.split("/")[-1]

    headers = {
        "Authorization": f"token {GITHUB_TOKEN}",
        "Accept": "application/vnd.github.v3+json",
    }

    url = f"https://api.github.com/repos/{GITHUB_REPO}/actions/runs/{run_id}"
    response = requests.get(url, headers=headers)

    if response.status_code != 200:
        raise Exception(
            f"Failed to fetch run details: {response.status_code} {response.text}"
        )

    return response.json()


def get_checks_fails(client: Client, commit_sha: str, branch_name: str):
    """
    Get tests that did not succeed for the given commit and branch.
    Exclude checks that have status 'error' as they are counted in get_checks_errors.
    """
    query = f"""SELECT job_status, job_name, status as test_status, test_name, results_link
            FROM (
                SELECT
                    argMax(check_status, check_start_time) as job_status,
                    check_name as job_name,
                    argMax(test_status, check_start_time) as status,
                    test_name,
                    report_url as results_link,
                    task_url
                FROM `gh-data`.checks
                WHERE commit_sha='{commit_sha}' AND head_ref='{branch_name}'
                GROUP BY check_name, test_name, report_url, task_url
            )
            WHERE test_status IN ('FAIL', 'ERROR')
            AND job_status!='error'
            ORDER BY job_name, test_name
            """
    return client.query_dataframe(query)


def get_checks_known_fails(
    client: Client, commit_sha: str, branch_name: str, known_fails: dict
):
    """
    Get tests that are known to fail for the given commit and branch.
    """
    if len(known_fails) == 0:
        return pd.DataFrame()

    query = f"""SELECT job_status, job_name, status as test_status, test_name, results_link
        FROM (
            SELECT
                argMax(check_status, check_start_time) as job_status,
                check_name as job_name,
                argMax(test_status, check_start_time) as status,
                test_name,
                report_url as results_link,
                task_url
            FROM `gh-data`.checks
            WHERE commit_sha='{commit_sha}' AND head_ref='{branch_name}'
            GROUP BY check_name, test_name, report_url, task_url
        )
        WHERE test_status='BROKEN'
        AND test_name IN ({','.join(f"'{test}'" for test in known_fails.keys())})
        ORDER BY job_name, test_name
        """

    df = client.query_dataframe(query)

    df.insert(
        len(df.columns) - 1,
        "reason",
        df["test_name"]
        .astype(str)
        .apply(
            lambda test_name: known_fails[test_name].get("reason", "No reason given")
        ),
    )

    return df


def get_checks_errors(client: Client, commit_sha: str, branch_name: str):
    """
    Get checks that have status 'error' for the given commit and branch.
    """
    query = f"""SELECT job_status, job_name, status as test_status, test_name, results_link
            FROM (
                SELECT
                    argMax(check_status, check_start_time) as job_status,
                    check_name as job_name,
                    argMax(test_status, check_start_time) as status,
                    test_name,
                    report_url as results_link,
                    task_url
                FROM `gh-data`.checks
                WHERE commit_sha='{commit_sha}' AND head_ref='{branch_name}'
                GROUP BY check_name, test_name, report_url, task_url
            )
            WHERE job_status=='error'
            ORDER BY job_name, test_name
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
    # Nested SELECT handles test reruns
    query = f"""SELECT arch, job_name, status, test_name, results_link
            FROM (
               SELECT
                    architecture as arch,
                    test_name,
                    argMax(result, start_time) AS status,
                    job_name,
                    report_url as results_link,
                    job_url
               FROM `gh-data`.clickhouse_regression_results
               GROUP BY architecture, test_name, job_url, job_name, report_url
               ORDER BY length(test_name) DESC
            )
            WHERE job_url LIKE '{job_url}%'
            AND status IN ('Fail', 'Error')
            """
    df = client.query_dataframe(query)
    df = drop_prefix_rows(df, "test_name")
    df["job_name"] = df["job_name"].str.title()
    return df


def get_new_fails_this_pr(
    client: Client,
    pr_info: dict,
    checks_fails: pd.DataFrame,
    regression_fails: pd.DataFrame,
):
    """
    Get tests that failed in the PR but passed in the base branch.
    Compares both checks and regression test results.
    """
    base_sha = pr_info.get("base", {}).get("sha")
    if not base_sha:
        raise Exception("No base SHA found for PR")

    # Modify tables to have the same columns
    if len(checks_fails) > 0:
        checks_fails = checks_fails.copy().drop(columns=["job_status"])
    if len(regression_fails) > 0:
        regression_fails = regression_fails.copy()
        regression_fails["job_name"] = regression_fails.apply(
            lambda row: f"{row['arch']} {row['job_name']}".strip(), axis=1
        )
        regression_fails["test_status"] = regression_fails["status"]

    # Combine both types of fails and select only desired columns
    desired_columns = ["job_name", "test_name", "test_status", "results_link"]
    all_pr_fails = pd.concat([checks_fails, regression_fails], ignore_index=True)[
        desired_columns
    ]
    if len(all_pr_fails) == 0:
        return pd.DataFrame()

    # Get all checks from the base branch that didn't fail
    base_checks_query = f"""SELECT job_name, status as test_status, test_name, results_link
            FROM (
                SELECT
                    check_name as job_name,
                    argMax(test_status, check_start_time) as status,
                    test_name,
                    report_url as results_link,
                    task_url
                FROM `gh-data`.checks
                WHERE commit_sha='{base_sha}'
                GROUP BY check_name, test_name, report_url, task_url
            )
            WHERE test_status NOT IN ('FAIL', 'ERROR')
            ORDER BY job_name, test_name
            """
    base_checks = client.query_dataframe(base_checks_query)

    # Get regression results from base branch that didn't fail
    base_regression_query = f"""SELECT arch, job_name, status, test_name, results_link
            FROM (
               SELECT
                    architecture as arch,
                    test_name,
                    argMax(result, start_time) AS status,
                    job_url,
                    job_name,
                    report_url as results_link
               FROM `gh-data`.clickhouse_regression_results
               WHERE results_link LIKE'%/{base_sha}/%'
               GROUP BY architecture, test_name, job_url, job_name, report_url
               ORDER BY length(test_name) DESC
            )
            WHERE status NOT IN ('Fail', 'Error')
            """
    base_regression = client.query_dataframe(base_regression_query)
    if len(base_regression) > 0:
        base_regression["job_name"] = base_regression.apply(
            lambda row: f"{row['arch']} {row['job_name']}".strip(), axis=1
        )
        base_regression["test_status"] = base_regression["status"]
        base_regression = base_regression.drop(columns=["arch", "status"])

    # Combine base results
    base_results = pd.concat([base_checks, base_regression], ignore_index=True)

    # Find tests that failed in PR but passed in base
    pr_failed_tests = set(zip(all_pr_fails["job_name"], all_pr_fails["test_name"]))
    base_passed_tests = set(zip(base_results["job_name"], base_results["test_name"]))

    new_fails = pr_failed_tests.intersection(base_passed_tests)

    # Filter PR results to only include new fails
    mask = all_pr_fails.apply(
        lambda row: (row["job_name"], row["test_name"]) in new_fails, axis=1
    )
    new_fails_df = all_pr_fails[mask]

    return new_fails_df


@lru_cache
def get_workflow_config() -> dict:
    workflow_config_files = glob("./ci/tmp/workflow_config*.json")
    if len(workflow_config_files) == 0:
        raise Exception("No workflow config file found")
    if len(workflow_config_files) > 1:
        raise Exception("Multiple workflow config files found")
    with open(workflow_config_files[0], "r") as f:
        return json.load(f)


def get_cached_job(job_name: str) -> dict:
    workflow_config = get_workflow_config()
    return workflow_config["cache_jobs"].get(job_name, {})


def get_cves(pr_number, commit_sha):
    """
    Fetch Grype results from S3.

    If no results are available for download, returns ... (Ellipsis).
    """
    s3_client = boto3.client("s3", endpoint_url=os.getenv("S3_URL"))
    prefixes_to_check = set()

    cached_server_job = get_cached_job("Docker server image")
    if cached_server_job:
        prefixes_to_check.add(
            f"{cached_server_job['pr_number']}/{cached_server_job['sha']}/grype/"
        )
    cached_keeper_job = get_cached_job("Docker keeper image")
    if cached_keeper_job:
        prefixes_to_check.add(
            f"{cached_keeper_job['pr_number']}/{cached_keeper_job['sha']}/grype/"
        )

    if not prefixes_to_check:
        prefixes_to_check = {f"{pr_number}/{commit_sha}/grype/"}

    grype_result_dirs = []
    for s3_prefix in prefixes_to_check:
        try:
            response = s3_client.list_objects_v2(
                Bucket=S3_BUCKET, Prefix=s3_prefix, Delimiter="/"
            )
            grype_result_dirs.extend(
                content["Prefix"] for content in response.get("CommonPrefixes", [])
            )
        except Exception as e:
            print(f"Error listing S3 objects at {s3_prefix}: {e}")
            continue

    if len(grype_result_dirs) == 0:
        # We were asked to check the CVE data, but none was found,
        # maybe this is a preview report and grype results are not available yet
        return ...

    results = []
    for path in grype_result_dirs:
        file_key = f"{path}result.json"
        try:
            file_response = s3_client.get_object(Bucket=S3_BUCKET, Key=file_key)
            content = file_response["Body"].read().decode("utf-8")
            results.append(json.loads(content))
        except Exception as e:
            print(f"Error getting S3 object at {file_key}: {e}")
            continue

    rows = []
    for scan_result in results:
        for match in scan_result["matches"]:
            rows.append(
                {
                    "docker_image": scan_result["source"]["target"]["userInput"],
                    "severity": match["vulnerability"]["severity"],
                    "identifier": match["vulnerability"]["id"],
                    "namespace": match["vulnerability"]["namespace"],
                }
            )

    if len(rows) == 0:
        return pd.DataFrame()

    df = pd.DataFrame(rows).drop_duplicates()
    df = df.sort_values(
        by="severity",
        key=lambda col: col.str.lower().map(
            {"critical": 1, "high": 2, "medium": 3, "low": 4, "negligible": 5}
        ),
    )
    return df


def url_to_html_link(url: str) -> str:
    if not url:
        return ""
    text = url.split("/")[-1].split("?")[0]
    if not text:
        text = "results"
    return f'<a href="{url}">{text}</a>'


def format_test_name_for_linewrap(text: str) -> str:
    """Tweak the test name to improve line wrapping."""
    return f'<span style="line-break: anywhere;">{text}</span>'


def format_test_status(text: str) -> str:
    """Format the test status for better readability."""
    color = (
        "red"
        if text.lower().startswith("fail")
        else "orange" if text.lower() in ("error", "broken", "pending") else "green"
    )
    return f'<span style="font-weight: bold; color: {color}">{text}</span>'


def format_results_as_html_table(results) -> str:
    if len(results) == 0:
        return "<p>Nothing to report</p>"
    results.columns = [col.replace("_", " ").title() for col in results.columns]
    html = results.to_html(
        index=False,
        formatters={
            "Results Link": url_to_html_link,
            "Test Name": format_test_name_for_linewrap,
            "Test Status": format_test_status,
            "Job Status": format_test_status,
            "Status": format_test_status,
            "Message": lambda m: m.replace("\n", " "),
            "Identifier": lambda i: url_to_html_link(
                "https://nvd.nist.gov/vuln/detail/" + i
            ),
        },
        escape=False,
        border=0,
        classes=["test-results-table"],
    )
    return html


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Create a combined CI report.")
    parser.add_argument(  # Need the full URL rather than just the ID to query the databases
        "--actions-run-url", required=True, help="URL of the actions run"
    )
    parser.add_argument(
        "--pr-number", help="Pull request number for the S3 path", type=int
    )
    parser.add_argument("--commit-sha", help="Commit SHA for the S3 path")
    parser.add_argument(
        "--no-upload", action="store_true", help="Do not upload the report"
    )
    parser.add_argument(
        "--known-fails", type=str, help="Path to the file with known fails"
    )
    parser.add_argument(
        "--cves", action="store_true", help="Get CVEs from Grype results"
    )
    parser.add_argument(
        "--mark-preview", action="store_true", help="Mark the report as a preview"
    )
    return parser.parse_args()


def create_workflow_report(
    actions_run_url: str,
    pr_number: int = None,
    commit_sha: str = None,
    no_upload: bool = False,
    known_fails: str = None,
    check_cves: bool = False,
    mark_preview: bool = False,
) -> str:
    if pr_number is None or commit_sha is None:
        run_details = get_run_details(actions_run_url)
        if pr_number is None:
            if len(run_details["pull_requests"]) > 0:
                pr_number = run_details["pull_requests"][0]["number"]
            else:
                pr_number = 0
        if commit_sha is None:
            commit_sha = run_details["head_commit"]["id"]

    host = os.getenv(DATABASE_HOST_VAR)
    if not host:
        print(f"{DATABASE_HOST_VAR} is not set")
    user = os.getenv(DATABASE_USER_VAR)
    if not user:
        print(f"{DATABASE_USER_VAR} is not set")
    password = os.getenv(DATABASE_PASSWORD_VAR)
    if not password:
        print(f"{DATABASE_PASSWORD_VAR} is not set")
    if not GITHUB_TOKEN:
        print("GITHUB_TOKEN is not set")
    if not all([host, user, password, GITHUB_TOKEN]):
        raise Exception("Required environment variables are not set")

    db_client = Client(
        host=host,
        user=user,
        password=password,
        port=9440,
        secure="y",
        verify=False,
        settings={"use_numpy": True},
    )

    run_details = get_run_details(actions_run_url)
    branch_name = run_details.get("head_branch", "unknown branch")

    fail_results = {
        "job_statuses": get_commit_statuses(commit_sha),
        "checks_fails": get_checks_fails(db_client, commit_sha, branch_name),
        "checks_known_fails": [],
        "pr_new_fails": [],
        "checks_errors": get_checks_errors(db_client, commit_sha, branch_name),
        "regression_fails": get_regression_fails(db_client, actions_run_url),
        "docker_images_cves": (
            [] if not check_cves else get_cves(pr_number, commit_sha)
        ),
    }

    # get_cves returns ... in the case where no Grype result files were found.
    # This might occur when run in preview mode.
    cves_not_checked = not check_cves or fail_results["docker_images_cves"] is ...

    if known_fails:
        if not os.path.exists(known_fails):
            print(f"Known fails file {known_fails} not found.")
            exit(1)

        with open(known_fails) as f:
            known_fails = json.load(f)

        if known_fails:
            fail_results["checks_known_fails"] = get_checks_known_fails(
                db_client, commit_sha, branch_name, known_fails
            )

    if pr_number == 0:
        pr_info_html = f"Release ({branch_name})"
    else:
        try:
            pr_info = get_pr_info_from_number(pr_number)
            pr_info_html = f"""<a href="https://github.com/{GITHUB_REPO}/pull/{pr_info["number"]}">
                    #{pr_info.get("number")} ({pr_info.get("base", {}).get('ref')} <- {pr_info.get("head", {}).get('ref')})  {pr_info.get("title")}
                    </a>"""
            fail_results["pr_new_fails"] = get_new_fails_this_pr(
                db_client,
                pr_info,
                fail_results["checks_fails"],
                fail_results["regression_fails"],
            )
        except Exception as e:
            pr_info_html = e

    high_cve_count = 0
    if not cves_not_checked and len(fail_results["docker_images_cves"]) > 0:
        high_cve_count = (
            fail_results["docker_images_cves"]["severity"]
            .str.lower()
            .isin(("high", "critical"))
            .sum()
        )

    # Set up the Jinja2 environment
    template_dir = os.path.dirname(__file__)

    # Load the template
    template = Environment(loader=FileSystemLoader(template_dir)).get_template(
        "ci_run_report.html.jinja"
    )

    # Define the context for rendering
    context = {
        "title": "ClickHouse® CI Workflow Run Report",
        "github_repo": GITHUB_REPO,
        "s3_bucket": S3_BUCKET,
        "pr_info_html": pr_info_html,
        "pr_number": pr_number,
        "workflow_id": actions_run_url.split("/")[-1],
        "commit_sha": commit_sha,
        "base_sha": "" if pr_number == 0 else pr_info.get("base", {}).get("sha"),
        "date": f"{datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC",
        "is_preview": mark_preview,
        "counts": {
            "jobs_status": f"{sum(fail_results['job_statuses']['job_status'] != 'success')} fail/error",
            "checks_errors": len(fail_results["checks_errors"]),
            "checks_new_fails": len(fail_results["checks_fails"]),
            "regression_new_fails": len(fail_results["regression_fails"]),
            "cves": "N/A" if cves_not_checked else f"{high_cve_count} high/critical",
            "checks_known_fails": (
                "N/A" if not known_fails else len(fail_results["checks_known_fails"])
            ),
            "pr_new_fails": len(fail_results["pr_new_fails"]),
        },
        "ci_jobs_status_html": format_results_as_html_table(
            fail_results["job_statuses"]
        ),
        "checks_errors_html": format_results_as_html_table(
            fail_results["checks_errors"]
        ),
        "checks_fails_html": format_results_as_html_table(fail_results["checks_fails"]),
        "regression_fails_html": format_results_as_html_table(
            fail_results["regression_fails"]
        ),
        "docker_images_cves_html": (
            "<p>Not Checked</p>"
            if cves_not_checked
            else format_results_as_html_table(fail_results["docker_images_cves"])
        ),
        "checks_known_fails_html": (
            "<p>Not Checked</p>"
            if not known_fails
            else format_results_as_html_table(fail_results["checks_known_fails"])
        ),
        "new_fails_html": format_results_as_html_table(fail_results["pr_new_fails"]),
    }

    # Render the template with the context
    rendered_html = template.render(context)

    report_name = "ci_run_report.html"
    report_path = Path(report_name)
    report_path.write_text(rendered_html, encoding="utf-8")

    if no_upload:
        print(f"Report saved to {report_path}")
        exit(0)

    report_destination_key = f"{pr_number}/{commit_sha}/{report_name}"

    # Upload the report to S3
    s3_client = boto3.client("s3", endpoint_url=os.getenv("S3_URL"))

    try:
        s3_client.put_object(
            Bucket=S3_BUCKET,
            Key=report_destination_key,
            Body=rendered_html,
            ContentType="text/html; charset=utf-8",
        )
    except NoCredentialsError:
        print("Credentials not available for S3 upload.")

    return f"https://s3.amazonaws.com/{S3_BUCKET}/" + report_destination_key


def main():
    args = parse_args()

    report_url = create_workflow_report(
        args.actions_run_url,
        args.pr_number,
        args.commit_sha,
        args.no_upload,
        args.known_fails,
        args.cves,
        args.mark_preview,
    )

    print(report_url)


if __name__ == "__main__":
    main()
