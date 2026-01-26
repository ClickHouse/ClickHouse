#!/usr/bin/env python3

import json
import re
from datetime import datetime, timedelta
from typing import List, Optional

from ci.praktika.dataclasses import TestCaseIssue, TestCaseIssueCatalog
from ci.praktika.result import Result
from ci.praktika.s3 import S3
from ci.praktika.utils import Shell, Utils
from ci.settings.settings import S3_REPORT_BUCKET_NAME


def extract_test_name(title: str, body: str) -> Optional[str]:
    """
    Best-effort extraction of a test identifier from an issue title/body.

    Heuristics (in order):
    - A 5-digit test id followed by an underscore and non-space characters,
      e.g. "01234_test_name".
    - A pytest-style name starting with "test_" followed by any characters
      except whitespace or common closing punctuation/quotes.

    Returns the raw match with trailing quotes/punctuation stripped, or None if
    no pattern matches.
    """
    # Combine title and body for searching
    text = f"{title}\n{body}"

    # Pattern 1: five digits + underscore + non-space chars (covers numbered tests)
    pattern1 = r"\b(\d{5}_\S+)"
    match1 = re.search(pattern1, text)
    if match1:
        test_name = match1.group(1)
        # Strip trailing quotes, backticks, and other punctuation
        return test_name.rstrip("`'\",.;:!?)")

    # Pattern 2: pytest-style names that start with test_, allowing rich suffixes
    # Use a negated class to stop at whitespace or closing punctuation/quotes.
    pattern2 = r"\b(test_[^\s`'\",]+)"
    match2 = re.search(pattern2, text)
    if match2:
        test_name = match2.group(1)
        # Strip trailing quotes, backticks, and other punctuation
        return test_name.rstrip("`'\",.;:!?)")

    return None


def fetch_github_issues(
    label: str, state: str = "open", days_back: int = None
) -> List[dict]:
    """
    Fetch issues from GitHub using gh CLI with manual pagination.

    Args:
        label: GitHub label to filter by
        state: Issue state (open or closed)
        days_back: For closed issues, only fetch those closed within this many days (default: None for all)

    Returns:
        List of issue dictionaries
    """
    all_issues = []
    limit_per_request = 1000  # Maximum we'll fetch per request

    # Build base command
    if state == "closed" and days_back:
        date_threshold = (datetime.now() - timedelta(days=days_back)).strftime(
            "%Y-%m-%d"
        )
        search_query = f'label:"{label}" is:closed closed:>{date_threshold}'
        base_cmd = f"gh issue list --search '{search_query}' --json number,title,body,closedAt --limit {limit_per_request}"
        print(
            f"Fetching {state} issues with label '{label}' closed in last {days_back} days (since {date_threshold})..."
        )
    else:
        base_cmd = f'gh issue list --label "{label}" --state {state} --json number,title,body,closedAt --limit {limit_per_request}'
        print(f"Fetching {state} issues with label '{label}'...")

    try:
        output = Shell.get_output(base_cmd, verbose=True)

        if not output or not output.strip():
            print(f"  No issues found for label '{label}' with state '{state}'")
            return []

        issues = json.loads(output)
        if not issues:
            print(f"  No issues found for label '{label}' with state '{state}'")
            return []

        all_issues.extend(issues)
        print(f"  Found {len(all_issues)} issues")

        # If the limit was hit, print a warning — there may be more issues to page
        if len(issues) == limit_per_request:
            print(
                f"  WARNING: Reached limit of {limit_per_request} issues. There may be more issues not fetched."
            )

        return all_issues
    except json.JSONDecodeError as e:
        print(f"ERROR: Failed to parse JSON response for label '{label}': {e}")
        return []
    except Exception as e:
        print(f"ERROR: Failed to fetch issues with label '{label}': {e}")
        return []


def process_issues(issues: List[dict], is_closed: bool = False) -> List[TestCaseIssue]:
    """
    Process raw GitHub issues into TestCaseIssue objects.

    Args:
        issues: List of raw issue dictionaries from GitHub
        is_closed: Whether these are closed issues

    Returns:
        List of TestCaseIssue objects
    """
    test_case_issues = []

    for issue in issues:
        number = issue.get("number", "")
        title = issue.get("title", "")
        body = issue.get("body", "")
        closed_at = issue.get("closedAt", "")

        # Extract test name from title or body
        test_name = extract_test_name(title, body)

        if not test_name:
            print(
                f"  Warning: Could not extract test name from issue #{number}: {title}"
            )
            test_name = "unknown"

        # Construct GitHub issue URL
        issue_url = f"https://github.com/ClickHouse/ClickHouse/issues/{number}"

        test_case_issue = TestCaseIssue(
            test_name=test_name,
            closed_at=closed_at if closed_at else "",
            issue=int(number),
            issue_url=issue_url,
            title=title,
            body=body if body else "",
        )
        test_case_issues.append(test_case_issue)

    return test_case_issues


def fetch_flaky_test_catalog() -> TestCaseIssueCatalog:
    """
    Fetch and organize all flaky test issues from GitHub.

    Returns:
        TestCaseIssueCatalog with active and resolved issues
    """
    catalog = TestCaseIssueCatalog()

    # Fetch open issues with label "flaky"
    print("\n--- Fetching active flaky test issues ---")
    open_issues = fetch_github_issues(label="flaky test", state="open")
    catalog.active_test_issues = process_issues(open_issues, is_closed=False)
    print(f"Processed {len(catalog.active_test_issues)} active issues\n")

    # Fetch closed issues with label "flaky test" from the last 30 days
    print("--- Fetching resolved flaky test issues ---")
    closed_issues = fetch_github_issues(
        label="flaky test", state="closed", days_back=30
    )
    catalog.resolved_test_issues = process_issues(closed_issues, is_closed=True)
    print(f"Processed {len(catalog.resolved_test_issues)} resolved issues\n")

    return catalog


if __name__ == "__main__":
    results = []
    catalog = None

    def fetch_catalog():
        global catalog
        catalog = fetch_flaky_test_catalog()
        catalog.dump()
        return True

    results.append(
        Result.from_commands_run(name="Fetch flaky test issues", command=fetch_catalog)
    )

    if results[-1].is_ok() and catalog is not None:
        # Print summary
        print("\n=== Flaky Test Issues Summary ===")
        print(f"Active issues: {len(catalog.active_test_issues)}")
        print(f"Resolved issues: {len(catalog.resolved_test_issues)}")

        # Print sample of active issues
        if catalog.active_test_issues:
            print("\n--- Sample Active Issues ---")
            for issue in catalog.active_test_issues[:5]:
                print(f"  Issue #{issue.issue}: {issue.test_name} - {issue.title}")

        # Print sample of resolved issues
        if catalog.resolved_test_issues:
            print("\n--- Sample Resolved Issues ---")
            for issue in catalog.resolved_test_issues[:5]:
                print(
                    f"  Issue #{issue.issue}: {issue.test_name} - {issue.title} (closed: {issue.closed_at})"
                )
    elif not results[-1].is_ok():
        print("ERROR: Fetch flaky test issues step failed")
    else:
        print("ERROR: Catalog is None after fetch; nothing to report")

    link = None
    if results[-1].is_ok() and catalog is not None:

        def upload():
            local_name = catalog.file_name_static("flaky_test_catalog")
            compressed_name = Utils.compress_gz(local_name)
            global link
            link = S3.copy_file_to_s3(
                local_path=compressed_name,
                s3_path=f"{S3_REPORT_BUCKET_NAME}/statistics",
                content_type="application/json",
                content_encoding="gzip",
            )
            return True

        results.append(
            Result.from_commands_run(name="Upload flaky test catalog", command=upload)
        )

    # Complete the job
    Result.create_from(results=results, links=[link]).complete_job()
