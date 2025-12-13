#!/usr/bin/env python3

import json
import re
import sys
from datetime import datetime, timedelta
from typing import List, Optional

sys.path.append("./")
from ci.praktika.issue import TestCaseIssueCatalog, TestingIssue
from ci.praktika.result import Result
from ci.praktika.s3 import S3
from ci.praktika.utils import Shell, Utils
from ci.settings.settings import S3_REPORT_BUCKET_NAME


def parse_issue_body_fields(body: str) -> dict:
    """
    Parse structured fields from issue body.

    Expected format in body:
    - Test name: <content>
    - Failure reason: <content>
    - Failure flags: <flag1>, <flag2>, ...
    - CI action: <content>
    - Test pattern: <content>
    - Job pattern: <content>

    Returns:
        Dictionary with parsed fields
    """
    fields = {
        "test_name": "",
        "failure_reason": "",
        "failure_flags": [],
        "ci_action": "",
        "test_pattern": "",
        "job_pattern": "",
    }

    if not body:
        return fields

    # Parse each field with regex
    # Pattern: field name followed by colon, then content until end of line
    patterns = {
        "test_name": r"Test name:\s*(.+?)(?:\n|$)",
        "failure_reason": r"Failure reason:\s*(.+?)(?:\n|$)",
        "ci_action": r"CI action:\s*(.+?)(?:\n|$)",
        "test_pattern": r"Test pattern:\s*(.+?)(?:\n|$)",
        "job_pattern": r"Job pattern:\s*(.+?)(?:\n|$)",
    }

    for field, pattern in patterns.items():
        match = re.search(pattern, body, re.IGNORECASE)
        if match:
            fields[field] = match.group(1).strip()

    # Parse failure flags (comma-separated list)
    flags_match = re.search(r"Failure flags:\s*(.+?)(?:\n|$)", body, re.IGNORECASE)
    if flags_match:
        flags_str = flags_match.group(1).strip()
        # Split by comma and strip whitespace from each flag
        fields["failure_flags"] = [
            flag.strip() for flag in flags_str.split(",") if flag.strip()
        ]

    return fields


def extract_test_name(title: str, body: str) -> Optional[str]:
    """
    Best-effort extraction of a test identifier from an issue title/body.

    Heuristics (in order):
    - "Test name:" field in body (exact extraction)
    - A 5-digit test id followed by an underscore and non-space characters,
      e.g. "01234_test_name".
    - A pytest-style name starting with "test_" followed by any characters
      except whitespace or common closing punctuation/quotes.

    Returns the raw match with trailing quotes/punctuation stripped, or None if
    no pattern matches.
    """
    # First, try to extract from "Test name:" field in body
    if body:
        test_name_pattern = r"Test name:\s*(\S+)"
        match = re.search(test_name_pattern, body)
        if match:
            return match.group(1).rstrip("`'\",.;:!?)")

    # Pattern 1: five digits + underscore + non-space chars (covers numbered tests)
    pattern1 = r"\b(\d{5}_\S+)"
    match1 = re.search(pattern1, title)
    if match1:
        test_name = match1.group(1)
        # Strip trailing quotes, backticks, and other punctuation
        return test_name.rstrip("`'\",.;:!?)")

    # Pattern 2: pytest-style names that start with test_, allowing rich suffixes
    # Use a negated class to stop at whitespace or closing punctuation/quotes.
    pattern2 = r"\b(test_[^\s`'\",]+)"
    match2 = re.search(pattern2, title)
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
    if isinstance(label, str):
        label = [label]
    all_issues = []
    limit_per_request = 1000  # Maximum we'll fetch per request

    # Build base command
    if state == "closed" and days_back:
        date_threshold = (datetime.now() - timedelta(days=days_back)).strftime(
            "%Y-%m-%d"
        )
        label_query = " ".join([f'label:"{lbl}"' for lbl in label])
        search_query = f"{label_query} is:closed closed:>{date_threshold}"
        base_cmd = f"gh issue list --search '{search_query}' --json number,title,body,closedAt,labels --limit {limit_per_request}"
        print(
            f"Fetching {state} issues with label '{label}' closed in last {days_back} days (since {date_threshold})..."
        )
    else:
        label_args = " ".join([f'--label "{lbl}"' for lbl in label])
        base_cmd = f"gh issue list {label_args} --state {state} --json number,title,body,closedAt,labels --limit {limit_per_request}"
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


def process_issues(issues: List[dict], is_closed: bool = False) -> List[TestingIssue]:
    """
    Process raw GitHub issues into TestCaseIssue objects.

    Args:
        issues: List of raw issue dictionaries from GitHub
        is_closed: Whether these are closed issues

    Returns:
        List of TestCaseIssue objects
    """
    issues = []

    for issue in issues:
        number = issue.get("number", "")
        title = issue.get("title", "")
        body = issue.get("body", "")
        closed_at = issue.get("closedAt", "")
        # Extract label names from the labels array
        labels = [label.get("name", "") for label in issue.get("labels", [])]
        # Parse structured fields from issue body
        body_fields = parse_issue_body_fields(body)

        if "flaky test" in labels:
            # Extract test name from title or body
            test_name = extract_test_name(title, body)

            if not test_name:
                print(
                    f"  Warning: Could not extract test name from issue #{number}: {title}"
                )
                continue
        else:
            test_name = title

        # Construct GitHub issue URL
        issue_url = f"https://github.com/ClickHouse/ClickHouse/issues/{number}"

        issue = TestingIssue(
            test_name=body_fields["test_name"] or test_name,
            closed_at=closed_at if closed_at else "",
            issue=int(number),
            issue_url=issue_url,
            title=title,
            body=body if body else "",
            labels=labels,
            failure_reason=body_fields["failure_reason"],
            failure_flags=body_fields["failure_flags"],
            ci_action=body_fields["ci_action"],
            test_pattern=body_fields["test_pattern"],
            job_pattern=body_fields["job_pattern"],
        )
        if issue.validate():
            issues.append(issue)

    return issues


def fetch_gh_ci_issues() -> TestCaseIssueCatalog:
    """
    Fetch and organize all flaky test issues from GitHub.

    Returns:
        TestCaseIssueCatalog with active and resolved issues
    """
    catalog = TestCaseIssueCatalog()

    # Fetch open issues with label "testing" first
    print("\n--- Fetching active testing issues ---")
    testing_issues = fetch_github_issues(label="testing", state="open")
    catalog.active_test_issues = process_issues(testing_issues, is_closed=False)
    print(f"Processed {len(catalog.active_test_issues)} testing issues")

    # Then fetch "flaky test" and add only new issues not already present
    print("--- Fetching active flaky test issues ---")
    flaky_issues = fetch_github_issues(label="flaky test", state="open")
    existing_issue_numbers = {issue.issue for issue in catalog.active_test_issues}
    new_flaky_issues = process_issues(flaky_issues, is_closed=False)
    added_count = 0
    for issue in new_flaky_issues:
        if issue.issue not in existing_issue_numbers:
            catalog.active_test_issues.append(issue)
            existing_issue_numbers.add(issue.issue)
            added_count += 1
    print(
        f"Added {added_count} new flaky test issues (total: {len(catalog.active_test_issues)})\n"
    )

    # Fetch closed issues with label "testing" from the last day
    print("--- Fetching resolved testing issues ---")
    closed_testing_issues = fetch_github_issues(
        label="testing", state="closed", days_back=1
    )
    catalog.resolved_test_issues = process_issues(closed_testing_issues, is_closed=True)
    print(f"Processed {len(catalog.resolved_test_issues)} resolved testing issues")

    # Then fetch closed "flaky test" and add only new issues not already present
    print("--- Fetching resolved flaky test issues ---")
    closed_flaky_issues = fetch_github_issues(
        label="flaky test", state="closed", days_back=1
    )
    existing_closed_numbers = {issue.issue for issue in catalog.resolved_test_issues}
    new_closed_flaky_issues = process_issues(closed_flaky_issues, is_closed=True)
    added_closed_count = 0
    for issue in new_closed_flaky_issues:
        if issue.issue not in existing_closed_numbers:
            catalog.resolved_test_issues.append(issue)
            existing_closed_numbers.add(issue.issue)
            added_closed_count += 1
    print(
        f"Added {added_closed_count} new resolved flaky test issues (total: {len(catalog.resolved_test_issues)})\n"
    )

    # Print summary by label for active issues
    print("=== Active Issues Summary by Label ===")
    label_counts = {}
    for issue in catalog.active_test_issues:
        for label in issue.labels:
            label_counts[label] = label_counts.get(label, 0) + 1

    # Sort by count descending
    sorted_labels = sorted(label_counts.items(), key=lambda x: x[1], reverse=True)
    for label, count in sorted_labels:
        print(f"  {label}: {count}")
    print()

    # Print summary by label for resolved issues
    print("=== Resolved Issues Summary by Label ===")
    resolved_label_counts = {}
    for issue in catalog.resolved_test_issues:
        for label in issue.labels:
            resolved_label_counts[label] = resolved_label_counts.get(label, 0) + 1

    # Sort by count descending
    sorted_resolved_labels = sorted(
        resolved_label_counts.items(), key=lambda x: x[1], reverse=True
    )
    for label, count in sorted_resolved_labels:
        print(f"  {label}: {count}")
    print()

    return catalog


if __name__ == "__main__":
    results = []
    catalog = None

    def fetch_catalog():
        global catalog
        catalog = fetch_gh_ci_issues()
        catalog.dump()
        return True

    results.append(
        Result.from_commands_run(name="Fetch flaky test issues", command=fetch_catalog)
    )

    if results[-1].is_ok() and catalog is not None:
        # Print summary
        print("\n=== Test Issues Summary ===")
        print(f"Active test_case issues: {len(catalog.active_test_issues)}")
        # print(f"Resolved test_case issues: {len(catalog.resolved_test_issues)}")

        # Print sample of active issues
        if catalog.active_test_issues:
            print("\n--- Sample Active Test Issues ---")
            for issue in catalog.active_test_issues[
                :: len(catalog.active_test_issues) // 5
            ]:
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
