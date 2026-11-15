#!/usr/bin/env python3

import json
from datetime import datetime, timedelta
from typing import List, Optional

from ci.praktika.result import Result
from ci.praktika.utils import Shell


def fetch_org_contributors(org: str = "ClickHouse", limit: int = 1000) -> set:
    """
    Fetch organization members using gh CLI.

    Args:
        org: Organization name (default: ClickHouse)
        limit: Maximum number of members to fetch (default: 500)

    Returns:
        Set of organization member usernames
    """
    print(f"Fetching up to {limit} organization members from {org}...")

    try:
        contributors = set()

        # Fetch org members only (not external contributors)
        cmd = f"gh api orgs/{org}/members --paginate --jq '.[].login' | head -n {limit}"
        output = Shell.get_output(cmd, verbose=True)
        if output and output.strip():
            members = [
                line.strip() for line in output.strip().split("\n") if line.strip()
            ]
            contributors.update(members)
            print(f"  Found {len(members)} organization members")

        return contributors

    except Exception as e:
        print(f"ERROR: Failed to fetch organization members: {e}")
        return set()


def fetch_prs_without_assignees(hours_back: int = 4) -> List[dict]:
    """
    Fetch pull requests without assignees using gh CLI.

    Args:
        hours_back: Only fetch PRs updated within this many hours (default: 4)

    Returns:
        List of PR dictionaries
    """
    print(f"Fetching open pull requests updated in the last {hours_back} hours...")

    try:
        # Calculate the time threshold
        time_threshold = (datetime.now() - timedelta(hours=hours_back)).strftime(
            "%Y-%m-%dT%H:%M:%S"
        )

        # Fetch PRs without assignees, include reviews to check for approvals
        # Use search query to filter by update time
        search_query = f"is:pr is:open updated:>{time_threshold}"
        cmd = f"gh pr list --search '{search_query}' --json number,title,assignees,reviews,author --limit 1000"
        output = Shell.get_output(cmd, verbose=True)

        if not output or not output.strip():
            print("  No pull requests found")
            return []

        prs = json.loads(output)

        # Filter PRs without assignees
        prs_without_assignees = [pr for pr in prs if not pr.get("assignees", [])]

        print(
            f"  Found {len(prs_without_assignees)} PRs without assignees out of {len(prs)} total open PRs"
        )
        return prs_without_assignees

    except json.JSONDecodeError as e:
        print(f"ERROR: Failed to parse JSON response: {e}")
        return []
    except Exception as e:
        print(f"ERROR: Failed to fetch pull requests: {e}")
        return []


def get_approved_prs(prs: List[dict], org_contributors: set = None) -> List[dict]:
    """
    Filter PRs that have at least one approval from an org contributor.

    Args:
        prs: List of PR dictionaries
        org_contributors: Set of org contributor usernames (if None, no filtering)

    Returns:
        List of PRs with approvals from org contributors, including the first approver
    """
    approved_prs = []
    skipped_count = 0

    for pr in prs:
        reviews = pr.get("reviews", [])

        # Find the first approval from an org contributor
        approver = None
        for review in reviews:
            if review.get("state") == "APPROVED":
                potential_approver = review.get("author", {}).get("login")
                if potential_approver:
                    # If org_contributors is provided, only accept org members
                    if (
                        org_contributors is None
                        or potential_approver in org_contributors
                    ):
                        approver = potential_approver
                        break
                    else:
                        skipped_count += 1

        if approver:
            pr["first_approver"] = approver
            approved_prs.append(pr)

    print(f"  Found {len(approved_prs)} PRs with approvals from org contributors")
    if org_contributors and skipped_count > 0:
        print(f"  Skipped {skipped_count} approvals from non-org contributors")
    return approved_prs


def assign_approver_to_pr(pr_number: int, approver: str) -> bool:
    """
    Assign an approver to a pull request.

    Args:
        pr_number: PR number
        approver: GitHub username of the approver

    Returns:
        True if successful, False otherwise
    """
    try:
        cmd = f"gh pr edit {pr_number} --add-assignee {approver}"
        output = Shell.get_output(cmd, verbose=True)
        print(f"  ✓ Assigned {approver} to PR #{pr_number}")
        return True
    except Exception as e:
        print(f"  ✗ Failed to assign {approver} to PR #{pr_number}: {e}")
        return False


def process_and_assign_prs(prs: List[dict]) -> tuple[int, int]:
    """
    Process PRs and assign approvers.

    Args:
        prs: List of PR dictionaries with approvals

    Returns:
        Tuple of (successful_assignments, failed_assignments)
    """
    successful = 0
    failed = 0

    if not prs:
        print("No PRs to process")
        return successful, failed

    print(f"\n--- Assigning approvers to {len(prs)} PRs ---")

    for pr in prs:
        pr_number = pr.get("number")
        approver = pr.get("first_approver")
        title = pr.get("title", "")

        print(f"\nPR #{pr_number}: {title}")
        print(f"  First approver: {approver}")

        if assign_approver_to_pr(pr_number, approver):
            successful += 1
        else:
            failed += 1

    return successful, failed


if __name__ == "__main__":
    results = []
    prs_to_assign = []
    org_contributors = set()

    def fetch_contributors():
        global org_contributors
        org_contributors = fetch_org_contributors(org="ClickHouse", limit=1000)
        return len(org_contributors) > 0

    results.append(
        Result.from_commands_run(
            name="Fetch organization contributors", command=fetch_contributors
        )
    )

    if not org_contributors:
        Result.create_from(
            results=results, info="Failed to fetch org members - cannot proceed"
        ).complete_job()

    def fetch_and_filter_prs():
        global prs_to_assign

        # Fetch PRs without assignees
        prs = fetch_prs_without_assignees()

        if not prs:
            print("No PRs without assignees found")
            return True

        # Filter for approved PRs by org contributors
        prs_to_assign = get_approved_prs(prs, org_contributors)

        return True

    results.append(
        Result.from_commands_run(
            name="Fetch PRs without assignees", command=fetch_and_filter_prs
        )
    )

    successful_assignments = 0
    failed_assignments = 0

    if results[-1].is_ok() and prs_to_assign:

        def assign_approvers():
            global successful_assignments, failed_assignments
            successful_assignments, failed_assignments = process_and_assign_prs(
                prs_to_assign
            )
            return failed_assignments == 0  # Success if no failures

        results.append(
            Result.from_commands_run(
                name="Assign approvers to PRs", command=assign_approvers
            )
        )

        # Print summary
        print("\n=== Assignment Summary ===")
        print(f"PRs processed: {len(prs_to_assign)}")
        print(f"Successfully assigned: {successful_assignments}")
        print(f"Failed assignments: {failed_assignments}")
    elif results[-1].is_ok() and not prs_to_assign:
        print("\n=== Summary ===")
        print("No approved PRs without assignees found")
    else:
        print("ERROR: Failed to fetch PRs")

    # Complete the job
    Result.create_from(results=results).complete_job()
