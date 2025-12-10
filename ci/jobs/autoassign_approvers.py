#!/usr/bin/env python3

import json
from datetime import datetime, timedelta
from typing import List, Optional

from ci.praktika.result import Result
from ci.praktika.utils import Shell


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
        
        print(f"  Found {len(prs_without_assignees)} PRs without assignees out of {len(prs)} total open PRs")
        return prs_without_assignees
        
    except json.JSONDecodeError as e:
        print(f"ERROR: Failed to parse JSON response: {e}")
        return []
    except Exception as e:
        print(f"ERROR: Failed to fetch pull requests: {e}")
        return []


def get_approved_prs(prs: List[dict]) -> List[dict]:
    """
    Filter PRs that have at least one approval.

    Args:
        prs: List of PR dictionaries

    Returns:
        List of PRs with approvals, including the first approver
    """
    approved_prs = []
    
    for pr in prs:
        reviews = pr.get("reviews", [])
        
        # Find the first approval
        approver = None
        for review in reviews:
            if review.get("state") == "APPROVED":
                approver = review.get("author", {}).get("login")
                if approver:
                    break
        
        if approver:
            pr["first_approver"] = approver
            approved_prs.append(pr)
    
    print(f"  Found {len(approved_prs)} PRs with approvals")
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
    
    def fetch_and_filter_prs():
        global prs_to_assign
        
        # Fetch PRs without assignees
        prs = fetch_prs_without_assignees()
        
        if not prs:
            print("No PRs without assignees found")
            return True
        
        # Filter for approved PRs
        prs_to_assign = get_approved_prs(prs)
        
        return True
    
    results.append(
        Result.from_commands_run(
            name="Fetch PRs without assignees", 
            command=fetch_and_filter_prs
        )
    )
    
    successful_assignments = 0
    failed_assignments = 0
    
    if results[-1].is_ok() and prs_to_assign:
        
        def assign_approvers():
            global successful_assignments, failed_assignments
            successful_assignments, failed_assignments = process_and_assign_prs(prs_to_assign)
            return failed_assignments == 0  # Success if no failures
        
        results.append(
            Result.from_commands_run(
                name="Assign approvers to PRs",
                command=assign_approvers
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
    Result.create_from(results=results, links=[]).complete_job()
