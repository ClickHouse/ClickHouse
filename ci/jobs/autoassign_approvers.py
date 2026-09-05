#!/usr/bin/env python3

import json
from datetime import datetime, timedelta
from typing import Dict, List, Optional

from ci.praktika.result import Result
from ci.praktika.utils import Shell, Utils


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

        # Fetch PRs and keep the unassigned ones. Whether a PR is approved is decided
        # later, per PR, from its full paginated review history: the `reviews` field of
        # `gh pr list` is capped at the first 100 review events, so using it as the
        # candidacy gate would drop approvals made late on long-lived PRs.
        search_query = f"is:pr is:open updated:>{time_threshold}"
        cmd = f"gh pr list --search '{search_query}' --json number,title,assignees,author --limit 1000"
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


def parse_timestamp(timestamp: str) -> Optional[datetime]:
    """
    Parse a GitHub ISO-8601 UTC timestamp, e.g. `2026-06-29T14:10:43Z`.
    """
    if not timestamp:
        return None
    return Utils.gh_str_to_datetime(timestamp)


def fetch_approvers(
    pr_number: int, org_contributors: set = None
) -> Dict[str, Optional[datetime]]:
    """
    Fetch the full, paginated review history of a PR and return the org contributors
    whose approval is still in effect, in chronological order of the first approval,
    mapped to the time of their latest approval.

    The nested `reviews` field from `gh pr list` is capped at the first 100 review
    events, so on a long-lived PR a re-approval made after an unassignment, or an
    approver who joined later, would be invisible there - and the job would skip or
    miss them forever. This paginated per-PR fetch sees the complete history.

    The whole review stream is collapsed per reviewer, and only reviewers whose
    latest verdict is still `APPROVED` are returned: an approval that was later
    replaced by `CHANGES_REQUESTED`, or dismissed, is not an approval any more, and
    assigning its author would contradict the current state of the review.
    `COMMENTED` reviews carry no verdict at all and are ignored, so a comment left
    after an approval does not revoke it.

    Raises on failure: assigning is the consequential action, so a PR whose review
    history cannot be read is left alone instead of being assigned blindly.
    """
    cmd = (
        "gh api repos/{owner}/{repo}/pulls/"
        f"{pr_number}"
        "/reviews --paginate --jq '.[] | select(.state != \"COMMENTED\" "
        'and .state != "PENDING") '
        "| {login: .user.login, state: .state, at: .submitted_at}'"
    )
    output = Shell.get_output_or_raise(cmd, verbose=True)

    # Reviews come in chronological order, so the stored state ends up being the
    # latest verdict of each reviewer, and the stored timestamp - keyed in the order
    # of the first approval - the time of their latest approval.
    states: Dict[str, str] = {}
    approved_at: Dict[str, Optional[datetime]] = {}
    for line in output.splitlines():
        line = line.strip()
        if not line:
            continue
        review = json.loads(line)
        login = review.get("login")
        if not login:
            continue
        if org_contributors is not None and login not in org_contributors:
            continue
        state = review.get("state")
        states[login] = state
        if state == "APPROVED":
            approved_at[login] = parse_timestamp(review.get("at"))

    approvers: Dict[str, Optional[datetime]] = {}
    for login, at in approved_at.items():
        if states[login] != "APPROVED":
            print(
                f"  Skipping {login}: their latest review is {states[login]}, "
                "their approval is not in effect any more"
            )
            continue
        approvers[login] = at

    return approvers


def fetch_removed_assignees(pr_number: int) -> Dict[str, datetime]:
    """
    Fetch the users that were removed from the PR assignees, with the time of the
    latest removal for each of them.

    Removing an assignee is a deliberate decision - either by a human, or by the bot
    that unassigns assignees inactive for 30 days. Assigning such a user again undoes
    that decision, and, since the approval that makes them a candidate here never goes
    away, it turns into an endless unassign/assign churn, so the caller skips them.

    Raises on failure: assigning is the consequential action, so a PR whose assignment
    history cannot be read is left alone instead of being assigned blindly.
    """
    cmd = (
        "gh api repos/{owner}/{repo}/issues/"
        f"{pr_number}"
        "/timeline --paginate --jq '.[] | select(.event == \"unassigned\") "
        "| {login: .assignee.login, at: .created_at}'"
    )
    output = Shell.get_output_or_raise(cmd, verbose=True)

    removed: Dict[str, datetime] = {}
    for line in output.splitlines():
        line = line.strip()
        if not line:
            continue
        event = json.loads(line)
        login = event.get("login")
        at = parse_timestamp(event.get("at"))
        if not login or at is None:
            continue
        if login not in removed or at > removed[login]:
            removed[login] = at

    return removed


def select_approver_to_assign(pr: dict, org_contributors: set = None) -> Optional[str]:
    """
    Pick the first approver who was not unassigned from the PR after their approval.

    Args:
        pr: PR dictionary of an unassigned PR
        org_contributors: Set of org contributor usernames (if None, no filtering)

    Returns:
        The login to assign, or None if the PR has no approval from an org contributor,
        or every approver was unassigned deliberately
    """
    approvers = fetch_approvers(pr.get("number"), org_contributors)
    if not approvers:
        return None

    removed_assignees = fetch_removed_assignees(pr.get("number"))

    for approver, approved_at in approvers.items():
        removed_at = removed_assignees.get(approver)
        if removed_at is None:
            return approver
        if approved_at is not None and removed_at < approved_at:
            # Removed before approving - the approval is the newer signal, assign again
            return approver
        print(
            f"  Skipping {approver}: unassigned at {removed_at.isoformat()} "
            "after their approval, assigning them back would undo that"
        )

    return None


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
        Shell.get_output(cmd, verbose=True)
        print(f"  ✓ Assigned {approver} to PR #{pr_number}")
        return True
    except Exception as e:
        print(f"  ✗ Failed to assign {approver} to PR #{pr_number}: {e}")
        return False


def process_and_assign_prs(
    prs: List[dict], org_contributors: set = None
) -> tuple[int, int, int]:
    """
    Process PRs and assign approvers.

    Args:
        prs: List of PR dictionaries of the unassigned PRs; the ones without an approval
            from an org contributor are skipped
        org_contributors: Set of org contributor usernames (if None, no filtering)

    Returns:
        Tuple of (successful_assignments, failed_assignments, skipped_prs)
    """
    successful = 0
    failed = 0
    skipped = 0

    if not prs:
        print("No PRs to process")
        return successful, failed, skipped

    print(f"\n--- Checking {len(prs)} PRs without assignees ---")

    for pr in prs:
        pr_number = pr.get("number")
        title = pr.get("title", "")

        print(f"\nPR #{pr_number}: {title}")

        try:
            approver = select_approver_to_assign(pr, org_contributors)
        except Exception as e:
            print(f"  ✗ Failed to read the history of PR #{pr_number}: {e}")
            failed += 1
            continue

        if not approver:
            print("  No approver to assign, leaving the PR unassigned")
            skipped += 1
            continue

        print(f"  First approver: {approver}")

        if assign_approver_to_pr(pr_number, approver):
            successful += 1
        else:
            failed += 1

    return successful, failed, skipped


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

        # Fetch PRs without assignees. Their approvals are read per PR, from the full
        # paginated review history, by `select_approver_to_assign`.
        prs_to_assign = fetch_prs_without_assignees()

        if not prs_to_assign:
            print("No PRs without assignees found")

        return True

    results.append(
        Result.from_commands_run(
            name="Fetch PRs without assignees", command=fetch_and_filter_prs
        )
    )

    successful_assignments = 0
    failed_assignments = 0
    skipped_prs = 0

    if results[-1].is_ok() and prs_to_assign:

        def assign_approvers():
            global successful_assignments, failed_assignments, skipped_prs
            (
                successful_assignments,
                failed_assignments,
                skipped_prs,
            ) = process_and_assign_prs(prs_to_assign, org_contributors)
            return failed_assignments == 0  # Success if no failures

        results.append(Result.from_commands_run(name="Assign approvers to PRs", command=assign_approvers))

        # Print summary
        print("\n=== Assignment Summary ===")
        print(f"PRs processed: {len(prs_to_assign)}")
        print(f"Successfully assigned: {successful_assignments}")
        print(f"Failed assignments: {failed_assignments}")
        print(f"Skipped (not approved, or approvers unassigned): {skipped_prs}")
    elif results[-1].is_ok() and not prs_to_assign:
        print("\n=== Summary ===")
        print("No PRs without assignees found")
    else:
        print("ERROR: Failed to fetch PRs")

    # Complete the job
    Result.create_from(results=results).complete_job()
