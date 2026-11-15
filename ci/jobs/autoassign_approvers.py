#!/usr/bin/env python3
"""
Hourly job that acts on the approvals of open pull requests:

- assigns the first approver to an approved PR that has no assignees;
- adds the `can be tested` label to an approved PR from a fork by an external contributor
  that has no such label;
- adds the `can be tested` label to every open PR of the automated agents listed in
  `AGENT_AUTHORS`, unconditionally.

Only approvals from ClickHouse organization members count.
"""

import json
import shlex
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from ci.praktika.result import Result
from ci.praktika.utils import Shell, Utils

CAN_BE_TESTED = "can be tested"

# GitHub logins of the automated agents whose PRs always get the `can be tested` label.
AGENT_AUTHORS = ["groeneai", "clickgapai", "actueleai"]

PR_FIELDS = "number,title,assignees,author,labels,isCrossRepository"

# The same list that `ci/jobs/scripts/workflow_hooks/trusted.py` uses.
TRUSTED_CONTRIBUTORS_CONFIG = Path(__file__).parents[1] / "defs" / "trusted_contributors.json"


def fetch_org_contributors(org: str = "ClickHouse") -> set:
    """
    Fetch the complete list of organization members using gh CLI.

    The list must be complete: an author missing from it is treated as an external
    contributor, and their approved PR gets the `can be tested` label, which reruns
    the whole CI. So a failure of any page is an error, and the output is not truncated.

    Raises on failure.
    """
    print(f"Fetching organization members of {org}...")
    output = Shell.get_output_or_raise(
        f"gh api orgs/{org}/members --paginate --jq '.[].login'", verbose=True
    )
    members = {line.strip() for line in output.splitlines() if line.strip()}
    if not members:
        raise RuntimeError(f"No organization members of {org} were returned")
    print(f"  Found {len(members)} organization members")
    return members


def has_label(pr: dict, label: str) -> bool:
    return any(item.get("name") == label for item in pr.get("labels", []))


def needs_assignee(pr: dict) -> bool:
    return not pr.get("assignees", [])


def load_trusted_contributors() -> set:
    return {
        login.lower()
        for login in json.loads(TRUSTED_CONTRIBUTORS_CONFIG.read_text(encoding="utf-8"))
    }


def is_external_contributor(pr: dict, org_contributors: set, trusted_contributors: set) -> bool:
    author = (pr.get("author") or {}).get("login") or ""
    if not author:
        # Unknown author: do not label, adding the label is the consequential action.
        return False
    return author not in org_contributors and author.lower() not in trusted_contributors


def needs_can_be_tested(pr: dict, org_contributors: set, trusted_contributors: set) -> bool:
    # The label only gates CI for PRs from forks by external contributors (see
    # `ci/jobs/scripts/workflow_hooks/trusted.py`): PRs from branches of the main repository,
    # and PRs from forks by organization members or trusted contributors are tested anyway.
    # Adding the label to them is not only useless: it makes CI rerun the whole PR.
    return (
        bool(pr.get("isCrossRepository"))
        and not has_label(pr, CAN_BE_TESTED)
        and is_external_contributor(pr, org_contributors, trusted_contributors)
    )


def fetch_recent_prs(
    org_contributors: set, trusted_contributors: set, hours_back: int = 4
) -> List[dict]:
    """
    Fetch the open pull requests updated within the last `hours_back` hours that
    either have no assignees, or come from a fork of an external contributor and have
    no `can be tested` label.

    Whether a PR is approved is decided later, per PR, from its full paginated review
    history: the `reviews` field of `gh pr list` is capped at the first 100 review
    events, so using it as the candidacy gate would drop approvals made late on
    long-lived PRs.

    Raises on failure.
    """
    print(f"Fetching open pull requests updated in the last {hours_back} hours...")

    time_threshold = (datetime.now() - timedelta(hours=hours_back)).strftime(
        "%Y-%m-%dT%H:%M:%S"
    )
    search_query = f"is:pr is:open updated:>{time_threshold}"
    cmd = f"gh pr list --search '{search_query}' --json {PR_FIELDS} --limit 1000"
    output = Shell.get_output_or_raise(cmd, verbose=True)
    prs = json.loads(output) if output else []

    candidates = [
        pr
        for pr in prs
        if needs_assignee(pr) or needs_can_be_tested(pr, org_contributors, trusted_contributors)
    ]
    print(
        f"  Found {len(candidates)} PRs without assignees or without the "
        f"`{CAN_BE_TESTED}` label out of {len(prs)} recently updated open PRs"
    )
    return candidates


def fetch_agent_prs_without_label() -> List[dict]:
    """
    Fetch all open PRs of `AGENT_AUTHORS` that have no `can be tested` label,
    regardless of when they were updated.

    Raises on failure.
    """
    prs = []
    for author in AGENT_AUTHORS:
        search_query = f'-label:"{CAN_BE_TESTED}"'
        cmd = (
            f"gh pr list --state open --author {shlex.quote(author)} "
            f"--search {shlex.quote(search_query)} --json {PR_FIELDS} --limit 1000"
        )
        output = Shell.get_output_or_raise(cmd, verbose=True)
        author_prs = json.loads(output) if output else []
        # The search index may lag behind: filter by the actual labels as well.
        author_prs = [pr for pr in author_prs if not has_label(pr, CAN_BE_TESTED)]
        print(f"  Found {len(author_prs)} open PRs of {author} without the label")
        prs += author_prs
    return prs


def parse_timestamp(timestamp: str) -> Optional[datetime]:
    """
    Parse a GitHub ISO-8601 UTC timestamp, e.g. `2026-06-29T14:10:43Z`.
    """
    if not timestamp:
        return None
    return Utils.to_datetime(timestamp, input_format="iso")


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
    acting on it would contradict the current state of the review.
    `COMMENTED` reviews carry no verdict at all and are ignored, so a comment left
    after an approval does not revoke it.

    Raises on failure: assigning and labeling are the consequential actions, so a PR
    whose review history cannot be read is left alone instead of being acted on blindly.
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


def fetch_removals(pr_number: int) -> Tuple[Dict[str, datetime], Optional[datetime]]:
    """
    Fetch the users that were removed from the PR assignees, with the time of the
    latest removal for each of them, and the time of the latest removal of the
    `can be tested` label.

    Removing an assignee or the label is a deliberate decision - either by a human, or
    by the bot that unassigns assignees inactive for 30 days. Doing it again undoes
    that decision, and, since the approval that triggers it never goes away, it turns
    into an endless remove/add churn, so the caller only acts on an approval that is
    newer than the removal.

    Raises on failure: assigning and labeling are the consequential actions, so a PR
    whose history cannot be read is left alone instead of being acted on blindly.
    """
    label = json.dumps(CAN_BE_TESTED)
    cmd = (
        "gh api repos/{owner}/{repo}/issues/"
        f"{pr_number}"
        "/timeline --paginate --jq '.[] | select(.event == \"unassigned\" "
        f'or (.event == "unlabeled" and .label.name == {label})) '
        "| {event: .event, login: .assignee.login, at: .created_at}'"
    )
    output = Shell.get_output_or_raise(cmd, verbose=True)

    removed_assignees: Dict[str, datetime] = {}
    label_removed_at: Optional[datetime] = None
    for line in output.splitlines():
        line = line.strip()
        if not line:
            continue
        event = json.loads(line)
        at = parse_timestamp(event.get("at"))
        if at is None:
            continue
        if event.get("event") == "unlabeled":
            if label_removed_at is None or at > label_removed_at:
                label_removed_at = at
            continue
        login = event.get("login")
        if not login:
            continue
        if login not in removed_assignees or at > removed_assignees[login]:
            removed_assignees[login] = at

    return removed_assignees, label_removed_at


def approved_after(approved_at: Optional[datetime], removed_at: Optional[datetime]) -> bool:
    if removed_at is None:
        return True
    # Removed before approving - the approval is the newer signal
    return approved_at is not None and removed_at < approved_at


def select_approver_to_assign(
    approvers: Dict[str, Optional[datetime]], removed_assignees: Dict[str, datetime]
) -> Optional[str]:
    """
    Pick the first approver who was not unassigned from the PR after their approval.
    """
    for approver, approved_at in approvers.items():
        removed_at = removed_assignees.get(approver)
        if approved_after(approved_at, removed_at):
            return approver
        print(
            f"  Skipping {approver}: unassigned at {removed_at.isoformat()} "
            "after their approval, assigning them back would undo that"
        )
    return None


def should_add_can_be_tested(
    approvers: Dict[str, Optional[datetime]], label_removed_at: Optional[datetime]
) -> bool:
    """
    The label is added if at least one approval in effect is newer than the latest
    removal of the label.
    """
    if any(approved_after(at, label_removed_at) for at in approvers.values()):
        return True
    print(
        f"  Skipping the `{CAN_BE_TESTED}` label: it was removed at "
        f"{label_removed_at.isoformat()} after every approval, adding it back would undo that"
    )
    return False


def assign_approver_to_pr(pr_number: int, approver: str) -> bool:
    cmd = f"gh pr edit {pr_number} --add-assignee {shlex.quote(approver)}"
    if Shell.check(cmd, verbose=True):
        print(f"  ✓ Assigned {approver} to PR #{pr_number}")
        return True
    print(f"  ✗ Failed to assign {approver} to PR #{pr_number}")
    return False


def add_can_be_tested_label(pr_number: int) -> bool:
    cmd = f"gh pr edit {pr_number} --add-label {shlex.quote(CAN_BE_TESTED)}"
    if Shell.check(cmd, verbose=True):
        print(f"  ✓ Added the `{CAN_BE_TESTED}` label to PR #{pr_number}")
        return True
    print(f"  ✗ Failed to add the `{CAN_BE_TESTED}` label to PR #{pr_number}")
    return False


class Stats:
    def __init__(self):
        self.assigned = 0
        self.labeled = 0
        self.failed = 0
        self.skipped = 0

    def __str__(self):
        return (
            f"assigned: {self.assigned}, labeled: {self.labeled}, "
            f"failed: {self.failed}, skipped: {self.skipped}"
        )


def label_agent_prs(prs: List[dict], stats: Stats) -> set:
    """Returns the numbers of the PRs that were labeled."""
    labeled = set()
    for pr in prs:
        print(f"\nPR #{pr.get('number')} by {pr.get('author', {}).get('login')}: {pr.get('title', '')}")
        if add_can_be_tested_label(pr.get("number")):
            stats.labeled += 1
            labeled.add(pr.get("number"))
        else:
            stats.failed += 1
    return labeled


def process_approved_prs(
    prs: List[dict],
    org_contributors: set,
    trusted_contributors: set,
    stats: Stats,
    already_labeled: set = frozenset(),
) -> None:
    """
    For every PR approved by an org contributor: assign the first approver if the PR
    has no assignees, and add the `can be tested` label if it is from a fork of an
    external contributor and has no such label. `already_labeled` are the PRs that got
    the label earlier in this run.
    """
    for pr in prs:
        pr_number = pr.get("number")
        if pr_number in already_labeled:
            pr = {**pr, "labels": pr.get("labels", []) + [{"name": CAN_BE_TESTED}]}
            if not needs_assignee(pr):
                continue
        print(f"\nPR #{pr_number}: {pr.get('title', '')}")

        try:
            approvers = fetch_approvers(pr_number, org_contributors)
            if not approvers:
                print("  Not approved by an org contributor, leaving the PR as is")
                stats.skipped += 1
                continue
            removed_assignees, label_removed_at = fetch_removals(pr_number)
        except Exception as e:
            print(f"  ✗ Failed to read the history of PR #{pr_number}: {e}")
            stats.failed += 1
            continue

        acted = False

        if needs_assignee(pr):
            approver = select_approver_to_assign(approvers, removed_assignees)
            if approver:
                print(f"  First approver: {approver}")
                acted = True
                if assign_approver_to_pr(pr_number, approver):
                    stats.assigned += 1
                else:
                    stats.failed += 1

        if needs_can_be_tested(
            pr, org_contributors, trusted_contributors
        ) and should_add_can_be_tested(approvers, label_removed_at):
            acted = True
            if add_can_be_tested_label(pr_number):
                stats.labeled += 1
            else:
                stats.failed += 1

        if not acted:
            stats.skipped += 1


if __name__ == "__main__":
    results = []
    stats = Stats()
    org_contributors = set()

    def fetch_contributors():
        global org_contributors
        org_contributors = fetch_org_contributors(org="ClickHouse")
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

    labeled_agent_prs = set()

    def label_agents():
        global labeled_agent_prs
        prs = fetch_agent_prs_without_label()
        failed_before = stats.failed
        labeled_agent_prs = label_agent_prs(prs, stats)
        return stats.failed == failed_before

    results.append(
        Result.from_commands_run(
            name=f"Add `{CAN_BE_TESTED}` to PRs of {', '.join(AGENT_AUTHORS)}",
            command=label_agents,
        )
    )

    def process_approved():
        trusted_contributors = load_trusted_contributors()
        prs = fetch_recent_prs(org_contributors, trusted_contributors)
        failed_before = stats.failed
        process_approved_prs(
            prs, org_contributors, trusted_contributors, stats, labeled_agent_prs
        )
        return stats.failed == failed_before

    results.append(
        Result.from_commands_run(
            name=f"Assign approvers and add `{CAN_BE_TESTED}` to approved PRs",
            command=process_approved,
        )
    )

    print(f"\n=== Summary ===\n{stats}")

    Result.create_from(results=results).complete_job()
