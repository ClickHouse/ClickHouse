import json
import os
import re
import traceback
from pathlib import Path

from praktika.info import Info
from praktika.utils import Shell

SYNC_REPO = "ClickHouse/clickhouse-private"

# Matches only the force-merge commit the merge queue creates for each PR, e.g.
#   Merge pull request #77106 from XXXX
# Anchored at the start so it does NOT match subjects that merely contain the
# phrase, such as a Revert (`Revert "Merge pull request #12345 from ..."`) or a
# cherry-picked merge commit.
MERGE_COMMIT_RE = re.compile(r"^Merge pull request #(\d+)")


def get_linked_pr_numbers():
    """
    Extract linked PR numbers from the GitHub push event payload.

    The merge queue can merge several PRs in a single batch, so a single push to
    master may contain multiple merge commits, each corresponding to a different
    upstream PR (and therefore a different Sync PR). The push event payload lists
    every commit in the batch under "commits" (oldest first, head commit last),
    so it is a reliable source for all of them - unlike the head commit message
    or local git state, which only reflect the latest PR.

    The batch's merge commits sit contiguously at the tip of the push, so walk
    from the head commit backwards and stop at the first commit that is not a
    merge-queue merge commit. This picks up exactly the batch and avoids matching
    unrelated commits deeper in the push (e.g. a revert of an old merge commit).

    The result is returned oldest-to-newest (public batch merge order) so the
    Sync PRs are replayed into the private repo in the same order they were
    merged into public master.
    """
    event_file_path = os.getenv("GITHUB_EVENT_PATH", "")
    if not event_file_path or not Path(event_file_path).is_file():
        msg = f"GITHUB_EVENT_PATH is not set or missing: '{event_file_path}' - skipping Sync PR merge"
        print(f"WARNING: {msg}")
        Info().add_workflow_warning(msg)
        return []

    with open(event_file_path, "r", encoding="utf-8") as f:
        github_event = json.load(f)

    commits = github_event.get("commits", [])
    if not commits:
        msg = "No commits found in the push event payload - skipping Sync PR merge"
        print(f"WARNING: {msg}")
        Info().add_workflow_warning(msg)
        return []

    pr_numbers = []
    for commit in reversed(commits):
        match = MERGE_COMMIT_RE.match(commit.get("message", ""))
        if not match:
            break
        pr_number = int(match.group(1))
        if pr_number not in pr_numbers:
            pr_numbers.append(pr_number)
    # Collected newest-to-oldest while walking from the head; reverse back to the
    # public batch merge order (oldest-to-newest) before returning.
    pr_numbers.reverse()
    return pr_numbers


def merge_sync_pr(linked_pr_number):
    # Keep the full JSON array (do not extract with --jq): `gh pr list` exits 0
    # with an empty result when nothing matches, and returning "[]" lets us tell
    # "no open Sync PR" (a quiet no-op) apart from a genuine retrieval failure
    # (empty stdout after retries).
    raw = Shell.get_output(
        f"gh pr list --state open --head sync-upstream/pr/{linked_pr_number} --repo {SYNC_REPO} --json number",
        verbose=True,
        retries=5,
    )
    if not raw:
        msg = f"Failed to retrieve Sync PR list for pr {linked_pr_number} after retries - skipping merge"
        print(f"WARNING: {msg}")
        Info().add_workflow_warning(msg)
        return

    try:
        sync_pr_numbers = [pr["number"] for pr in json.loads(raw)]
    except (json.JSONDecodeError, KeyError, TypeError):
        msg = f"Failed to parse Sync PR list for pr {linked_pr_number}: {raw!r}"
        print(f"WARNING: {msg}")
        Info().add_workflow_warning(msg)
        return

    if len(sync_pr_numbers) == 0:
        # Expected on reruns (Sync PR already merged) or before the Sync PR is
        # created - not a problem, so keep it quiet.
        print(f"No open Sync PR found for pr {linked_pr_number} - nothing to merge")
        return

    if len(sync_pr_numbers) > 1:
        msg = f"Expected at most one open Sync PR for branch sync-upstream/pr/{linked_pr_number}, found {len(sync_pr_numbers)}: {sync_pr_numbers} - skipping merge"
        print(f"WARNING: {msg}")
        Info().add_workflow_warning(msg)
        return

    sync_pr_number = sync_pr_numbers[0]

    if not Shell.check(f"gh pr ready {sync_pr_number} --repo {SYNC_REPO}", verbose=True, retries=5):
        msg = f"Failed to set Sync PR {sync_pr_number} as ready"
        print(f"WARNING: {msg}")
        Info().add_workflow_warning(msg)
        return

    if not Shell.check(
        f"gh pr merge {sync_pr_number} --repo {SYNC_REPO} --merge",
        verbose=True,
        retries=5,
    ):
        msg = f"Failed to merge Sync PR {sync_pr_number}"
        print(f"WARNING: {msg}")
        Info().add_workflow_warning(msg)
        return

    print(f"Sync PR {sync_pr_number} merged (for pr {linked_pr_number})")


def check():
    linked_pr_numbers = get_linked_pr_numbers()

    if not linked_pr_numbers:
        # Expected no-op for any push whose tip is not a merge-queue merge commit
        # (e.g. direct automation commits to master) - nothing to sync. Genuine
        # payload-read failures are already reported inside get_linked_pr_numbers.
        print("No linked PR numbers found in the push event - nothing to sync")
        return

    print(f"Found linked PR numbers to sync: {linked_pr_numbers}")
    for linked_pr_number in linked_pr_numbers:
        merge_sync_pr(linked_pr_number)


if __name__ == "__main__":
    try:
        check()
    except Exception:
        traceback.print_exc()
