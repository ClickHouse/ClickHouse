#!/usr/bin/env python3
"""
Adds the `ready-for-backport` label to PRs that satisfy all of the following:
  1. Carry a backport label (`pr-must-backport`, `pr-must-backport-force`,
     `pr-critical-bugfix`, or a version-specific `vX.Y-must-backport`).
  2. Were merged more than READY_DELAY_DAYS days ago, giving time for any
     potential revert to land first.
  3. Have not been reverted (no merged PR with title `Revert "..."` and
     body `Reverts {repo}#<N>` found).
  4. Do not already carry `ready-for-backport` or `pr-backports-created`.

The label is later consumed by `cherry_pick.py`, which skips PRs without it.
"""

import argparse
import json
import shlex
import sys
from datetime import date, timedelta
from typing import Any, Dict, List

from pathlib import Path

_repo_root = Path(__file__).parents[3]  # ci/jobs/scripts/ -> repo root
sys.path.insert(0, str(_repo_root))
sys.path.insert(0, str(_repo_root / "ci"))
from jobs.scripts.workflow_hooks.pr_labels_and_category import Labels
from praktika.utils import Shell

READY_DELAY_DAYS = 7


def gh_search(query: str, per_page: int = 100, max_results: int = 1000) -> List[Dict[str, Any]]:
    """Run a GitHub Issues search and return all matching items, handling pagination."""
    all_items = []  # type: List[Dict[str, Any]]
    page = 1
    while True:
        output = Shell.get_output(
            f"gh api -X GET search/issues "
            f"-f q={shlex.quote(query)} "
            f"-F per_page={per_page} "
            f"-F page={page}",
            verbose=True,
        )
        if not output:
            break
        data = json.loads(output)
        items = data.get("items", [])
        if not items:
            break
        all_items.extend(items)
        if (
            len(all_items) >= data.get("total_count", 0)
            or len(items) < per_page
            or len(all_items) >= max_results
        ):
            break
        page += 1
    return all_items


def get_release_branches(repo: str) -> List[str]:
    """Return head-branch names of all open release PRs."""
    output = Shell.get_output(
        f"gh pr list --repo {repo} "
        f"--label {shlex.quote(Labels.RELEASE)} "
        f"--state open --json headRefName --limit 50",
        verbose=True,
    )
    if not output:
        return []
    return [pr["headRefName"] for pr in json.loads(output)]


def find_revert_pr(repo: str, pr_number: int) -> Dict[str, Any]:
    """Return the revert PR item if one exists, otherwise an empty dict.

    GitHub-generated revert PRs have the title `Revert "<original title>"`
    (no PR number in the title) and a body containing `Reverts {repo}#<N>`.

    Three layers of protection against false positives:
    - Body search for the exact `Reverts {repo}#{pr_number}` string — this is
      the canonical GitHub revert body format and uniquely identifies the target.
    - Self-match exclusion — skip any result whose number equals pr_number.
    - Title validation — require the title to start with `Revert "` to confirm
      it is a GitHub-generated revert PR, not a PR that merely mentions the
      revert reference in passing.
    """
    query = f'type:pr repo:{repo} is:merged "Reverts {repo}#{pr_number}"'
    items = gh_search(query, per_page=10, max_results=10)
    for item in items:
        if item["number"] == pr_number:
            continue  # self-match
        if item.get("title", "").startswith('Revert "'):
            return item
    return {}


def mark_ready(repo: str, pr_number: int, dry_run: bool) -> None:
    label = shlex.quote(Labels.READY_FOR_BACKPORT)
    if dry_run:
        print(f"  DRY RUN: would add {Labels.READY_FOR_BACKPORT!r} to PR #{pr_number}")
        return
    print(f"  Adding {Labels.READY_FOR_BACKPORT!r} to PR #{pr_number}")
    Shell.run(
        f"gh pr edit {pr_number} --add-label {label} --repo {repo}",
        verbose=True,
    )


def remove_backport_labels(
    repo: str, pr_number: int, pr_labels: List[str], all_backport_labels: List[str], dry_run: bool
) -> None:
    to_remove = [l for l in pr_labels if l in set(all_backport_labels) | {Labels.READY_FOR_BACKPORT}]
    if not to_remove:
        return
    remove_args = " ".join(f"--remove-label {shlex.quote(l)}" for l in to_remove)
    if dry_run:
        print(f"  DRY RUN: would remove labels {to_remove} from PR #{pr_number}")
        return
    print(f"  Removing labels {to_remove} from PR #{pr_number}")
    Shell.run(
        f"gh pr edit {pr_number} {remove_args} --repo {repo}",
        verbose=True,
    )


def main() -> int:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--repo", default="ClickHouse/ClickHouse", help="owner/name")
    parser.add_argument("--dry-run", action="store_true", help="do not apply any labels")
    args = parser.parse_args()

    repo = args.repo
    cutoff = (date.today() - timedelta(days=READY_DELAY_DAYS)).isoformat()

    release_branches = get_release_branches(repo)
    version_labels = [
        f"v{branch.replace('release/', '')}-must-backport"
        for branch in release_branches
    ]
    # Comma-separated label list is OR in GitHub Issues search.
    all_backport_labels = (
        [Labels.MUST_BACKPORT, Labels.MUST_BACKPORT_FORCE, Labels.PR_CRITICAL_BUGFIX]
        + version_labels
    )
    label_filter = ",".join(all_backport_labels)

    updated_since = (date.today() - timedelta(days=90)).isoformat()
    query = (
        f"type:pr repo:{repo} is:merged "
        f"label:{label_filter} "
        f"-label:{Labels.PR_BACKPORTS_CREATED} "
        f"-label:{Labels.READY_FOR_BACKPORT} "
        f"merged:2020-01-01..{cutoff} "
        f"updated:>={updated_since}"
    )
    print(f"Search query: {query}")
    candidates = gh_search(query)
    print(f"Found {len(candidates)} candidate PRs")

    for pr in candidates:
        number = pr["number"]
        title = pr["title"]
        pr_labels = [l["name"] for l in pr.get("labels", [])]
        print(f"Checking PR #{number}: {title}")
        revert_pr = find_revert_pr(repo, number)
        if revert_pr:
            print(
                f"  Reverted by PR #{revert_pr['number']}: {revert_pr.get('title', '')} "
                f"({revert_pr.get('html_url', '')})"
            )
            remove_backport_labels(repo, number, pr_labels, all_backport_labels, args.dry_run)
            continue
        mark_ready(repo, number, args.dry_run)

    return 0


if __name__ == "__main__":
    sys.exit(main())
