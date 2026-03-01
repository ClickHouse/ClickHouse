#!/usr/bin/env python3

import dataclasses
import json
import sys
from datetime import datetime, timedelta

sys.path.append(".")
from ci.praktika.utils import Shell

REPOS = [
    "ClickHouse/ClickHouse",
    "ClickHouse/ClickHouse-private",
]

INTERVAL_DAYS = 7


@dataclasses.dataclass
class PullRequest:
    number: int
    title: str
    merged_at: str
    repo: str
    url: str


if __name__ == "__main__":
    result_merged = []  # List[PullRequest]

    if not Shell.check("gh auth status", strict=False):
        print(
            "Error: GitHub CLI is not authenticated. Please run 'gh auth login' first."
        )
        exit(1)

    result_approved = []  # List[PullRequest]
    for repo in REPOS:
        print(f"Processing repository: {repo}")

        try:
            seven_days_ago = (datetime.now() - timedelta(days=INTERVAL_DAYS)).strftime(
                "%Y-%m-%d"
            )
            res = Shell.get_output(
                f"gh pr list --json number,title,mergedAt,headRepository,url --limit 1000 --repo {repo} --author @me --state merged --search 'merged:>{seven_days_ago}'"
            )
            if res:
                prs_data = json.loads(res)
                for pr_data in prs_data:
                    if pr_data.get("mergedAt"):  # Only include actually merged PRs
                        pr = PullRequest(
                            number=pr_data["number"],
                            title=pr_data["title"],
                            merged_at=pr_data["mergedAt"],
                            repo=repo,
                            url=pr_data["url"],
                        )
                        result_merged.append(pr)
        except (json.JSONDecodeError, KeyError) as e:
            print(f"Error processing authored PRs for {repo}: {e}")
            continue

        try:
            print(f"  Checking PRs reviewed by me...")
            seven_days_ago = (datetime.now() - timedelta(days=INTERVAL_DAYS)).strftime(
                "%Y-%m-%d"
            )
            res = Shell.get_output(
                f"gh pr list --json number,title,mergedAt,headRepository,url,author --limit 1000 --repo {repo} --state merged --search 'merged:>{seven_days_ago} reviewed-by:@me'"
            )

            if res and res.strip() != "[]":
                prs_data = json.loads(res)
                current_user = Shell.get_output("gh api user --jq .login").strip()
                for pr_data in prs_data:
                    if pr_data.get("author", {}).get("login") == current_user:
                        continue
                    pr = PullRequest(
                        number=pr_data["number"],
                        title=pr_data["title"],
                        merged_at=pr_data["mergedAt"],
                        url=pr_data["url"],
                        repo=repo,
                    )
                    result_approved.append(pr)
        except (json.JSONDecodeError, KeyError) as e:
            print(f"Error processing approved PRs for {repo}: {e}")
            continue

    print(f"\nSummary:")
    print(f"- Authored and merged: {len(result_merged)} PRs")
    print(f"- Reviewed and merged: {len(result_approved)} PRs")

    # Display results
    print(f"\n=== Merged in the last {INTERVAL_DAYS}d ===")
    if result_merged:
        for pr in result_merged:
            print(f"{pr.url}: {pr.title}")  # - merged at {pr.merged_at}
    else:
        print("No PRs found")

    print(f"\n=== Reviewed in the last {INTERVAL_DAYS}d ===")
    if result_approved:
        for pr in result_approved:
            print(f"{pr.url}: {pr.title}")  # - merged at {pr.merged_at}
    else:
        print("No PRs found")
