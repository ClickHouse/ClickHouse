#!/usr/bin/env python

"""Script for automatic sync PRs handling in private repos"""

import sys

from get_robot_token import get_best_robot_token
from pr_info import PRInfo
from github_helper import GitHub


def main():
    gh = GitHub(get_best_robot_token())

    pr_info = PRInfo()
    assert pr_info.merged_pr, "BUG. merged PR number could not been determined"

    prs = gh.get_pulls_from_search(
        query=f"head:sync-upstream/pr/{pr_info.merged_pr} org:ClickHouse type:pr",
        repo="ClickHouse/clickhouse-private",
    )
    if len(prs) > 1:
        print(f"WARNING: More than one PR found [{prs}] - exiting")
        sys.exit(0)
    if len(prs) == 0:
        print("WARNING: No Sync PR found")
        sys.exit(0)

    pr = prs[0]

    if pr.state == "closed":
        print(f"Sync PR [{pr.number}] already closed - exiting")
        sys.exit(0)

    if pr.state != "open":
        print(f"WARNING: Unknown Sync PR [{pr.number}] state [{pr.state}] - exiting")
        sys.exit(0)

    print(f"Trying to merge Sync PR [{pr.number}]")
    if pr.draft:
        gh.toggle_pr_draft(pr)
    pr.merge()


if __name__ == "__main__":
    main()
