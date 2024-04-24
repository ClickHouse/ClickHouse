#!/usr/bin/env python3
import logging

from github import Github

from commit_status_helper import (
    CI_STATUS_NAME,
    get_commit,
    get_commit_filtered_statuses,
    post_commit_status,
    trigger_mergeable_check,
)
from get_robot_token import get_best_robot_token
from pr_info import PRInfo
from report import PENDING, SUCCESS


def main():
    logging.basicConfig(level=logging.INFO)

    pr_info = PRInfo(need_orgs=True)
    gh = Github(get_best_robot_token(), per_page=100)
    commit = get_commit(gh, pr_info.sha)
    # Unconditionally update the Mergeable Check at the final step
    statuses = get_commit_filtered_statuses(commit)
    trigger_mergeable_check(commit, statuses)

    if not pr_info.is_merge_queue:
        statuses = [s for s in statuses if s.context == CI_STATUS_NAME]
        if not statuses:
            return
        # Take the latest status
        status = statuses[-1]
        if status.state == PENDING:
            post_commit_status(
                commit,
                SUCCESS,
                status.target_url,
                "All checks finished",
                CI_STATUS_NAME,
                pr_info,
                dump_to_file=True,
            )


if __name__ == "__main__":
    main()
