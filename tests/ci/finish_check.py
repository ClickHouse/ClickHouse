#!/usr/bin/env python3
import logging
from github import Github

from commit_status_helper import (
    CI_STATUS_NAME,
    get_commit,
    get_commit_filtered_statuses,
    post_commit_status,
    update_mergeable_check,
)
from get_robot_token import get_best_robot_token
from pr_info import PRInfo


def main():
    logging.basicConfig(level=logging.INFO)

    pr_info = PRInfo(need_orgs=True)
    gh = Github(get_best_robot_token(), per_page=100)
    # Update the Mergeable Check at the final step
    update_mergeable_check(gh, pr_info, CI_STATUS_NAME)
    commit = get_commit(gh, pr_info.sha)

    statuses = [
        status
        for status in get_commit_filtered_statuses(commit)
        if status.context == CI_STATUS_NAME
    ]
    if not statuses:
        return
    # Take the latest status
    status = statuses[-1]
    if status.state == "pending":
        post_commit_status(
            commit,
            "success",
            status.target_url,
            "All checks finished",
            CI_STATUS_NAME,
            pr_info,
            dump_to_file=True,
        )


if __name__ == "__main__":
    main()
