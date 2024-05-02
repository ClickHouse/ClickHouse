#!/usr/bin/env python3
import logging
import sys

from github import Github

from ci_config import StatusNames
from commit_status_helper import (
    get_commit,
    get_commit_filtered_statuses,
    post_commit_status,
    set_mergeable_check,
    trigger_mergeable_check,
)
from get_robot_token import get_best_robot_token
from pr_info import PRInfo
from report import PENDING, SUCCESS


def main():
    logging.basicConfig(level=logging.INFO)

    has_failure = False

    # FIXME: temporary hack to fail Mergeable Check in MQ if pipeline has any failed jobs
    if len(sys.argv) > 1 and sys.argv[1] == "--pipeline-failure":
        has_failure = True

    pr_info = PRInfo(need_orgs=True)
    gh = Github(get_best_robot_token(), per_page=100)
    commit = get_commit(gh, pr_info.sha)
    statuses = None

    if pr_info.is_merge_queue:
        # in MQ Mergeable check status must never be green if any failures in workflow
        if has_failure:
            set_mergeable_check(commit, "workflow failed", "failure")
        else:
            # This must be the only place where green MCheck is set in the MQ (in the end of CI) to avoid early merge
            set_mergeable_check(commit, "workflow passed", "success")
    else:
        statuses = get_commit_filtered_statuses(commit)
        trigger_mergeable_check(commit, statuses)

        statuses = [s for s in statuses if s.context == StatusNames.CI]
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
                StatusNames.CI,
                pr_info,
                dump_to_file=True,
            )


if __name__ == "__main__":
    main()
