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
    update_upstream_sync_status,
)
from get_robot_token import get_best_robot_token
from pr_info import PRInfo
from report import PENDING, SUCCESS, FAILURE
from synchronizer_utils import SYNC_BRANCH_PREFIX
from env_helper import GITHUB_REPOSITORY, GITHUB_UPSTREAM_REPOSITORY


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
        state = trigger_mergeable_check(commit, statuses, set_if_green=True)

        # Process upstream StatusNames.SYNC
        if (
            pr_info.head_ref.startswith(f"{SYNC_BRANCH_PREFIX}/pr/")
            and GITHUB_REPOSITORY != GITHUB_UPSTREAM_REPOSITORY
        ):
            upstream_pr_number = int(pr_info.head_ref.split("/pr/", maxsplit=1)[1])
            update_upstream_sync_status(
                upstream_pr_number,
                pr_info.number,
                gh,
                state,
                can_set_green_mergeable_status=True,
            )

        ci_running_statuses = [s for s in statuses if s.context == StatusNames.CI]
        if not ci_running_statuses:
            return
        # Take the latest status
        ci_status = ci_running_statuses[-1]

        has_failure = False
        has_pending = False
        for status in statuses:
            if status.context in (StatusNames.MERGEABLE, StatusNames.CI):
                # do not account these statuses
                continue
            if status.state == PENDING:
                if status.context == StatusNames.SYNC:
                    # do not account sync status if pending - it's a different WF
                    continue
                has_pending = True
            elif status.state == SUCCESS:
                continue
            else:
                has_failure = True

        ci_state = SUCCESS
        if has_failure:
            ci_state = FAILURE
        elif has_pending:
            print("ERROR: CI must not have pending jobs by the time of finish check")
            ci_state = FAILURE

        if ci_status.state == PENDING:
            post_commit_status(
                commit,
                ci_state,
                ci_status.target_url,
                "All checks finished",
                StatusNames.CI,
                pr_info,
                dump_to_file=True,
            )


if __name__ == "__main__":
    main()
