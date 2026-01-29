#!/usr/bin/env python3
import argparse
import logging

from github import Github

from ci_config import CI
from commit_status_helper import (
    get_commit,
    get_commit_filtered_statuses,
    post_commit_status,
)
from get_robot_token import get_best_robot_token
from pr_info import PRInfo
from report import FAILURE, PENDING, SUCCESS, StatusType


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        description="Script to merge the given PR. Additional checks for approved "
        "status and green commit statuses could be done",
    )
    parser.add_argument(
        "--wf-status",
        type=str,
        default="",
        help="overall workflow status [success|failure]",
    )
    return parser.parse_args()


def main():
    logging.basicConfig(level=logging.INFO)
    args = parse_args()

    has_workflow_failures = args.wf_status == FAILURE

    pr_info = PRInfo(need_orgs=True)
    gh = Github(get_best_robot_token(), per_page=100)
    commit = get_commit(gh, pr_info.sha)

    statuses = get_commit_filtered_statuses(commit)

    ci_running_statuses = [s for s in statuses if s.context == CI.StatusNames.CI]
    if not ci_running_statuses:
        return
    # Take the latest status
    ci_status = ci_running_statuses[-1]

    has_failure = False
    has_pending = False
    error_cnt = 0
    for status in statuses:
        if status.context in (
            CI.StatusNames.MERGEABLE,
            CI.StatusNames.CI,
            CI.StatusNames.SYNC,
        ):
            # do not account these statuses
            continue
        if status.state == PENDING:
            has_pending = True
        elif status.state != SUCCESS:
            has_failure = True
            error_cnt += 1

    ci_state = SUCCESS  # type: StatusType
    description = "All checks finished"
    if has_failure:
        ci_state = FAILURE
        description = f"All checks finished. {error_cnt} jobs failed"
    elif has_workflow_failures:
        ci_state = FAILURE
        description = "All checks finished. Workflow has failures."
    elif has_pending:
        print("ERROR: CI must not have pending jobs by the time of finish check")
        description = "ERROR: workflow has pending jobs"
        ci_state = FAILURE

    post_commit_status(
        commit,
        ci_state,
        ci_status.target_url,
        description,
        CI.StatusNames.CI,
        pr_info,
        dump_to_file=True,
    )


if __name__ == "__main__":
    main()
