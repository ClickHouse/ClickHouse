#!/usr/bin/env python3

import csv
import os
import time
from typing import Dict, List, Literal, Optional, Union
import logging

from github import Github
from github.GithubObject import _NotSetType, NotSet as NotSet
from github.Commit import Commit
from github.CommitStatus import CommitStatus
from github.IssueComment import IssueComment
from github.Repository import Repository

from ci_config import CI_CONFIG, REQUIRED_CHECKS, CHECK_DESCRIPTIONS, CheckDescription
from env_helper import GITHUB_REPOSITORY, GITHUB_RUN_URL
from pr_info import PRInfo, SKIP_MERGEABLE_CHECK_LABEL
from report import TestResult, TestResults
from s3_helper import S3Helper
from upload_result_helper import upload_results

RETRY = 5
CommitStatuses = List[CommitStatus]
MERGEABLE_NAME = "Mergeable Check"
GH_REPO = None  # type: Optional[Repository]
CI_STATUS_NAME = "CI running"


class RerunHelper:
    def __init__(self, commit: Commit, check_name: str):
        self.check_name = check_name
        self.commit = commit
        self.statuses = get_commit_filtered_statuses(commit)

    def is_already_finished_by_status(self) -> bool:
        # currently we agree even for failed statuses
        for status in self.statuses:
            if self.check_name in status.context and status.state in (
                "success",
                "failure",
            ):
                return True
        return False

    def get_finished_status(self) -> Optional[CommitStatus]:
        for status in self.statuses:
            if self.check_name in status.context:
                return status
        return None


def override_status(status: str, check_name: str, invert: bool = False) -> str:
    if CI_CONFIG["tests_config"].get(check_name, {}).get("force_tests", False):
        return "success"

    if invert:
        if status == "success":
            return "error"
        return "success"

    return status


def get_commit(gh: Github, commit_sha: str, retry_count: int = RETRY) -> Commit:
    for i in range(retry_count):
        try:
            repo = get_repo(gh)
            commit = repo.get_commit(commit_sha)
            break
        except Exception as ex:
            if i == retry_count - 1:
                raise ex
            time.sleep(i)

    return commit


def post_commit_status(
    commit: Commit,
    state: str,
    report_url: Union[_NotSetType, str] = NotSet,
    description: Union[_NotSetType, str] = NotSet,
    check_name: Union[_NotSetType, str] = NotSet,
    pr_info: Optional[PRInfo] = None,
) -> None:
    """The parameters are given in the same order as for commit.create_status,
    if an optional parameter `pr_info` is given, the `set_status_comment` functions
    is invoked to add or update the comment with statuses overview"""
    for i in range(RETRY):
        try:
            commit.create_status(
                state=state,
                target_url=report_url,
                description=description,
                context=check_name,
            )
            break
        except Exception as ex:
            if i == RETRY - 1:
                raise ex
            time.sleep(i)
    if pr_info:
        status_updated = False
        for i in range(RETRY):
            try:
                set_status_comment(commit, pr_info)
                status_updated = True
                break
            except Exception as ex:
                logging.warning(
                    "Failed to update the status commit, will retry %s times: %s",
                    RETRY - i - 1,
                    ex,
                )

        if not status_updated:
            logging.error("Failed to update the status comment, continue anyway")


def set_status_comment(commit: Commit, pr_info: PRInfo) -> None:
    """It adds or updates the comment status to all Pull Requests but for release
    one, so the method does nothing for simple pushes and pull requests with
    `release`/`release-lts` labels"""
    # to reduce number of parameters, the Github is constructed on the fly
    gh = Github()
    gh.__requester = commit._requester  # type:ignore #pylint:disable=protected-access
    repo = get_repo(gh)
    statuses = sorted(get_commit_filtered_statuses(commit), key=lambda x: x.context)
    if not statuses:
        return

    if not [status for status in statuses if status.context == CI_STATUS_NAME]:
        # This is the case, when some statuses already exist for the check,
        # but not the CI_STATUS_NAME. We should create it as pending.
        # W/o pr_info to avoid recursion, and yes, one extra create_ci_report
        post_commit_status(
            commit,
            "pending",
            create_ci_report(pr_info, statuses),
            "The report for running CI",
            CI_STATUS_NAME,
        )

    # We update the report in generate_status_comment function, so do it each
    # run, even in the release PRs and normal pushes
    comment_body = generate_status_comment(pr_info, statuses)
    # We post the comment only to normal and backport PRs
    if pr_info.number == 0 or pr_info.labels.intersection({"release", "release-lts"}):
        return

    comment_service_header = comment_body.split("\n", 1)[0]
    comment = None  # type: Optional[IssueComment]
    pr = repo.get_pull(pr_info.number)
    for ic in pr.get_issue_comments():
        if ic.body.startswith(comment_service_header):
            comment = ic
            break

    if comment is None:
        pr.create_issue_comment(comment_body)
        return

    if comment.body == comment_body:
        logging.info("The status comment is already updated, no needs to change it")
        return
    comment.edit(comment_body)


def generate_status_comment(pr_info: PRInfo, statuses: CommitStatuses) -> str:
    """The method generates the comment body, as well it updates the CI report"""

    def beauty_state(state: str) -> str:
        if state == "success":
            return f"🟢 {state}"
        if state == "pending":
            return f"🟡 {state}"
        if state in ["error", "failure"]:
            return f"🔴 {state}"
        return state

    report_url = create_ci_report(pr_info, statuses)
    worst_state = get_worst_state(statuses)
    if not worst_state:
        # Theoretically possible, although
        # the function should not be used on empty statuses
        worst_state = "The commit doesn't have the statuses yet"
    else:
        worst_state = f"The overall status of the commit is {beauty_state(worst_state)}"

    comment_body = (
        f"<!-- automatic status comment for PR #{pr_info.number} "
        f"from {pr_info.head_name}:{pr_info.head_ref} -->\n"
        f"This is an automated comment for commit {pr_info.sha} with "
        f"description of existing statuses. It's updated for the latest CI running\n"
        f"The full report is available [here]({report_url})\n"
        f"{worst_state}\n\n<table>"
        "<thead><tr><th>Check name</th><th>Description</th><th>Status</th></tr></thead>\n"
        "<tbody>"
    )
    # group checks by the name to get the worst one per each
    grouped_statuses = {}  # type: Dict[CheckDescription, CommitStatuses]
    for status in statuses:
        cd = None
        for c in CHECK_DESCRIPTIONS:
            if c.match_func(status.context):
                cd = c
                break

        if cd is None or cd == CHECK_DESCRIPTIONS[-1]:
            # This is the case for either non-found description or a fallback
            cd = CheckDescription(
                status.context,
                CHECK_DESCRIPTIONS[-1].description,
                CHECK_DESCRIPTIONS[-1].match_func,
            )

        if cd in grouped_statuses:
            grouped_statuses[cd].append(status)
        else:
            grouped_statuses[cd] = [status]

    table_rows = []  # type: List[str]
    for desc, gs in grouped_statuses.items():
        table_rows.append(
            f"<tr><td>{desc.name}</td><td>{desc.description}</td>"
            f"<td>{beauty_state(get_worst_state(gs))}</td></tr>\n"
        )

    table_rows.sort()

    comment_footer = "</table>"
    return "".join([comment_body, *table_rows, comment_footer])


def get_worst_state(statuses: CommitStatuses) -> str:
    worst_status = None
    states = {"error": 0, "failure": 1, "pending": 2, "success": 3}
    for status in statuses:
        if worst_status is None:
            worst_status = status
            continue
        if states[status.state] < states[worst_status.state]:
            worst_status = status
        if worst_status.state == "error":
            break

    if worst_status is None:
        return ""
    return worst_status.state


def create_ci_report(pr_info: PRInfo, statuses: CommitStatuses) -> str:
    """The function converst the statuses to TestResults and uploads the report
    to S3 tests bucket. Then it returns the URL"""
    test_results = []  # type: TestResults
    for status in statuses:
        log_urls = None
        if status.target_url is not None:
            log_urls = [status.target_url]
        test_results.append(TestResult(status.context, status.state, log_urls=log_urls))
    return upload_results(
        S3Helper(), pr_info.number, pr_info.sha, test_results, [], CI_STATUS_NAME
    )


def post_commit_status_to_file(
    file_path: str, description: str, state: str, report_url: str
) -> None:
    if os.path.exists(file_path):
        raise Exception(f'File "{file_path}" already exists!')
    with open(file_path, "w", encoding="utf-8") as f:
        out = csv.writer(f, delimiter="\t")
        out.writerow([state, report_url, description])


def get_commit_filtered_statuses(commit: Commit) -> CommitStatuses:
    """
    Squash statuses to latest state
    1. context="first", state="success", update_time=1
    2. context="second", state="success", update_time=2
    3. context="first", stat="failure", update_time=3
    =========>
    1. context="second", state="success"
    2. context="first", stat="failure"
    """
    filtered = {}
    for status in sorted(commit.get_statuses(), key=lambda x: x.updated_at):
        filtered[status.context] = status
    return list(filtered.values())


def get_repo(gh: Github) -> Repository:
    global GH_REPO
    if GH_REPO is not None:
        return GH_REPO
    GH_REPO = gh.get_repo(GITHUB_REPOSITORY)
    return GH_REPO


def remove_labels(gh: Github, pr_info: PRInfo, labels_names: List[str]) -> None:
    repo = get_repo(gh)
    pull_request = repo.get_pull(pr_info.number)
    for label in labels_names:
        pull_request.remove_from_labels(label)
        pr_info.labels.remove(label)


def post_labels(gh: Github, pr_info: PRInfo, labels_names: List[str]) -> None:
    repo = get_repo(gh)
    pull_request = repo.get_pull(pr_info.number)
    for label in labels_names:
        pull_request.add_to_labels(label)
        pr_info.labels.add(label)


def format_description(description: str) -> str:
    if len(description) > 140:
        description = description[:137] + "..."
    return description


def set_mergeable_check(
    commit: Commit,
    description: str = "",
    state: Literal["success", "failure"] = "success",
) -> None:
    commit.create_status(
        context=MERGEABLE_NAME,
        description=description,
        state=state,
        target_url=GITHUB_RUN_URL,
    )


def update_mergeable_check(gh: Github, pr_info: PRInfo, check_name: str) -> None:
    not_run = (
        pr_info.labels.intersection({SKIP_MERGEABLE_CHECK_LABEL, "release"})
        or check_name not in REQUIRED_CHECKS
        or pr_info.release_pr
        or pr_info.number == 0
    )
    if not_run:
        # Let's avoid unnecessary work
        return

    logging.info("Update Mergeable Check by %s", check_name)

    commit = get_commit(gh, pr_info.sha)
    statuses = get_commit_filtered_statuses(commit)

    required_checks = [
        status for status in statuses if status.context in REQUIRED_CHECKS
    ]

    mergeable_status = None
    for status in statuses:
        if status.context == MERGEABLE_NAME:
            mergeable_status = status
            break

    success = []
    fail = []
    for status in required_checks:
        if status.state == "success":
            success.append(status.context)
        else:
            fail.append(status.context)

    if fail:
        description = "failed: " + ", ".join(fail)
        description = format_description(description)
        if mergeable_status is None or mergeable_status.description != description:
            set_mergeable_check(commit, description, "failure")
        return

    description = ", ".join(success)
    description = format_description(description)
    if mergeable_status is None or mergeable_status.description != description:
        set_mergeable_check(commit, description)
