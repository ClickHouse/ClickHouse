#!/usr/bin/env python3

import csv
import os
import time
from typing import List, Literal, Optional
import logging

from github import Github
from github.Commit import Commit
from github.CommitStatus import CommitStatus
from github.IssueComment import IssueComment
from github.Repository import Repository

from ci_config import CI_CONFIG, REQUIRED_CHECKS, CHECK_DESCRIPTIONS
from env_helper import GITHUB_REPOSITORY, GITHUB_RUN_URL
from pr_info import PRInfo, SKIP_MERGEABLE_CHECK_LABEL

RETRY = 5
CommitStatuses = List[CommitStatus]
MERGEABLE_NAME = "Mergeable Check"
GH_REPO = None  # type: Optional[Repository]


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
    report_url: str,
    description: str,
    check_name: str,
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
        set_status_comment(commit, pr_info)


def set_status_comment(commit: Commit, pr_info: PRInfo) -> None:
    """It adds or updates the comment status to all Pull Requests but for release
    one, so the method does nothing for simple pushes and pull requests with
    `release`/`release-lts` labels"""
    if pr_info.number == 0 or pr_info.labels.intersection({"release", "release-lts"}):
        return
    # to reduce number of parameters, the Github is constructed on the fly
    gh = Github()
    gh.__requester = commit._requester  # type:ignore #pylint:disable=protected-access
    repo = get_repo(gh)
    pr = repo.get_pull(pr_info.number)
    comment_header = f"<!-- commit {commit.sha} automatic status comment -->\n"
    comment = None  # type: Optional[IssueComment]
    for ic in pr.get_issue_comments():
        if ic.body.startswith(comment_header):
            comment = ic
            break

    statuses_limit = 25  # the arbitrary limit to use <details> html tag
    statuses = sorted(get_commit_filtered_statuses(commit), key=lambda x: x.context)
    details = ""
    details_ending = ""
    if statuses_limit < len(statuses):
        details = "<details><summary>Show the statuses</summary>\n\n"
        details_ending = "\n\n</details>"
    comment_body = (
        f"This is an automated comment for commit `{commit.sha}` with "
        f"description of existing statuses\n\n{details}"
        "<table><thead><tr><th>Check name</th><th>Description</th><th>Status</th>"
        "<th>Status message</th><th>Optional report URL</th></tr></thead>\n<tbody>"
    )
    table_rows = []  # type: List[str]
    for status in statuses:
        description = ""  # it's impossible to have nothing, but safer to define
        for cd in CHECK_DESCRIPTIONS:
            if cd.match_func(status.context):
                description = cd.description
                break

        if status.state == "success":
            state = "🟢 "
        elif status.state == "pending":
            state = "🟡 "
        elif status.state in ["error", "failure"]:
            state = "🔴 "
        else:
            state = ""

        if status.target_url:
            link = f'<a href="{status.target_url}">Report</a>'
        else:
            link = ""

        table_rows.append(
            f"<tr><td>{status.context}</td><td>{description}</td>"
            f"<td>{state}{status.state}</td><td>{status.description}</td>"
            f"<td>{link}</td></tr>\n"
        )

    comment_footer = f"</table>{details_ending}"
    comment_body = "".join([comment_header, comment_body, *table_rows, comment_footer])
    if comment is not None:
        comment.edit(comment_body)
        return
    pr.create_issue_comment(comment_body)


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
        if success:
            description += "; succeeded: " + ", ".join(success)
        description = format_description(description)
        if mergeable_status is None or mergeable_status.description != description:
            set_mergeable_check(commit, description, "failure")
        return

    description = ", ".join(success)
    description = format_description(description)
    if mergeable_status is None or mergeable_status.description != description:
        set_mergeable_check(commit, description)
