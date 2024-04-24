#!/usr/bin/env python3

import csv
import json
import logging
import time
from collections import defaultdict
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Dict, List, Optional, Union

from github import Github
from github.Commit import Commit
from github.CommitStatus import CommitStatus
from github.GithubException import GithubException
from github.GithubObject import NotSet
from github.IssueComment import IssueComment
from github.Repository import Repository

from ci_config import CHECK_DESCRIPTIONS, REQUIRED_CHECKS, CheckDescription
from env_helper import GITHUB_REPOSITORY, GITHUB_RUN_URL, TEMP_PATH
from pr_info import SKIP_MERGEABLE_CHECK_LABEL, PRInfo
from report import (
    ERROR,
    FAILURE,
    PENDING,
    SUCCESS,
    StatusType,
    TestResult,
    TestResults,
    get_worst_status,
)
from s3_helper import S3Helper
from upload_result_helper import upload_results

RETRY = 5
CommitStatuses = List[CommitStatus]
MERGEABLE_NAME = "Mergeable Check"
GH_REPO = None  # type: Optional[Repository]
CI_STATUS_NAME = "CI running"
STATUS_FILE_PATH = Path(TEMP_PATH) / "status.json"


class RerunHelper:
    def __init__(self, commit: Commit, check_name: str):
        self.check_name = check_name
        self.commit = commit
        self.statuses = get_commit_filtered_statuses(commit)

    def is_already_finished_by_status(self) -> bool:
        # currently we agree even for failed statuses
        for status in self.statuses:
            if self.check_name in status.context and status.state in (
                SUCCESS,
                FAILURE,
            ):
                return True
        return False

    def get_finished_status(self) -> Optional[CommitStatus]:
        for status in self.statuses:
            if self.check_name in status.context:
                return status
        return None


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
    state: StatusType,
    report_url: Optional[str] = None,
    description: Optional[str] = None,
    check_name: Optional[str] = None,
    pr_info: Optional[PRInfo] = None,
    dump_to_file: bool = False,
) -> None:
    """The parameters are given in the same order as for commit.create_status,
    if an optional parameter `pr_info` is given, the `set_status_comment` functions
    is invoked to add or update the comment with statuses overview"""
    for i in range(RETRY):
        try:
            commit.create_status(
                state=state,
                target_url=report_url if report_url is not None else NotSet,
                description=description if description is not None else NotSet,
                context=check_name if check_name is not None else NotSet,
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
    if dump_to_file:
        assert pr_info
        CommitStatusData(
            status=state,
            description=description or "",
            report_url=report_url or "",
            sha=pr_info.sha,
            pr_num=pr_info.number,
        ).dump_status()


STATUS_ICON_MAP = defaultdict(
    str,
    {
        ERROR: "❌",
        FAILURE: "❌",
        PENDING: "⏳",
        SUCCESS: "✅",
    },
)


def set_status_comment(commit: Commit, pr_info: PRInfo) -> None:
    """It adds or updates the comment status to all Pull Requests but for release
    one, so the method does nothing for simple pushes and pull requests with
    `release`/`release-lts` labels"""

    if pr_info.is_merge_queue:
        # skip report creation for the MQ
        return

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
            PENDING,
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

    report_url = create_ci_report(pr_info, statuses)
    worst_state = get_worst_state(statuses)

    comment_body = (
        f"<!-- automatic status comment for PR #{pr_info.number} "
        f"from {pr_info.head_name}:{pr_info.head_ref} -->\n"
        f"*This is an automated comment for commit {pr_info.sha} with "
        f"description of existing statuses. It's updated for the latest CI running*\n\n"
        f"[{STATUS_ICON_MAP[worst_state]} Click here]({report_url}) to open a full report in a separate page\n"
        f"\n"
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

    table_header = (
        "<table>\n"
        "<thead><tr><th>Check name</th><th>Description</th><th>Status</th></tr></thead>\n"
        "<tbody>\n"
    )
    table_footer = "<tbody>\n</table>\n"

    details_header = "<details><summary>Successful checks</summary>\n"
    details_footer = "</details>\n"

    visible_table_rows = []  # type: List[str]
    hidden_table_rows = []  # type: List[str]
    for desc, gs in grouped_statuses.items():
        state = get_worst_state(gs)
        state_text = f"{STATUS_ICON_MAP[state]} {state}"
        # take the first target_url with the worst state
        for status in gs:
            if status.target_url and status.state == state:
                state_text = f'<a href="{status.target_url}">{state_text}</a>'
                break

        table_row = (
            f"<tr><td>{desc.name}</td><td>{desc.description}</td>"
            f"<td>{state_text}</td></tr>\n"
        )
        if state == SUCCESS:
            hidden_table_rows.append(table_row)
        else:
            visible_table_rows.append(table_row)

    result = [comment_body]

    if visible_table_rows:
        visible_table_rows.sort()
        result.append(table_header)
        result.extend(visible_table_rows)
        result.append(table_footer)

    if hidden_table_rows:
        hidden_table_rows.sort()
        result.append(details_header)
        result.append(table_header)
        result.extend(hidden_table_rows)
        result.append(table_footer)
        result.append(details_footer)

    return "".join(result)


def get_worst_state(statuses: CommitStatuses) -> StatusType:
    return get_worst_status(status.state for status in statuses)


def create_ci_report(pr_info: PRInfo, statuses: CommitStatuses) -> str:
    """The function converst the statuses to TestResults and uploads the report
    to S3 tests bucket. Then it returns the URL"""
    test_results = []  # type: TestResults
    for status in statuses:
        log_urls = []
        if status.target_url is not None:
            log_urls.append(status.target_url)
        raw_logs = status.description or None
        test_results.append(
            TestResult(
                status.context, status.state, log_urls=log_urls, raw_logs=raw_logs
            )
        )
    return upload_results(
        S3Helper(), pr_info.number, pr_info.sha, test_results, [], CI_STATUS_NAME
    )


def post_commit_status_to_file(
    file_path: Path, description: str, state: str, report_url: str
) -> None:
    if file_path.exists():
        raise FileExistsError(f'File "{file_path}" already exists!')
    with open(file_path, "w", encoding="utf-8") as f:
        out = csv.writer(f, delimiter="\t")
        out.writerow([state, report_url, description])


@dataclass
class CommitStatusData:
    """
    if u about to add/remove fields in this class be causious that it dumps/loads to/from files (see it's method)
    - you might want to add default values for new fields so that it won't break with old files
    """

    status: str
    report_url: str
    description: str
    sha: str = "deadbeaf"
    pr_num: int = -1

    @classmethod
    def _filter_dict(cls, data: dict) -> Dict:
        return {k: v for k, v in data.items() if k in cls.__annotations__.keys()}

    @classmethod
    def load_from_file(cls, file_path: Union[Path, str]):  # type: ignore
        res = {}
        with open(file_path, "r", encoding="utf-8") as json_file:
            res = json.load(json_file)
        return CommitStatusData(**cls._filter_dict(res))

    @classmethod
    def load_status(cls):  # type: ignore
        return cls.load_from_file(STATUS_FILE_PATH)

    @classmethod
    def exist(cls) -> bool:
        return STATUS_FILE_PATH.is_file()

    def dump_status(self) -> None:
        STATUS_FILE_PATH.parent.mkdir(parents=True, exist_ok=True)
        self.dump_to_file(STATUS_FILE_PATH)

    def dump_to_file(self, file_path: Union[Path, str]) -> None:
        file_path = Path(file_path) or STATUS_FILE_PATH
        with open(file_path, "w", encoding="utf-8") as json_file:
            json.dump(asdict(self), json_file)

    def is_ok(self):
        return self.status == SUCCESS

    def is_failure(self):
        return self.status == FAILURE

    @staticmethod
    def cleanup():
        STATUS_FILE_PATH.unlink(missing_ok=True)


def get_commit_filtered_statuses(commit: Commit) -> CommitStatuses:
    """
    Squash statuses to latest state
    1. context="first", state=SUCCESS, update_time=1
    2. context="second", state=SUCCESS, update_time=2
    3. context="first", stat=FAILURE, update_time=3
    =========>
    1. context="second", state=SUCCESS
    2. context="first", stat=FAILURE
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
        try:
            pull_request.remove_from_labels(label)
        except GithubException as exc:
            if not (
                exc.status == 404
                and isinstance(exc.data, dict)
                and exc.data.get("message", "") == "Label does not exist"
            ):
                raise
            logging.warning(
                "The label '%s' does not exist in PR #%s", pr_info.number, label
            )
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
    state: StatusType = SUCCESS,
) -> None:
    commit.create_status(
        context=MERGEABLE_NAME,
        description=format_description(description),
        state=state,
        target_url=GITHUB_RUN_URL,
    )


def update_mergeable_check(commit: Commit, pr_info: PRInfo, check_name: str) -> None:
    "check if the check_name in REQUIRED_CHECKS and then trigger update"
    not_run = (
        pr_info.labels.intersection({SKIP_MERGEABLE_CHECK_LABEL, "release"})
        or check_name not in REQUIRED_CHECKS
        or pr_info.release_pr
        or pr_info.number == 0
    )

    # FIXME: For now, always set mergeable check in the Merge Queue. It's required to pass MQ
    if not_run and not pr_info.is_merge_queue:
        # Let's avoid unnecessary work
        return

    logging.info("Update Mergeable Check by %s", check_name)

    statuses = get_commit_filtered_statuses(commit)
    trigger_mergeable_check(commit, statuses)


def trigger_mergeable_check(commit: Commit, statuses: CommitStatuses) -> None:
    """calculate and update MERGEABLE_NAME"""
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
        if status.state == SUCCESS:
            success.append(status.context)
        else:
            fail.append(status.context)

    state: StatusType = SUCCESS

    if success:
        description = ", ".join(success)
    else:
        description = "awaiting job statuses"

    if fail:
        description = "failed: " + ", ".join(fail)
        state = FAILURE
    description = format_description(description)

    if mergeable_status is None or mergeable_status.description != description:
        set_mergeable_check(commit, description, state)
