#!/usr/bin/env python3

import csv
import json
import logging
import time
from collections import defaultdict
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Callable, Dict, List, Optional, Union

from github import Github
from github.Commit import Commit
from github.CommitStatus import CommitStatus
from github.GithubException import GithubException
from github.GithubObject import NotSet
from github.IssueComment import IssueComment
from github.Repository import Repository

from ci_config import CI
from env_helper import GITHUB_REPOSITORY, TEMP_PATH
from pr_info import PRInfo
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
GH_REPO = None  # type: Optional[Repository]
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
    state: StatusType,  # do not change it, it MUST be StatusType and nothing else
    report_url: Optional[str] = None,
    description: Optional[str] = None,
    check_name: Optional[str] = None,
    pr_info: Optional[PRInfo] = None,
    dump_to_file: bool = False,
) -> CommitStatus:
    """The parameters are given in the same order as for commit.create_status,
    if an optional parameter `pr_info` is given, the `set_status_comment` functions
    is invoked to add or update the comment with statuses overview"""
    for i in range(RETRY):
        try:
            commit_status = commit.create_status(
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
    if pr_info and check_name not in (
        CI.StatusNames.MERGEABLE,
        CI.StatusNames.CI,
        CI.StatusNames.PR_CHECK,
        CI.StatusNames.SYNC,
    ):
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

    return commit_status


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
    statuses = [
        status
        for status in statuses
        if status.context
        not in (
            CI.StatusNames.MERGEABLE,
            CI.StatusNames.CI,
            CI.StatusNames.PR_CHECK,
            CI.StatusNames.SYNC,
        )
    ]
    if not statuses:
        return

    if not [status for status in statuses if status.context == CI.StatusNames.CI]:
        # This is the case, when some statuses already exist for the check,
        # but not the CI.StatusNames.CI. We should create it as pending.
        # W/o pr_info to avoid recursion, and yes, one extra create_ci_report
        post_commit_status(
            commit,
            PENDING,
            create_ci_report(pr_info, statuses),
            "The report for running CI",
            CI.StatusNames.CI,
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
    """The function converts the statuses to TestResults and uploads the report
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
        S3Helper(), pr_info.number, pr_info.sha, test_results, [], CI.StatusNames.CI
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
    global GH_REPO  # pylint:disable=global-statement
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
) -> CommitStatus:
    report_url = ""
    return post_commit_status(
        commit,
        state,
        report_url,
        format_description(description),
        CI.StatusNames.MERGEABLE,
    )


def trigger_mergeable_check(
    commit: Commit,
    statuses: CommitStatuses,
    set_from_sync: bool = False,
    workflow_failed: bool = False,
) -> StatusType:
    """calculate and update CI.StatusNames.MERGEABLE"""
    required_checks = [status for status in statuses if CI.is_required(status.context)]

    mergeable_status = None
    for status in statuses:
        if status.context == CI.StatusNames.MERGEABLE:
            mergeable_status = status
            break

    success = []
    fail = []
    pending = []
    for status in required_checks:
        if status.state == SUCCESS:
            success.append(status.context)
        elif status.state == PENDING:
            pending.append(status.context)
        else:
            fail.append(status.context)

    state: StatusType = SUCCESS

    if fail:
        description = "failed: " + ", ".join(fail)
        state = FAILURE
    elif workflow_failed:
        description = "check workflow failures"
        state = FAILURE
    elif pending:
        description = "pending: " + ", ".join(pending)
        state = PENDING
    else:
        # all good
        description = ", ".join(success)

    description = format_description(description)

    if set_from_sync:
        # update Mergeable Check from sync WF only if its status already present or its new status is FAILURE
        #   to avoid false-positives
        if mergeable_status or state == FAILURE:
            set_mergeable_check(commit, description, state)
    elif mergeable_status is None or mergeable_status.description != description:
        set_mergeable_check(commit, description, state)

    return state


def update_upstream_sync_status(
    pr_info: PRInfo,
    state: StatusType,
) -> None:
    last_synced_upstream_commit = pr_info.get_latest_sync_commit()

    logging.info(
        "Using commit [%s] to post the [%s] status [%s]",
        last_synced_upstream_commit.sha,
        state,
        CI.StatusNames.SYNC,
    )
    if state == SUCCESS:
        description = CI.SyncState.COMPLETED
    else:
        description = CI.SyncState.TESTS_FAILED

    post_commit_status(
        last_synced_upstream_commit,
        state,
        "",
        description,
        CI.StatusNames.SYNC,
    )
    trigger_mergeable_check(
        last_synced_upstream_commit,
        get_commit_filtered_statuses(last_synced_upstream_commit),
        set_from_sync=True,
    )


@dataclass
class CheckDescription:
    name: str
    description: str  # the check descriptions, will be put into the status table
    match_func: Callable[[str], bool]  # the function to check vs the commit status

    def __hash__(self) -> int:
        return hash(self.name + self.description)


CHECK_DESCRIPTIONS = [
    CheckDescription(
        CI.StatusNames.PR_CHECK,
        "Checks correctness of the PR's body",
        lambda x: x == CI.StatusNames.PR_CHECK,
    ),
    CheckDescription(
        CI.StatusNames.SYNC,
        "If it fails, ask a maintainer for help",
        lambda x: x == CI.StatusNames.SYNC,
    ),
    CheckDescription(
        "AST fuzzer",
        "Runs randomly generated queries to catch program errors. "
        "The build type is optionally given in parenthesis. "
        "If it fails, ask a maintainer for help",
        lambda x: x.startswith("AST fuzzer"),
    ),
    CheckDescription(
        CI.JobNames.BUGFIX_VALIDATE,
        "Checks that either a new test (functional or integration) or there "
        "some changed tests that fail with the binary built on master branch",
        lambda x: x == CI.JobNames.BUGFIX_VALIDATE,
    ),
    CheckDescription(
        CI.StatusNames.CI,
        "A meta-check that indicates the running CI. Normally, it's in <b>success</b> or "
        "<b>pending</b> state. The failed status indicates some problems with the PR",
        lambda x: x == "CI running",
    ),
    CheckDescription(
        "Builds",
        "Builds ClickHouse in various configurations for use in further steps. "
        "You have to fix the builds that fail. Build logs often has enough "
        "information to fix the error, but you might have to reproduce the failure "
        "locally. The <b>cmake</b> options can be found in the build log, grepping for "
        '<b>cmake</b>. Use these options and follow the <a href="'
        'https://clickhouse.com/docs/en/development/build">general build process</a>',
        lambda x: x.startswith("ClickHouse") and x.endswith("build check"),
    ),
    CheckDescription(
        "Compatibility check",
        "Checks that <b>clickhouse</b> binary runs on distributions with old libc "
        "versions. If it fails, ask a maintainer for help",
        lambda x: x.startswith("Compatibility check"),
    ),
    CheckDescription(
        CI.JobNames.DOCKER_SERVER,
        "The check to build and optionally push the mentioned image to docker hub",
        lambda x: x.startswith("Docker server"),
    ),
    CheckDescription(
        CI.JobNames.DOCKER_KEEPER,
        "The check to build and optionally push the mentioned image to docker hub",
        lambda x: x.startswith("Docker keeper"),
    ),
    CheckDescription(
        CI.JobNames.DOCS_CHECK,
        "Builds and tests the documentation",
        lambda x: x == CI.JobNames.DOCS_CHECK,
    ),
    CheckDescription(
        CI.JobNames.FAST_TEST,
        "Normally this is the first check that is ran for a PR. It builds ClickHouse "
        'and runs most of <a href="https://clickhouse.com/docs/en/development/tests'
        '#functional-tests">stateless functional tests</a>, '
        "omitting some. If it fails, further checks are not started until it is fixed. "
        "Look at the report to see which tests fail, then reproduce the failure "
        'locally as described <a href="https://clickhouse.com/docs/en/development/'
        'tests#functional-test-locally">here</a>',
        lambda x: x == CI.JobNames.FAST_TEST,
    ),
    CheckDescription(
        "Flaky tests",
        "Checks if new added or modified tests are flaky by running them repeatedly, "
        "in parallel, with more randomization. Functional tests are run 100 times "
        "with address sanitizer, and additional randomization of thread scheduling. "
        "Integration tests are run up to 10 times. If at least once a new test has "
        "failed, or was too long, this check will be red. We don't allow flaky tests, "
        'read <a href="https://clickhouse.com/blog/decorating-a-christmas-tree-with-'
        'the-help-of-flaky-tests/">the doc</a>',
        lambda x: "tests flaky check" in x,
    ),
    CheckDescription(
        "Install packages",
        "Checks that the built packages are installable in a clear environment",
        lambda x: x.startswith("Install packages ("),
    ),
    CheckDescription(
        "Integration tests",
        "The integration tests report. In parenthesis the package type is given, "
        "and in square brackets are the optional part/total tests",
        lambda x: x.startswith("Integration tests ("),
    ),
    CheckDescription(
        CI.StatusNames.MERGEABLE,
        "Checks if all other necessary checks are successful",
        lambda x: x == CI.StatusNames.MERGEABLE,
    ),
    CheckDescription(
        "Performance Comparison",
        "Measure changes in query performance. The performance test report is "
        'described in detail <a href="https://github.com/ClickHouse/ClickHouse/tree'
        '/master/docker/test/performance-comparison#how-to-read-the-report">here</a>. '
        "In square brackets are the optional part/total tests",
        lambda x: x.startswith("Performance Comparison"),
    ),
    CheckDescription(
        "Push to Dockerhub",
        "The check for building and pushing the CI related docker images to docker hub",
        lambda x: x.startswith("Push") and "to Dockerhub" in x,
    ),
    CheckDescription(
        "Sqllogic",
        "Run clickhouse on the "
        '<a href="https://www.sqlite.org/sqllogictest">sqllogic</a> '
        "test set against sqlite and checks that all statements are passed",
        lambda x: x.startswith("Sqllogic test"),
    ),
    CheckDescription(
        "SQLancer",
        "Fuzzing tests that detect logical bugs with "
        '<a href="https://github.com/sqlancer/sqlancer">SQLancer</a> tool',
        lambda x: x.startswith("SQLancer"),
    ),
    CheckDescription(
        "Stateful tests",
        "Runs stateful functional tests for ClickHouse binaries built in various "
        "configurations -- release, debug, with sanitizers, etc",
        lambda x: x.startswith("Stateful tests ("),
    ),
    CheckDescription(
        "Stateless tests",
        "Runs stateless functional tests for ClickHouse binaries built in various "
        "configurations -- release, debug, with sanitizers, etc",
        lambda x: x.startswith("Stateless tests ("),
    ),
    CheckDescription(
        "Stress test",
        "Runs stateless functional tests concurrently from several clients to detect "
        "concurrency-related errors",
        lambda x: x.startswith("Stress test ("),
    ),
    CheckDescription(
        CI.JobNames.STYLE_CHECK,
        "Runs a set of checks to keep the code style clean. If some of tests failed, "
        "see the related log from the report",
        lambda x: x == CI.JobNames.STYLE_CHECK,
    ),
    CheckDescription(
        "Unit tests",
        "Runs the unit tests for different release types",
        lambda x: x.startswith("Unit tests ("),
    ),
    CheckDescription(
        "Upgrade check",
        "Runs stress tests on server version from last release and then tries to "
        "upgrade it to the version from the PR. It checks if the new server can "
        "successfully startup without any errors, crashes or sanitizer asserts",
        lambda x: x.startswith("Upgrade check ("),
    ),
    CheckDescription(
        "ClickBench",
        "Runs [ClickBench](https://github.com/ClickHouse/ClickBench/) with instant-attach table",
        lambda x: x.startswith("ClickBench"),
    ),
    CheckDescription(
        "Fallback for unknown",
        "There's no description for the check yet, please add it to "
        "tests/ci/ci_config.py:CHECK_DESCRIPTIONS",
        lambda x: True,
    ),
]
