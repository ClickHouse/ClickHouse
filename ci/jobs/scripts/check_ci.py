import json
import re
import sys
import time
import traceback
from dataclasses import dataclass
from typing import List, Optional, Tuple
from urllib.parse import quote

sys.path.append("./")

from ci.praktika.gh import GH
from ci.praktika.interactive import UserPrompt
from ci.praktika.result import Result
from ci.praktika.utils import Shell


class CheckStatuses:
    PR = "PR"
    CH_INC_SYNC = "CH Inc sync"
    MERGEABLE_CHECK = "Mergeable Check"


FORCE_MERGE = True


pr_number = None
head_sha = None


class JobTypes:
    STATELESS = "Stateless"
    INTEGRATION = "Integration"
    AST_FUZZER = "AST Fuzzer"
    BUILD = "Build"
    FORMATTER = "Formatter"
    BUZZ_FUZZER = "Buzz"
    DOCKER = "Docker"
    COMPATIBILITY = "Compatibility"
    INSTALL = "Install"
    STRESS = "Stress"
    UPGRADE = "Upgrade"
    PERFORMANCE = "Performance"
    FINISH_WORKFLOW = "Finish Workflow"
    DOCKER_TEST_IMAGES = "Docker test images"
    BUGFIX_VALIDATION_FUNCTIONAL = "Bugfix validation functional"
    CLICKBENCH = "ClickBench"


# TODO: make the list empty
JOBS_USER_CAN_IGNORE = [
    JobTypes.BUGFIX_VALIDATION_FUNCTIONAL,
    JobTypes.STRESS,
    JobTypes.UPGRADE,
    JobTypes.PERFORMANCE,
]
# TODO: make the list empty
TEST_NAMES_USER_CAN_IGNORE = ["Scrapping", "Killed", "Fatal messages", "Server died"]


@dataclass
class CIFailure:
    job_name: str
    job_status: str
    test_name: str
    test_status: str
    test_info: str
    praktika_result: Result
    issue: Optional[GH.GHIssue] = None
    job_type: str = ""
    ignorable: bool = False
    is_not_completed: bool = False

    def __post_init__(self):
        if "Stateless" in self.job_name:
            self.job_type = JobTypes.STATELESS
        elif "Integration" in self.job_name:
            self.job_type = JobTypes.INTEGRATION
        elif "AST" in self.job_name:
            self.job_type = JobTypes.AST_FUZZER
        elif "Buzz" in self.job_name:
            self.job_type = JobTypes.BUZZ_FUZZER
        elif self.job_name.startswith("Build"):
            self.job_type = JobTypes.BUILD
        elif (
            "Docker server image" in self.job_name
            or "Docker keeper image" in self.job_name
        ):
            self.job_type = JobTypes.DOCKER
        elif "Compatibility" in self.job_name:
            self.job_type = JobTypes.COMPATIBILITY
        elif "Install" in self.job_name:
            self.job_type = JobTypes.INSTALL
        elif JobTypes.CLICKBENCH in self.job_name:
            self.job_type = JobTypes.CLICKBENCH
        elif "Stress" in self.job_name:
            self.job_type = JobTypes.STRESS
        elif "Upgrade" in self.job_name:
            self.job_type = JobTypes.UPGRADE
        elif "Bugfix validation (functional tests)" in self.job_name:
            self.job_type = JobTypes.BUGFIX_VALIDATION_FUNCTIONAL
        elif "Performance" in self.job_name:
            self.job_type = JobTypes.PERFORMANCE
        elif self.job_name.startswith("Dockers Build"):
            self.job_type = JobTypes.DOCKER_TEST_IMAGES
        elif "Finish Workflow" in self.job_name:
            self.job_type = JobTypes.FINISH_WORKFLOW
        else:
            raise Exception(f"Unknown job type for job name: {self.job_name}")

        if self.job_status in (
            Result.Status.RUNNING,
            Result.Status.PENDING,
            Result.Status.DROPPED,
        ):
            # User agreed to proceed with running job
            self.ignorable = True
            self.is_not_completed = True
            self.praktika_result.set_comment("IGNORED")

        if self.job_type in JOBS_USER_CAN_IGNORE:
            self.ignorable = True
            self.praktika_result.set_comment("IGNORED")

        if any(t in self.test_name for t in TEST_NAMES_USER_CAN_IGNORE):
            self.ignorable = True
            self.praktika_result.set_comment("IGNORED")

        # self.issue_url = self.praktika_result.get_hlabel_link("issue") or ""
        self.issue_url = ""  # do not set issue_url for now to fetch it from GH and match the failure reason from the issue boddy
        self.labels = self.praktika_result.ext.get("labels", [])
        self.cidb_link = self.praktika_result.get_hlabel_link("cidb") or ""

    def __str__(self):
        res = f"[{self.test_status or self.job_status}]"
        res += f" {self.job_name}"
        if self.test_name:
            res += f": {self.test_name}"
        if self.labels:
            res += f"\n    Flags: {', '.join(self.labels)}"
        if self.issue_url:
            res += f"\n    Issue: {self.issue_url}"
        return res

    def __repr__(self):
        res = f"\n - test output:\n"
        res += self.praktika_result.get_info_truncated(
            truncate_from_top=False, max_info_lines_cnt=20, max_line_length=200
        )
        res += f"\n - flags: {', '.join(self.labels) or 'not flaged'}"
        res += f"\n - issue: {self.issue_url or 'not found'}"
        res += f"\n - cidb: {self.cidb_link or 'not found'}"
        return res

    @staticmethod
    def group_by_job(
        failures: List["CIFailure"],
    ) -> List[Tuple[str, List["CIFailure"]]]:
        grouped = {}
        for failure in failures:
            if failure.job_name not in grouped:
                grouped[failure.job_name] = []
            grouped[failure.job_name].append(failure)
        return list(grouped.items())

    @classmethod
    def get_job_report_url(cls, pr_number, head_sha, job_name=""):
        quoted_job_name = quote(job_name, safe="")
        if not pr_number:
            res = (
                "https://s3.amazonaws.com/clickhouse-test-reports/json.html"
                f"?REF=master&sha={head_sha}&name_0=MasterCI"
            )
        else:
            res = (
                "https://s3.amazonaws.com/clickhouse-test-reports/json.html"
                f"?PR={pr_number}&sha={head_sha}&name_0=PR"
            )
        if job_name:
            res += f"&name_1={quoted_job_name}"
        return res

    def _create_and_link_gh_issue(
        self, title: str, body: str, labels: List[str]
    ) -> bool:
        """
        Helper method to create a GitHub issue and link it to this failure.

        Args:
            title: Issue title
            body: Issue body (markdown)
            labels: List of labels to apply

        Returns:
            bool: True if issue was created successfully, False otherwise
        """
        print(f"\nIssue to create:")
        print("-" * 100)
        print(f"- Title:\n  {title}")
        print(f"- Labels:\n  {', '.join(labels)}")
        print(f"- Body:\n{body}")
        print("-" * 100)
        body = (
            "_Important: This issue was automatically created and is used by CI. "
            "Do not modify the issue title. Do not remove issue labels._\n\n"
        ) + body

        if not UserPrompt.confirm("Proceed with issue creation?"):
            return False

        issue_url = GH.create_issue(
            title, body, labels, repo="ClickHouse/ClickHouse", verbose=True
        )
        if issue_url:
            print(f"Issue {issue_url} created")
            self.issue_url = issue_url
            self.praktika_result.set_clickable_label("issue", issue_url)
        else:
            raise Exception("Failed to create issue")
        return True

    def create_gh_issue_on_flaky_or_broken_test(self):
        if self.issue_url:
            assert False, "BUG: Issue URL already exists, cannot create duplicate"

        print(repr(self))

        test_name = self.test_name
        if "[" in self.test_name:
            # Despite parameter might be valuable for failure reproduction, drop it for simplicity
            test_name = test_name.split("[")[0]

        failure_reason = UserPrompt.get_string(
            "Enter the exact error text from the test output above that identifies the failure.\n"
            "This must be a substring from the output (e.g., 'Logical error', 'Result differs').\n"
            "It will be used to match and group similar failures",
            validator=lambda x: x in self.praktika_result.info,
        )
        title = f"Flaky test: {test_name}"
        body = f"""\
Failure reason: {failure_reason}
CI report: [{self.job_name}]({self.get_job_report_url(pr_number, head_sha, self.job_name)})
CIDB statistics: [cidb]({self.cidb_link})

Test output:
```
{self.praktika_result.get_info_truncated(truncate_from_top=False, max_info_lines_cnt=50, max_line_length=200)}
```
"""
        labels = ["testing", "flaky test"]

        return self._create_and_link_gh_issue(title, body, labels)

    def create_gh_issue_on_fuzzer_or_stress_finding(self):
        if self.issue_url:
            assert False, "BUG"

        title = self.test_name
        body = f"""\
CI report: [{self.job_name}]({self.get_job_report_url(pr_number, head_sha, self.job_name)})
CIDB statistics: [cidb]({self.cidb_link})

Test output:
```
{self.praktika_result.get_info_truncated(truncate_from_top=False, max_info_lines_cnt=200, max_line_length=200)}
```
"""
        labels = ["testing", "fuzz"]

        return self._create_and_link_gh_issue(title, body, labels)

    def create_issue(self):
        """
        Interactively create GitHub issues for failures that don't have existing issues.

        Returns:
            bool: True if issue was created successfully, False otherwise
        """
        if self.issue_url:
            raise AssertionError(
                "BUG: Issue URL is already set, this should be a known issue"
            )

        if self.job_type in (JobTypes.STATELESS, JobTypes.INTEGRATION):
            if not re.match(r"^(\d{5}|test)_", self.test_name):
                raise Exception(f"Unsupported test name format: {self.test_name}")
            if self.create_gh_issue_on_flaky_or_broken_test():
                return True
        elif self.job_type in (JobTypes.BUZZ_FUZZER, JobTypes.AST_FUZZER):
            if self.create_gh_issue_on_fuzzer_or_stress_finding():
                return True
        else:
            raise Exception(f"Unsupported job type: {self.job_type}")
        return False

    def can_process(self):
        if self.job_type in (JobTypes.STATELESS, JobTypes.INTEGRATION):
            if not re.match(r"^(\d{5}|test)_", self.test_name):
                print(
                    f"Only regular test failures can be handled, not [{self.test_name}] - skip"
                )
                return False
        if self.job_type in (JobTypes.DOCKER_TEST_IMAGES):
            print("It's likely infrastructure problem - cannot handle it")
            return False
        if self.job_type in (JobTypes.PERFORMANCE, JobTypes.STRESS):
            print("Job type is not supported yet")
            return False
        if self.job_type in (JobTypes.FINISH_WORKFLOW):
            print("This issue should be fixed before merge - cannot handle it")
            return False
        if "OOM" in self.test_name:
            print("Cannot handle OOM errors - skip")
            return False
        if self.job_status == Result.Status.ERROR:
            print("Cannot handle jobs with status error - skip")
            return False
        return True

    def check_issue(self):
        """
        Check if this failure has an existing open GitHub issue.

        Returns:
            bool: True if an existing issue was found and linked, False otherwise
        """
        assert self.test_name
        check_failure_reason = False  # Flag to check if failure reason matches

        if self.job_type == JobTypes.BUILD:
            search_in_title = self.job_name
            labels = ["build"]
        elif self.job_type in (JobTypes.STATELESS, JobTypes.INTEGRATION):
            search_in_title = self.test_name
            if "[" in self.test_name:
                search_in_title = self.test_name.split("[")[0]
            labels = ["flaky test"]
            check_failure_reason = True  # Flag to check if failure reason matches
        elif self.job_type in (JobTypes.BUZZ_FUZZER, JobTypes.AST_FUZZER):
            search_in_title = self.test_name
            labels = ["fuzz"]
        else:
            raise Exception(f"Unsupported job type: {self.job_type}")

        # Calculate hours since CI start (or default to 24 if not available)
        global ci_start_time
        if ci_start_time:
            hours_since_start = int((time.time() - ci_start_time) / 3600) + 1
        else:
            hours_since_start = 24  # Default fallback

        print(f"Searching for issues with title: {search_in_title}")
        issues = GH.find_issue(
            title=search_in_title,
            labels=labels,
            repo="ClickHouse/ClickHouse",
            verbose=True,
            include_closed_hours=hours_since_start,
        )

        if issues and len(issues) > 1:
            print("WARNING: Multiple issues found - check for duplicates")
            for issue in issues:
                state_label = " [CLOSED]" if issue.is_closed else " [OPEN]"
                print(f"  #{issue.number}{state_label}: {issue.title}")

        issue = issues[0] if issues else None
        if issue:
            state_label = " [CLOSED]" if issue.is_closed else " [OPEN]"
            print(f"Found existing issue #{issue.number}{state_label}: {issue.title}")

            # For flaky tests, verify the failure reason matches
            if check_failure_reason:
                # Extract failure reason from issue body
                # Pattern: "Failure reason: <reason>" or "Failure reason: Reason: <reason>"
                failure_reason_match = re.search(
                    r"Failure reason:\s*(?:Reason:\s*)?(.+?)(?:\n|$)",
                    issue.body or "",
                    re.IGNORECASE,
                )

                if failure_reason_match:
                    expected_reason = failure_reason_match.group(1).strip()
                    current_info = self.praktika_result.info or ""

                    if expected_reason in current_info:
                        print(f"      --> Failure reason matches: '{expected_reason}'")
                        self.issue_url = issue.html_url
                        self.praktika_result.set_clickable_label(
                            "issue", issue.html_url
                        )
                        return True
                    else:
                        print(
                            f"      --> WARNING: Issue exists but failure reason does not match"
                        )
                        print(f"      --> Expected reason: '{expected_reason}'")
                        print(
                            f"      --> Current failure info does not contain this reason"
                        )
                        return False
                else:
                    print(
                        f"      --> WARNING: Could not extract failure reason from issue body"
                    )

            # For other job types or if no reason check needed, just link the issue
            self.issue_url = issue.html_url
            self.praktika_result.set_clickable_label("issue", issue.html_url)
            return True

        return False


@dataclass
class JobFailuresList:
    job_name: str
    known_failures: List[CIFailure]
    unknown_failures: List[CIFailure]
    is_finished: bool


class JobResultProcessor:
    failure_counter = 0

    @staticmethod
    def process_job_failures(job_failures: JobFailuresList):
        """
        Process and categorize job failures interactively.

        Handles known, unknown, and unprocessed failures for a job.
        Allows user to check for existing issues or create new ones.

        Args:
            job_failures: JobFailuresList containing all failure types for a job
        """
        print(
            f"\n=== {job_failures.job_name}. {len(job_failures.unknown_failures)} unknown failures ==="
        )

        # Skip jobs with too many unknown failures to avoid overwhelming the user
        if len(job_failures.unknown_failures) > 7:
            for f in job_failures.unknown_failures:
                f.praktika_result.set_comment("IGNORED")
            print(
                f"  Too many failures ({len(job_failures.unknown_failures)}), skipping"
            )
            time.sleep(3)
            return

        # Display known failures summary
        if job_failures.known_failures:
            print(f"  Known failures ({len(job_failures.known_failures)}):")
            for failure in job_failures.known_failures:
                print(failure)

        # Process unknown failures
        if not job_failures.unknown_failures:
            return

        still_unknown = []
        if job_failures.unknown_failures:
            for failure in job_failures.unknown_failures:
                print("")
                JobResultProcessor.failure_counter += 1
                print(f"{JobResultProcessor.failure_counter}. {failure}")
                print(f"cidb: {failure.cidb_link}")

                if not failure.can_process():
                    still_unknown.append(failure)
                    time.sleep(3)
                    continue

                # Try to find existing issue first
                if failure.check_issue():
                    print(f"Linked to existing issue: {failure.issue_url}")
                    job_failures.known_failures.append(failure)
                    failure.praktika_result.set_comment("ISSUE EXISTS")
                    # let user read resolution before moving to the next failure
                    time.sleep(3)
                    continue
                else:
                    print("Issue not found")

                # Create new issue if user confirms
                if UserPrompt.confirm("Create GitHub issue for this failure?"):
                    if failure.create_issue():
                        print(f"Issue created: {failure.issue_url}")
                        failure.praktika_result.set_comment("ISSUE CREATED")
                        job_failures.known_failures.append(failure)
                        global issues_created
                        issues_created += 1
                    else:
                        print("ERROR: Failed to create issue")
                        still_unknown.append(failure)
                    # let user read resolution before moving to the next failure
                    time.sleep(3)
                else:
                    still_unknown.append(failure)

        job_failures.unknown_failures = still_unknown

    @staticmethod
    def get_ci_praktika_result(pr_number, commit_sha):
        if pr_number != 0:
            report_url = f"https://s3.amazonaws.com/clickhouse-test-reports/PRs/{pr_number}/{commit_sha}/result_pr.json"
        else:
            report_url = f"https://s3.amazonaws.com/clickhouse-test-reports/REFs/master/{commit_sha}/result_masterci.json"
        _ = Shell.check(f"curl {report_url} -o /tmp/result_pr.json > /dev/null 2>&1")
        return Result.from_file("/tmp/result_pr.json")

    @staticmethod
    def collect_all_failures(pr_result):
        """
        Collect all failures from PR result into a flat list of CIFailure objects.

        Args:
            pr_result: Result object containing job results

        Returns:
            List of CIFailure objects representing all failures
        """
        failures = []
        for job_result in pr_result.results:
            if not job_result.is_ok():
                if job_result.results:
                    # Job has test-level failures
                    for test_result in job_result.results:
                        if not test_result.is_ok():
                            failures.append(
                                CIFailure(
                                    job_name=job_result.name,
                                    job_status=job_result.status,
                                    test_name=test_result.name,
                                    test_status=test_result.status,
                                    test_info=test_result.info,
                                    praktika_result=test_result,
                                )
                            )
                else:
                    # Job-level failure without test results
                    failures.append(
                        CIFailure(
                            job_name=job_result.name,
                            job_status=job_result.status,
                            test_name="",
                            test_status="",
                            test_info=job_result.info,
                            praktika_result=job_result,
                        )
                    )
        return failures

    @staticmethod
    def process_sync_status(commit_status_data: GH.CommitStatus, sha: str):
        if commit_status_data.state in (Result.Status.SUCCESS,):
            pass
        elif commit_status_data.state in (Result.Status.FAILED,):
            if commit_status_data.description == "tests failed":
                print(
                    f"\nCH Sync failed for commit, description: {commit_status_data.description}"
                )
                if UserPrompt.confirm("You sure it can be ignored?"):
                    GH.post_commit_status(
                        commit_status_data.context,
                        Result.Status.SUCCESS,
                        "Ignored",
                        commit_status_data.url,
                        sha=sha,
                        repo="ClickHouse/ClickHouse",
                    )
                else:
                    sys.exit(0)
            else:
                print(
                    f"\nCH Sync commit status state: {commit_status_data.state} and description: {commit_status_data.description} - cannot proceed"
                )
                sys.exit(1)
        elif commit_status_data.state in (Result.Status.PENDING,):
            if commit_status_data.description == "tests started":
                print(
                    f"\n{commit_status_data.context} is pending with description {commit_status_data.description}"
                )
                if UserPrompt.confirm("You sure it can be ignored?"):
                    GH.post_commit_status(
                        commit_status_data.context,
                        Result.Status.SUCCESS,
                        "Ignored",
                        commit_status_data.url,
                        sha=sha,
                        repo="ClickHouse/ClickHouse",
                    )
                else:
                    sys.exit(0)
            else:
                print(
                    f"\nCH Sync commit status state: {commit_status_data.state} and description: {commit_status_data.description} - cannot proceed"
                )
                sys.exit(0)

    @staticmethod
    def process_mergeable_check_status(commit_status_data: GH.CommitStatus, sha: str):
        if commit_status_data and commit_status_data.state in (Result.Status.SUCCESS,):
            pass
        elif commit_status_data.state in (Result.Status.FAILED,):
            if UserPrompt.confirm("Do you want to unblock mergeable check?"):
                GH.post_commit_status(
                    CheckStatuses.MERGEABLE_CHECK,
                    Result.Status.SUCCESS,
                    "Manually overridden",
                    "",
                    sha=sha,
                    repo="ClickHouse/ClickHouse",
                )
            else:
                sys.exit(0)
        else:
            raise Exception(
                f"Mergeable check commit status state: {commit_status_data.state} and description: {commit_status_data.description} - cannot proceed"
            )


def get_commit_statuses(head_sha: str) -> dict:
    """
    Fetch and filter commit statuses for the given commit SHA.

    Args:
        head_sha: The commit SHA to fetch statuses for

    Returns:
        Dictionary mapping status context names to GH.CommitStatus objects
    """
    # Get commit statuses with pagination
    statuses_list = Shell.get_output(
        f"gh api repos/ClickHouse/ClickHouse/commits/{head_sha}/statuses --paginate"
    )
    statuses_list = json.loads(statuses_list)

    # Filter for specific statuses (take the first match for each context)
    required_checks = [
        CheckStatuses.PR,
        CheckStatuses.CH_INC_SYNC,
        CheckStatuses.MERGEABLE_CHECK,
    ]
    status_map = {}

    for status in statuses_list:
        context = status["context"]
        if context in required_checks and context not in status_map:
            status_map[context] = GH.CommitStatus(
                state=status["state"],
                description=status.get("description", "N/A"),
                url=status.get("target_url", ""),
                context=context,
            )

    print(f"\nCommit statuses:")
    for check in required_checks:
        if check in status_map:
            state = status_map[check].state
            desc = status_map[check].description
            print(f"  - {check}: {state} - {desc}")
        else:
            print(f"  - {check}: unknown")
    print("")

    return status_map


issues_created = 0
ci_start_time = None


def main():
    global head_sha, pr_number
    is_master_commit = False
    my_prs_number_and_title = Shell.get_output(
        "gh pr list --author @me --json number,title --base master --limit 20 --repo ClickHouse/ClickHouse"
    )
    my_prs_number_and_title = json.loads(my_prs_number_and_title)
    pr_menu = []
    pr_menu.append((f"Process commit sha on master", 0))
    pr_menu.append((f"Enter PR number manually", 1))
    for pr_dict in my_prs_number_and_title:
        pr_number = pr_dict["number"]
        pr_title = pr_dict["title"]
        pr_menu.append((f"#{pr_number}: {pr_title}", pr_number))

    selected_pr = UserPrompt.select_from_menu(pr_menu, "Select a PR to merge")
    if selected_pr[1] == 1:
        # PR numbers are expected to be in the range 80000-100000 for recent PRs
        pr_number = UserPrompt.get_number(
            "Enter PR number", lambda x: x > 80000 and x < 100000
        )
    elif selected_pr[1] == 0:
        is_master_commit = True
        commit_sha = UserPrompt.get_string(
            "Enter commit sha", validator=lambda x: len(x) == 40
        )
        pr_number = None
    else:
        pr_number = selected_pr[1]

    if not is_master_commit:
        pr_url = f"https://github.com/ClickHouse/ClickHouse/pull/{pr_number}"
        pr_data = Shell.get_output(
            f"gh pr view {pr_number} --json headRefOid,headRefName"
        )
        pr_data = json.loads(pr_data)
        head_sha = pr_data["headRefOid"]
        if GH.pr_has_conflicts(pr_number, "ClickHouse/ClickHouse"):
            print("PR has conflicts, cannot merge")
            sys.exit(1)
    else:
        head_sha = commit_sha
        pr_url = f"https://github.com/ClickHouse/ClickHouse/commit/{commit_sha}"

    print(f"Change URL: {pr_url}")
    print(f"Commit SHA: {head_sha}")
    print(f"CI Report: {CIFailure.get_job_report_url(pr_number, head_sha)}")

    if not is_master_commit:
        status_map = get_commit_statuses(head_sha)
        sync_status = status_map.get(CheckStatuses.CH_INC_SYNC)
        mergeable_check_status = status_map.get(CheckStatuses.MERGEABLE_CHECK)
        if (
            status_map[CheckStatuses.PR].state
            not in (
                Result.Status.SUCCESS,
                Result.Status.FAILED,
            )
            and not FORCE_MERGE
        ):
            raise Exception(
                f"Status for {commit_status_data.context} is not completed: {commit_status_data.state} - cannot proceed"
            )
    else:
        status_map = {}
        sync_status = None
        mergeable_check_status = None

    workflow_result = JobResultProcessor.get_ci_praktika_result(
        pr_number if not is_master_commit else 0, head_sha
    )

    global ci_start_time
    ci_start_time = workflow_result.start_time
    ci_failures = JobResultProcessor.collect_all_failures(workflow_result)

    not_finished_jobs = []
    known_failures = []
    unknown_failures = []
    for failure in ci_failures:
        if not failure.praktika_result.is_completed():
            not_finished_jobs.append(failure)
        elif failure.issue_url:
            known_failures.append(failure)
        else:
            unknown_failures.append(failure)
    pre_existing_issues_count = len(known_failures)

    if not_finished_jobs:
        if not UserPrompt.confirm(
            f"Proceed without waiting for {len(not_finished_jobs)} not finished job(s)?"
        ):
            sys.exit(0)

    job_to_failures = {}

    def add_failure_to_job(failure, failure_type):
        if failure.job_name not in job_to_failures:
            job_to_failures[failure.job_name] = JobFailuresList(
                job_name=failure.job_name,
                known_failures=[],
                unknown_failures=[],
                is_finished=True,
            )
        getattr(job_to_failures[failure.job_name], failure_type).append(failure)
        job_to_failures[failure.job_name].is_finished = (
            job_to_failures[failure.job_name].is_finished
            and failure.praktika_result.is_completed()
        )

    for failure in unknown_failures:
        add_failure_to_job(failure, "unknown_failures")

    for failure in known_failures:
        add_failure_to_job(failure, "known_failures")

    visited_jobs = set()
    job_failures_pairs = []
    for failure in ci_failures:
        if failure.job_name not in visited_jobs:
            if failure.job_name not in job_to_failures:
                assert failure.is_not_completed, failure
                continue
            job_failures_pairs.append(
                (failure.job_name, job_to_failures[failure.job_name])
            )
            visited_jobs.add(failure.job_name)

    if job_failures_pairs:
        print("\nStart processing job failures one by one:\n")
        for job_name, failures in job_failures_pairs:
            JobResultProcessor.process_job_failures(failures)

    known_failures = []
    unknown_failures = []

    for job_name, failures in job_failures_pairs:
        known_failures.extend(failures.known_failures)
        unknown_failures.extend(failures.unknown_failures)

    print("\nCI failures:")
    if known_failures:
        print("\n--- Known problems ---")
        for failure in known_failures:
            print(failure)

    if unknown_failures:
        print("\n--- Unknown problems ---")
        for failure in unknown_failures:
            print(failure)

    if not_finished_jobs:
        print("\n--- Not finished jobs ---")
        for failure in not_finished_jobs:
            print(failure)

    if is_master_commit:
        sys.exit(0)

    question = "CI status:\n"
    if unknown_failures or issues_created > 0 or pre_existing_issues_count > 0:
        if not_finished_jobs:
            question += f" - {len(not_finished_jobs)} not finished job{'s' if len(not_finished_jobs) != 1 else ''}\n"
        if unknown_failures:
            question += f" - {len(unknown_failures)} unknown failure{'s' if len(unknown_failures) != 1 else ''}\n"
        if issues_created > 0:
            question += f" - {issues_created} issue{'s' if issues_created != 1 else ''} just created\n"
        if pre_existing_issues_count > 0:
            question += f" - {pre_existing_issues_count} pre-existing issue{'s' if pre_existing_issues_count != 1 else ''}\n"
        question += " - all other checks passed\n"
        question += f" - Sync status: {sync_status.state}, description: {sync_status.description}\n"
    else:
        question = "All checks passed! Congratulations!\n"

    question += "\nDo you want to update PR CI comment?"
    if not UserPrompt.confirm(question):
        sys.exit(0)

    try:
        print("\nUpdating CI summary in the PR comment")
        summary_body = GH.ResultSummaryForGH.from_result(
            workflow_result,
            sha=head_sha,
        ).to_markdown(pr_number, head_sha, workflow_name="PR", branch="")
        if not GH.post_updateable_comment(
            comment_tags_and_bodies={"summary": summary_body},
            pr=pr_number,
            repo="ClickHouse/ClickHouse",
            only_update=True,
            verbose=False,
        ):
            print(f"ERROR: failed to post CI summary")
    except Exception as e:
        print(f"ERROR: failed to post CI summary, ex: {e}")
        traceback.print_exc()

    JobResultProcessor.process_sync_status(sync_status, sha=head_sha)

    JobResultProcessor.process_mergeable_check_status(
        mergeable_check_status, sha=head_sha
    )

    if unknown_failures:
        for failure in unknown_failures:
            failure.praktika_result.set_comment("IGNORED")

    if Shell.check(
        f"gh pr view {pr_number} --json isDraft --jq '.isDraft' | grep -q true"
    ):
        if UserPrompt.confirm(f"It's a draft PR. Do you want to undraft it?"):
            Shell.check(f"gh pr ready {pr_number}", strict=True, verbose=True)
        else:
            sys.exit(0)

    if UserPrompt.confirm(f"Do you want to merge PR {pr_number}?"):
        if Shell.check(f"gh pr merge {pr_number} --auto"):
            print(f"PR {pr_number} auto merge has been enabled")


if __name__ == "__main__":
    main()
