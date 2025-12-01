import json
import re
import sys
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
    STRESS = "Stress"
    UPGRADE = "Upgrade"
    PERFORMANCE = "Performance"
    FINISH_WORKFLOW = "Finish Workflow"


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

    def __post_init__(self):
        if "Stateless" in self.job_name:
            self.job_type = JobTypes.STATELESS
            if (
                any(
                    t in self.test_name
                    for t in ["Scrapping", "Killed", "Fatal messages", "Server died"]
                )
                or self.test_status == "SERVER_DIED"
            ):
                # TODO: Find right way to handle this
                self.ignorable = True
                self.praktika_result.set_comment("IGNORED")
        elif "Integration" in self.job_name:
            self.job_type = JobTypes.INTEGRATION
        elif "AST" in self.job_name:
            self.job_type = JobTypes.AST_FUZZER
        elif "Buzz" in self.job_name:
            self.job_type = JobTypes.BUZZ_FUZZER
        elif "Build" in self.job_name:
            self.job_type = JobTypes.BUILD
        elif (
            "Docker server image" in self.job_name
            or "Docker keeper image" in self.job_name
        ):
            self.job_type = JobTypes.DOCKER
        elif "Compatibility check" in self.job_name:
            self.job_type = JobTypes.COMPATIBILITY
        elif "Stress" in self.job_name:
            self.job_type = JobTypes.STRESS
            self.ignorable = True
            self.praktika_result.set_comment("IGNORED")
        elif "Upgrade" in self.job_name:
            self.job_type = JobTypes.UPGRADE
        elif "Performance" in self.job_name:
            self.job_type = JobTypes.PERFORMANCE
        elif "Finish Workflow" in self.job_name:
            self.job_type = JobTypes.FINISH_WORKFLOW
        else:
            raise Exception(f"Unknown job type for job name: {self.job_name}")
        self.issue_url = self.praktika_result.get_hlabel_link("flaky") or ""
        self.labels = self.praktika_result.ext.get("labels", [])
        self.cidb_link = self.praktika_result.get_hlabel_link("cidb") or ""

    def __str__(self):
        res = f"  {self.test_status or self.job_status}: {self.job_name}: {self.test_name if self.test_name else 'N/A'}\n"
        if self.labels:
            res += f"    Flags: {', '.join(self.labels)}\n"
        if self.issue_url:
            res += f"    Issue: {self.issue_url}\n"
        return res

    def __repr__(self):
        job_res = Result(
            name=self.job_name, status=self.job_status, results=[self.praktika_result]
        )
        res = job_res.to_stdout_formatted(
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

    def create_gh_issue_on_flaky_or_broken_test(self):
        if self.issue_url:
            assert False, "BUG"

        if not UserPrompt.confirm("Do you want to create an issue for this failure?"):
            return False

        failure_reason = UserPrompt.get_string(
            "Enter failure keyword from the test output identifying the problem (e.g. 'Timeout exceeded', 'Logical error', 'Result differs', etc.)",
            validator=lambda x: x in self.praktika_result.info,
        )
        title = f"Flaky test: {self.test_name}"
        body = f"""\
Failure reason: {failure_reason}
CI report: [{self.job_name}]({self.get_job_report_url(pr_number, head_sha, self.job_name)})
CIDB statistics: [cidb]({self.cidb_link})

Test output:
```
{self.praktika_result.to_stdout_formatted(truncate_from_top=False, max_info_lines_cnt=50, max_line_length=200)}
```
"""
        labels = ["testing", "flaky test"]
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

    def create_gh_issue_on_fuzzer_or_stress_finding(self):
        if self.issue_url:
            assert False, "BUG"

        if not UserPrompt.confirm("Do you want to create an issue for this failure?"):
            return False

        title = self.test_name
        body = f"""\
CI report: [{self.job_name}]({self.get_job_report_url(pr_number, head_sha, self.job_name)})
CIDB statistics: [cidb]({self.cidb_link})

Test output:
```
{self.praktika_result.to_stdout_formatted(truncate_from_top=False, max_info_lines_cnt=200, max_line_length=200)}
```
"""
        labels = ["testing", "fuzz"]
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


class JobResultProcessor:

    @staticmethod
    def process_job_result(job_result: Result):
        print(f"Job {job_result.name} status is {job_result.status}")
        if "Stateless" in job_result.name:
            JobResultProcessor.process_stateless_job(job_result)
        elif "Integration" in job_result.name:
            JobResultProcessor.process_integration_job(job_result)
        elif "AST" in job_result.name:
            JobResultProcessor.process_ast_fuzzer_job(job_result)
        else:
            raise Exception(f"Unknown job type: {job_result.name}")

    @staticmethod
    def process_stateless_job(job_result: Result):
        print(f"Failed tests:")
        for test in job_result.results:
            print(
                test.to_stdout_formatted(
                    truncate_from_top=False, max_info_lines_cnt=20, max_line_length=200
                )
            )

    @staticmethod
    def process_integration_job(job_result: Result):
        print(f"Failed tests:")
        for test in job_result.results:
            print(
                test.to_stdout_formatted(
                    truncate_from_top=False, max_info_lines_cnt=20, max_line_length=200
                )
            )

    @staticmethod
    def process_ast_fuzzer_job(job_result: Result):
        assert False, "TODO"

    @staticmethod
    def get_ci_praktika_result(pr_number, commit_sha):
        if pr_number != 0:
            report_url = f"https://s3.amazonaws.com/clickhouse-test-reports/PRs/{pr_number}/{commit_sha}/result_pr.json"
        else:
            report_url = f"https://s3.amazonaws.com/clickhouse-test-reports/REFs/master/{commit_sha}/result_masterci.json"
        _ = Shell.check(f"curl {report_url} -o /tmp/result_pr.json > /dev/null 2>&1")
        return Result.from_file("/tmp/result_pr.json")

    @staticmethod
    def collect_all_failures(pr_result, failures):
        success_job_cnt = 0
        failed_job_cnt = 0
        skipped_job_cnt = 0
        dropped_job_cnt = 0
        running_job_cnt = 0
        pending_job_cnt = 0
        error_job_cnt = 0
        for job_result in pr_result.results:
            if job_result.is_success():
                success_job_cnt += 1
            elif job_result.is_failure():
                failed_job_cnt += 1
            elif job_result.is_skipped():
                skipped_job_cnt += 1
            elif job_result.is_dropped():
                dropped_job_cnt += 1
            elif job_result.is_running():
                running_job_cnt += 1
            elif job_result.is_pending():
                pending_job_cnt += 1
            elif job_result.is_error():
                error_job_cnt += 1
            else:
                raise Exception(f"Unknown job result status: {job_result.status}")
        assert (
            success_job_cnt
            + failed_job_cnt
            + skipped_job_cnt
            + dropped_job_cnt
            + running_job_cnt
            + pending_job_cnt
            + error_job_cnt
            == len(pr_result.results)
        )
        if pr_result.is_ok():
            print(f"All jobs are successful - ready to merge")
        else:
            print(f"Not all jobs are successful - proceed with caution")
            for job_result in pr_result.results:
                if not job_result.is_ok():
                    if job_result.results:
                        for test_result in job_result.results:
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

    @staticmethod
    def process_sync_status(commit_status_data: GH.CommitStatus, sha: str):
        if commit_status_data.state in (Result.Status.SUCCESS,):
            pass
        elif commit_status_data.state in (Result.Status.FAILED,):
            print(f"\nCH Sync failed for commit {commit_status_data.context}")
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
                    f"CH Sync commit status state: {commit_status_data.state} and description: {commit_status_data.description} - cannot proceed"
                )
                sys.exit(0)

    @staticmethod
    def process_mergeable_check_status(commit_status_data: GH.CommitStatus, sha: str):
        if commit_status_data and commit_status_data.state in (Result.Status.SUCCESS,):
            pass
        elif FORCE_MERGE or commit_status_data.state in (Result.Status.FAILED,):
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


def find_existing_issues_for_failures(failures: list[CIFailure]):
    """
    Check if any of the provided failures have existing open GitHub issues.

    Args:
        failures: List of CIFailure objects to check against open GitHub issues

    Returns:
        Tuple of (failures_with_open_issue, unknown_failures) where:
        - failures_with_open_issue: failures that have existing GitHub issues
        - unknown_failures: failures without existing GitHub issues
    """
    job_failures_pairs = CIFailure.group_by_job(failures)
    failures_with_open_issue = []
    unknown_failures = []
    for job_name, job_failures in job_failures_pairs:
        print(f"\n  - {job_name}: status {len(job_failures)} failures")
        for failure in job_failures:
            if failure.test_name:
                print(f"    - {failure.test_name}: {failure.test_status}")
            else:
                print(f"    - {failure.job_name}: {failure.job_status}")

            # if not failure.test_name:
            #     print(
            #         f"      --> It's looks like infrastracture problem - cannot handle it"
            #     )
            #     continue

            if failure.job_type == JobTypes.BUILD:
                search_in_title = failure.job_name
                labels = ["build"]
            elif failure.job_type in (JobTypes.STATELESS, JobTypes.INTEGRATION):
                assert re.match(
                    r"^(\d{5}|test)_", failure.test_name
                ), f"Unexpected test name format: {failure.test_name}"
                search_in_title = failure.test_name
                labels = ["flaky test"]
            elif failure.job_type in (JobTypes.BUZZ_FUZZER, JobTypes.AST_FUZZER):
                if not failure.test_name:
                    assert (
                        failure.job_status == Result.Status.ERROR
                    ), f"Unexpected job status: {failure.job_status}"
                    print(
                        f"      --> It's looks like infrastracture problem - cannot handle it"
                    )
                    continue
                search_in_title = failure.test_name
                labels = ["fuzz"]
            elif failure.job_type in (JobTypes.PERFORMANCE):
                # TODO:
                # search_in_title = failure.test_name
                # labels = ["performance"]
                print(
                    f"      --> It's looks like Performance tests - not yet supported"
                )
                continue
            else:
                raise Exception(f"Unexpected job type: {failure.job_type}")

            issues = GH.find_issue(
                title=search_in_title,
                labels=labels,
                repo="ClickHouse/ClickHouse",
            )
            if issues and len(issues) > 1:
                print("WARNING: More than one issue found: check duplicates")
                for issue in issues:
                    print(f"  {issue.number}: {issue.title}")
            issue = issues[0] if issues else None
            if issue:
                print(f"      --> Issue {issue.html_url} already exists")
                failure.issue_url = issue.html_url
                failure.praktika_result.set_clickable_label("issue", issue.html_url)
                failure.praktika_result.set_comment("ISSUE EXISTS")
                failures_with_open_issue.append(failure)
            else:
                print(f"      --> No existing issue found for {failure.test_name}")
                unknown_failures.append(failure)

    return failures_with_open_issue, unknown_failures


def create_issues_for_failures(failures: list[CIFailure]):
    """
    Interactively create GitHub issues for failures that don't have existing issues.

    Args:
        failures: List of CIFailure objects to create issues for

    Returns:
        List of failures that still need issues (those for which issues were not created)
    """
    failures_with_created_issues = []
    job_failures_pairs = CIFailure.group_by_job(failures)
    print(f"There are failures in [{len(job_failures_pairs)}] jobs")
    for job_name, job_failures in job_failures_pairs:
        print(f"  - {job_name}: {len(job_failures)} failures")
        for failure in job_failures:
            print(f"    - {failure.test_name}: {failure.test_status}")
            # Check if this is a standard test name format (e.g., 12345_test_name or test_name)
            if failure.job_type in (JobTypes.STATELESS, JobTypes.INTEGRATION):
                if re.match(r"^(\d{5}|test)_", failure.test_name):
                    assert (
                        not failure.issue_url
                    ), "BUG: Issue URL is already set, this should be a known issue"
                    print(repr(failure))
                    if failure.create_gh_issue_on_flaky_or_broken_test():
                        failure.praktika_result.set_comment("ISSUE CREATED")
                        failures_with_created_issues.append(failure)
                else:
                    raise Exception(
                        f"Unsupported test name format: {failure.test_name}"
                    )
            elif failure.job_type in (JobTypes.BUZZ_FUZZER, JobTypes.AST_FUZZER):
                if failure.create_gh_issue_on_fuzzer_or_stress_finding():
                    failure.praktika_result.set_comment("ISSUE CREATED")
                    failures_with_created_issues.append(failure)
            else:
                # Non-standard test name format - needs special handling
                raise Exception(f"Unsupported job type: {failure.job_type}")
    return [f for f in failures if f not in failures_with_created_issues]


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


def main():
    global head_sha, pr_number
    is_master_commit = False
    my_prs_number_and_title = Shell.get_output(
        "gh pr list --author @me --json number,title --base master --limit 20"
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

    ci_failures = []
    workflow_result = JobResultProcessor.get_ci_praktika_result(
        pr_number if not is_master_commit else 0, head_sha
    )

    JobResultProcessor.collect_all_failures(
        workflow_result,
        failures=ci_failures,
    )

    not_finished_jobs = []
    known_failures = []
    unknown_failures = []
    unprocessed_failures = []
    for failure in ci_failures:
        if not failure.praktika_result.is_completed():
            not_finished_jobs.append(failure)
        elif failure.issue_url:
            known_failures.append(failure)
        elif failure.ignorable:
            unprocessed_failures.append(failure)
        else:
            unknown_failures.append(failure)
    pre_existing_issues_count = len(known_failures)

    if not_finished_jobs:
        if FORCE_MERGE:
            print(f"Not finished jobs:")
            for failure in not_finished_jobs:
                print(failure)
            if not UserPrompt.confirm("Proceed without waiting for not finished jobs?"):
                sys.exit(0)
        else:
            print(
                f"There are {len(not_finished_jobs)} not finished jobs. Cannot proceed"
            )
            sys.exit(0)

    if unknown_failures:
        print("\nUnknown failures:")
        for failure in unknown_failures:
            print(failure)
        if UserPrompt.confirm(
            f"There are {len(unknown_failures)} unknown failures. Check for existing GitHub issues?"
        ):
            failures_with_open_issue, unknown_failures = (
                find_existing_issues_for_failures(unknown_failures)
            )
            known_failures.extend(failures_with_open_issue)
            pre_existing_issues_count += len(failures_with_open_issue)
        else:
            sys.exit(0)

    print("\nCI failures:")
    if known_failures:
        print("\n--- Known problems ---")
        for failure in known_failures:
            print(failure)

    newly_created_issues_count = 0
    if unknown_failures:
        print("\n--- Unknown problems ---")
        for failure in unknown_failures:
            print(failure)

        if UserPrompt.confirm(
            f"There are {len(unknown_failures)} unknown failures. Do you want to create an issue for any of them?"
        ):
            unknown_failures_before_creation = len(unknown_failures)
            unknown_failures = create_issues_for_failures(unknown_failures)
            newly_created_issues_count = unknown_failures_before_creation - len(
                unknown_failures
            )
            print("All failures processed")

    if is_master_commit:
        sys.exit(0)

    should_update_PR_comment = False
    if not unknown_failures:
        if unprocessed_failures:
            print("\n--- Unprocessed failures ---")
            for failure in unprocessed_failures:
                print(failure)
        question = "CI status:\n"
        if (
            unprocessed_failures
            or newly_created_issues_count > 0
            or pre_existing_issues_count > 0
        ):
            should_update_PR_comment = True
            if not_finished_jobs:
                question += f" - {len(not_finished_jobs)} not finished job{'s' if len(not_finished_jobs) != 1 else ''}\n"
            if unprocessed_failures:
                unprocessed_count = len(unprocessed_failures)
                question += f" - {unprocessed_count} unprocessed failure{'s' if unprocessed_count != 1 else ''}\n"
            if newly_created_issues_count > 0:
                question += f" - {newly_created_issues_count} issue{'s' if newly_created_issues_count != 1 else ''} just created\n"
            if pre_existing_issues_count > 0:
                question += f" - {pre_existing_issues_count} pre-existing issue{'s' if pre_existing_issues_count != 1 else ''}\n"
            question += " - all other checks passed\n"
        else:
            question = "All checks passed! Congratulations!\n"
        question += "\nDo you want to merge the PR?"
        if not UserPrompt.confirm(question):
            sys.exit(0)
    else:
        print("There are unknown failures. Not merging the PR")
        sys.exit(0)

    JobResultProcessor.process_mergeable_check_status(
        mergeable_check_status, sha=head_sha
    )

    if unprocessed_failures:
        for failure in unprocessed_failures:
            failure.praktika_result.set_comment("IGNORED")

    JobResultProcessor.process_sync_status(sync_status, sha=head_sha)

    assert not unknown_failures, "BUG: unknown failures are not processed"
    if should_update_PR_comment:
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

    if Shell.check(
        f"gh pr view {pr_number} --json isDraft --jq '.isDraft' | grep -q true"
    ):
        if UserPrompt.confirm(f"It's a draft PR. Do you want to undraft it?"):
            Shell.check(f"gh pr ready {pr_number}", strict=True, verbose=True)
        else:
            sys.exit(0)

    if Shell.check(f"gh pr merge {pr_number} --auto"):
        print(f"PR {pr_number} auto merge has been enabled")


if __name__ == "__main__":
    main()
