import argparse
import json
import re
import sys
import time
import traceback
from datetime import datetime
from typing import List
from urllib.parse import quote

sys.path.append("./")

from ci.praktika.gh import GH
from ci.praktika.interactive import UserPrompt
from ci.praktika.issue import Issue, IssueLabels, TestCaseIssueCatalog
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
    BUGFIX_VALIDATION_INTEGRATION = "Bugfix validation integration"
    CLICKBENCH = "ClickBench"


class CreateIssue:
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

    @classmethod
    def create_and_link_gh_issue(cls, title: str, body: str, labels: List[str]) -> str:
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
            "_Important: This issue was automatically generated and is used by CI for matching failures. "
            "DO NOT modify the body content. DO NOT remove labels._\n\n"
        ) + body

        if not UserPrompt.confirm("Proceed with issue creation?"):
            return ""

        return Issue.create_from(title=title, body=body, labels=labels).create_on_gh(
            repo_name="ClickHouse/ClickHouse"
        )

    @classmethod
    def create_gh_issue_on_flaky_or_broken_test(cls, result, job_name):
        print(CreateIssue.repr_result(result))

        test_name = result.name
        if "[" in result.name:
            # Despite parameter might be valuable for failure reproduction, drop it for simplicity
            test_name = test_name.split("[")[0]

        failure_reason = UserPrompt.get_string(
            "\nEnter the exact error text from the test output above that identifies the failure.\n"
            "This must be a substring from the output (e.g., 'result differs with reference', 'Client failed! Return code: 210').\n"
            "It will be used to match and group similar failures",
            validator=lambda x: x in result.info,
        )
        if result.name.startswith("test_"):
            # pytest case
            test_name = UserPrompt.get_string(
                "\nEnter the test name. Must be full match with the failed test or omiting parameters or test function name for broader matching",
                validator=lambda x: result.name.startswith(x),
                default=result.name,
            )
        title = f"Flaky test: {test_name}"
        body = f"""\
Test name: {test_name}
Failure reason: {failure_reason}
CI report: [{job_name}]({cls.get_job_report_url(pr_number, head_sha, job_name)})

CIDB statistics: [cidb]({result.get_hlabel_link('cidb')})

Test output:
```
{result.get_info_truncated(truncate_from_top=False, max_info_lines_cnt=50, max_line_length=200)}
```
"""
        labels = [IssueLabels.CI_ISSUE, IssueLabels.FLAKY_TEST]

        return cls.create_and_link_gh_issue(title, body, labels)

    @classmethod
    def create_gh_issue_on_fuzzer_or_stress_finding(cls, result, job_name):
        title = result.name
        body = f"""\
Test name: {result.name}
CI report: [{job_name}]({cls.get_job_report_url(pr_number, head_sha, job_name)})
CIDB statistics: [cidb]({result.get_hlabel_link("cidb")})

Test output:
```
{result.get_info_truncated(truncate_from_top=False, max_info_lines_cnt=200, max_line_length=200)}
```
"""
        labels = [IssueLabels.CI_ISSUE, IssueLabels.FUZZ]

        return cls.create_and_link_gh_issue(title, body, labels)

    @classmethod
    def can_process(cls, job_result, test_result):
        if job_result.is_error() or test_result.is_error():
            print(f"Cannot handle error status in job [{job_result.name}] - skip")
            return False
        if len(job_result.results) > 4:
            print("Cannot handle more than 4 test failures in one job - skip")
            return False
        if any(key in job_result.name for key in ("Stateless", "Integration")):
            if not re.match(r"^(\d{5}|test)_", test_result.name):
                print(
                    f"Only regular test failures can be handled, not [{test_result.name}] - skip"
                )
                return False
            else:
                return True
        if any(
            key in job_result.name
            for key in ("Performance", "Stress", "Finish", "Bugfix", "Docker")
        ):
            print("Job type is not supported yet")
            return False
        if "OOM" in test_result.name:
            print("Cannot handle OOM errors - skip")
            return False
        if (
            any(key in job_result.name for key in ("Buzz", "AST"))
            and job_result.results
        ):
            return True
        print("Cannot handle this failure - skip")
        return False

    @classmethod
    def repr_result(cls, result):
        res = f"\n - test output:\n"
        res += result.get_info_truncated(
            truncate_from_top=False, max_info_lines_cnt=20, max_line_length=200
        )
        res += f"\n - flags: {', '.join(result.get_labels()) or 'not flaged'}"
        res += f"\n - cidb: {result.get_hlabel_link("cidb") or 'not found'}"
        return res

    @classmethod
    def create_issue(cls, result, job_name):
        """
        Interactively create GitHub issues for failures that don't have existing issues.

        Returns:
            bool: True if issue was created successfully, False otherwise
        """
        if any(key in job_name for key in ("Stateless", "Integration")):
            issue_url = cls.create_gh_issue_on_flaky_or_broken_test(result, job_name)
        elif any(key in job_name for key in ("Buzz", "AST")):
            issue_url = cls.create_gh_issue_on_fuzzer_or_stress_finding(
                result, job_name
            )
        else:
            raise Exception(f"Unsupported job type: {job_name}")
        return issue_url

    @classmethod
    def create_infrastructure_issue(cls, result, job_name):
        """
        Interactively create GitHub issues for infrastructure failures that don't have existing issues.

        Returns:
            bool: True if issue was created successfully, False otherwise
        """
        print(CreateIssue.repr_result(result))

        failure_reason = UserPrompt.get_string(
            "Enter the exact error text from the test output above that identifies the failure.\n"
            "This must be a substring that appears in the output (e.g., 'Timeout', 'Connection refused').\n"
            "This text will be used to automatically detect and categorize similar failures in the future",
            validator=lambda x: x in result.info,
        )
        ci_failure_flags = UserPrompt.get_string(
            "Enter the CI failure flag associated with this failure type.\n"
            "Available options: 'retry_ok' (for transient failures that may succeed on retry) or leave empty for none",
            validator=lambda x: x in ("retry_ok", "") and result.has_label(x),
        )
        # support only one flag for now, in future we may need to support multiple flags
        ci_failure_flags = [ci_failure_flags] if ci_failure_flags else []
        menu = []
        menu.append(("Block: Block merge until issue is resolved", "block"))
        menu.append(
            (
                "Rerun: Automatically retry failed job once, block merge if retry also fails",
                "rerun",
            )
        )
        menu.append(
            (
                "Ignore: Allow merge regardless of this failure (use for known non-critical issues)",
                "ignore",
            )
        )
        required_ci_action = UserPrompt.select_from_menu(
            menu, question="Select the CI action to take when this failure is detected"
        )
        job_pattern = (
            UserPrompt.get_string(
                f"Enter SQL LIKE pattern to match job names affected by this failure.\n"
                f"Current job: '{job_name}'\n"
                f"Use '%' as wildcard (e.g., '%Stateless%' matches any job containing 'Stateless')",
                validator=lambda x: all(
                    part in job_name for part in x.split("%") if part
                ),
            )
            or "%"
        )
        test_pattern = (
            UserPrompt.get_string(
                f"Enter SQL LIKE pattern to match test names affected by this failure.\n"
                f"Current test: '{result.name}'\n"
                f"Use '%' as wildcard (e.g., '%timeout%' matches any test containing 'timeout')",
                validator=lambda x: all(
                    part in result.name for part in x.split("%") if part
                ),
            )
            or "%"
        )
        title = UserPrompt.get_string(
            "Enter a concise, descriptive title for this infrastructure issue",
            validator=lambda x: len(x.strip()) > 10,
        )
        body = f"""\
Failure reason: {failure_reason}

Failure flags: {', '.join(ci_failure_flags)}

Required CI action: {required_ci_action[1]}

Job pattern: {job_pattern}
Test pattern: {test_pattern}

CI report example: [{job_name}]({cls.get_job_report_url(pr_number, head_sha, job_name)})
Test output example:
```
{result.get_info_truncated(truncate_from_top=False, max_info_lines_cnt=50, max_line_length=200)}
```
"""
        labels = [IssueLabels.CI_ISSUE, IssueLabels.INFRASTRUCTURE]

        return cls.create_and_link_gh_issue(title, body, labels)


class CommitStatusCheck:

    @staticmethod
    def get_ci_praktika_result(pr_number, commit_sha):
        if pr_number != 0:
            report_url = f"https://s3.amazonaws.com/clickhouse-test-reports/PRs/{pr_number}/{commit_sha}/result_pr.json"
        else:
            report_url = f"https://s3.amazonaws.com/clickhouse-test-reports/REFs/master/{commit_sha}/result_masterci.json"
        _ = Shell.check(f"curl {report_url} -o /tmp/result_pr.json > /dev/null 2>&1")
        return Result.from_file("/tmp/result_pr.json")

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
            if UserPrompt.confirm("Do you want to override mergeable check?"):
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

    @classmethod
    def get_commit_statuses(cls, head_sha: str) -> dict:
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
create_infrastructure_issue = False


def main():
    global head_sha, pr_number, create_infrastructure_issue
    is_master_commit = False

    # Parse command-line arguments
    parser = argparse.ArgumentParser(
        description="Check CI status and process failures for ClickHouse PRs"
    )
    parser.add_argument(
        "pr_number",
        type=int,
        nargs="?",
        help="PR number to process (optional, interactive mode if not provided)",
    )
    parser.add_argument(
        "--create-infrastructure-issue",
        action="store_true",
        help="Create infrastructure issues for failures",
    )
    args = parser.parse_args()
    create_infrastructure_issue = args.create_infrastructure_issue

    # Check gh CLI version
    gh_version_output = Shell.get_output("gh --version")
    # Output format: "gh version 2.xx.x (yyyy-mm-dd)\n..."
    version_match = re.search(r"gh version (\d+)\.(\d+)", gh_version_output)
    if version_match:
        major = int(version_match.group(1))
        minor = int(version_match.group(2))
        if major < 2 or (major == 2 and minor <= 80):
            print(
                f"ERROR: gh CLI version {major}.{minor} is too old. Version newer than 2.80 is required."
            )
            sys.exit(1)
    else:
        print("ERROR: Could not parse gh CLI version")
        sys.exit(1)

    # Check if PR number was provided as command-line argument
    if args.pr_number is not None:
        pr_number = args.pr_number
        print(f"Using PR number from command line: {pr_number}")
    else:
        # Interactive mode: show menu to select PR
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
    print(f"CI Report: {CreateIssue.get_job_report_url(pr_number, head_sha)}")

    if not is_master_commit:
        status_map = CommitStatusCheck.get_commit_statuses(head_sha)
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

    workflow_result = CommitStatusCheck.get_ci_praktika_result(
        pr_number if not is_master_commit else 0, head_sha
    ).to_failed_results_with_flat_leaves()

    global ci_start_time
    ci_start_time = workflow_result.start_time

    not_finished_jobs = []
    known_failures = []
    failures_to_process = []
    unknown_failures = []

    # check if workflow failures and if any of them are unknown before matching against open issues
    workflow_has_unknown_failures = False
    for job_result in workflow_result.results:
        if not job_result.is_completed():
            not_finished_jobs.append((job_result.name, job_result))
        if not job_result.results:
            if job_result.has_label("issue"):
                known_failures.append((job_result.name, job_result))
                job_result.set_comment("ISSUE EXISTS")
                continue
            else:
                workflow_has_unknown_failures = True
        for task_result in job_result.results:
            if task_result.has_label("issue"):
                known_failures.append((job_result.name, task_result))
                task_result.set_comment("ISSUE EXISTS")
                continue
            else:
                workflow_has_unknown_failures = True

    # check unknown failures against open issues
    if workflow_has_unknown_failures:
        try:
            issue_catalog = TestCaseIssueCatalog.from_fs(TestCaseIssueCatalog.name)
        except Exception as e:
            print(f"Failed to load issue catalog: {e}")
            issue_catalog = None
        if (
            not issue_catalog
            or issue_catalog.updated_at < datetime.now().timestamp() - 10 * 60
        ):
            issue_catalog = TestCaseIssueCatalog.from_gh(verbose=False)
            issue_catalog.dump()
        print(f"Loaded {len(issue_catalog.active_test_issues)} active issues from gh\n")
        print("Checking failures against open issues...\n")
        for issue in issue_catalog.active_test_issues:
            issue.check_result(workflow_result)

        failures_to_process = []
        not_finished_jobs = []
        # reset and fill again after checking against existing issues
        known_failures = []
        unknown_failures = []

        workflow_result.dump()
        for result in workflow_result.results:
            if result.has_label("issue"):
                known_failures.append((result.name, result))
                result.set_comment("ISSUE EXISTS")
                continue
            if not result.is_completed():
                not_finished_jobs.append((result.name, result))
                continue
            if not result.results:
                if not CreateIssue.can_process(result, result):
                    unknown_failures.append((result.name, result))
                    continue
                failures_to_process.append((result.name, result))
            else:
                for sub_result in result.results:
                    if sub_result.has_label("issue"):
                        known_failures.append((result.name, sub_result))
                        sub_result.set_comment("ISSUE EXISTS")
                        continue
                    if not CreateIssue.can_process(result, sub_result):
                        unknown_failures.append((result.name, sub_result))
                        continue
                    failures_to_process.append((result.name, sub_result))

    pre_existing_issues_count = len(known_failures)

    if not_finished_jobs:
        if not UserPrompt.confirm(
            f"Proceed without waiting for {len(not_finished_jobs)} not finished job(s)?"
        ):
            sys.exit(0)

    if failures_to_process:
        failure_cnt = 0
        print("\nStart processing unknown failures one by one:")
        for job_name, failure_result in failures_to_process:
            print("")
            failure_cnt += 1
            print(f"{failure_cnt}. [ {failure_result.status} ] {failure_result.name}")
            if job_name != failure_result.name:
                print(f"  in {job_name}")
                print(f"cidb: {failure_result.get_hlabel_link('cidb')}")

            # Create new issue if user confirms
            if UserPrompt.confirm("Create GitHub issue for this failure?"):
                if create_infrastructure_issue and UserPrompt.confirm(
                    "Create infrastructure issue?"
                ):
                    print(
                        "Creating infrastructure issue [--create-infrastructure-issue]"
                    )
                    ci_issue = CreateIssue.create_infrastructure_issue(
                        failure_result, job_name
                    )
                else:
                    ci_issue = CreateIssue.create_issue(failure_result, job_name)
                if ci_issue:
                    print(f"Issue created: {ci_issue.url}")
                    issue_catalog.active_test_issues.append(ci_issue)
                    issue_catalog.dump()
                    failure_result.set_comment("ISSUE CREATED")
                    failure_result.set_clickable_label("issue", ci_issue.url)
                    known_failures.append((job_name, failure_result))
                    global issues_created
                    issues_created += 1
                else:
                    print("WARNING: Issue has not been created - skipping this failure")
                    unknown_failures.append((job_name, failure_result))
                # let user read resolution before moving to the next failure
                time.sleep(3)
            else:
                unknown_failures.append((job_name, failure_result))

    print("\nCI failures:")
    if known_failures:
        print("\n--- Known problems ---")
        for job_name, failure in known_failures:
            print(f"[{failure.status}] {failure.name} in {job_name}")

    if unknown_failures:
        print("\n--- Unknown problems ---")
        for job_name, failure in unknown_failures:
            print(f"[{failure.status}] {failure.name} in {job_name}")
            failure.set_comment("IGNORED")

    if not_finished_jobs:
        print("\n--- Not finished jobs ---")
        for _, failure in not_finished_jobs:
            print(f"[{failure.status}] {failure.name}")

    if is_master_commit:
        sys.exit(0)

    question = "CI status:\n"
    if unknown_failures or issues_created > 0 or pre_existing_issues_count > 0:
        if not_finished_jobs:
            question += f" - {len(not_finished_jobs)} not finished job{'s' if len(not_finished_jobs) != 1 else ''}\n"
        if unknown_failures:
            question += f" - {len(unknown_failures)} unknown failure{'s' if len(unknown_failures) != 1 else ''}\n"
        if known_failures:
            question += f" - {len(known_failures)} known failure{'s' if len(known_failures) != 1 else ''}\n"
        question += " - all other checks passed\n"
        question += f" - Sync status: {sync_status.state}, description: {sync_status.description}\n"
    else:
        question = "All checks passed! Congratulations!\n"

    if unknown_failures or known_failures:
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

    CommitStatusCheck.process_sync_status(sync_status, sha=head_sha)

    CommitStatusCheck.process_mergeable_check_status(
        mergeable_check_status, sha=head_sha
    )

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
