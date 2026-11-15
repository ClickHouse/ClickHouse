import argparse
import json
import re
import sys
import traceback
from datetime import datetime
from typing import Optional
from urllib.parse import quote

sys.path.append(".")

from ci.praktika.gh import GH
from ci.praktika.git import Git
from ci.praktika.interactive import UserPrompt
from ci.praktika.issue import TestCaseIssueCatalog
from ci.praktika.result import Result
from ci.praktika.utils import Shell, Utils


class CheckStatuses:
    PR = "PR"
    CH_INC_SYNC = "CH Inc sync"
    MERGEABLE_CHECK = "Mergeable Check"


FORCE_MERGE = True

PUBLIC_REPO = "ClickHouse/ClickHouse"
SYNC_REPO = "ClickHouse/clickhouse-private"
S3_PROXY_BASE_URL = "http://ci-reports:8080"
S3_PRIVATE_REPORT_BUCKET = "clickhouse-test-reports-private"


pr_number = None
head_sha = None


class JobTypes:
    STATELESS = "Stateless"
    INTEGRATION = "Integration"
    AST_FUZZER = "AST fuzzer"
    BUILD = "Build"
    FORMATTER = "Formatter"
    LACASA_FUZZER = "La Casa Del Dolor"
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
    UNIT_TESTS = "Unit tests"


GROENEAI_LOGIN = "groeneai"
MAX_COMMENT_LENGTH = 65536


def get_job_report_url(pr_number, head_sha, job_name=""):
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


class CommitStatusCheck:

    @staticmethod
    def get_ci_praktika_result(pr_number, commit_sha):
        # Reports live under the normalized workflow name, both as a path prefix
        # and in the result file name:
        #   PRs/<pr>/<sha>/<workflow>/result_<workflow>.json
        #   REFs/master/<sha>/<workflow>/result_<workflow>.json
        if pr_number != 0:
            workflow = Utils.normalize_string("PR")
            ref_prefix = f"PRs/{pr_number}"
        else:
            workflow = Utils.normalize_string("MasterCI")
            ref_prefix = "REFs/master"
        report_url = (
            f"https://s3.amazonaws.com/clickhouse-test-reports/"
            f"{ref_prefix}/{commit_sha}/{workflow}/result_{workflow}.json"
        )
        # -f so a missing report fails here instead of downloading the S3 error
        # body and blowing up later in Result.from_file with an opaque parse error.
        if not Shell.check(
            f"curl -f {report_url} -o /tmp/result_pr.json > /dev/null 2>&1"
        ):
            raise RuntimeError(f"Failed to fetch CI report from {report_url}")
        return Result.from_file("/tmp/result_pr.json")

    @staticmethod
    def process_sync_status(commit_status_data: Optional[GH.CommitStatus], sha: str):
        if not commit_status_data:
            if not UserPrompt.confirm(
                "CH Inc sync status is missing. Override to success?"
            ):
                sys.exit(0)
            GH.post_commit_status(
                CheckStatuses.CH_INC_SYNC,
                Result.Status.OK,
                "Manually overridden",
                "",
                sha=sha,
                repo="ClickHouse/ClickHouse",
            )
            return
        if commit_status_data.state in (Result.GHStatus.SUCCESS,):
            pass
        elif commit_status_data.state in (Result.GHStatus.FAILURE,):
            if commit_status_data.description.startswith("tests failed"):
                print(
                    f"\nCH Sync failed for commit, description: {commit_status_data.description}"
                )
                if UserPrompt.confirm("You sure it can be ignored?"):
                    GH.post_commit_status(
                        commit_status_data.context,
                        Result.Status.OK,
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
        elif commit_status_data.state in (Result.GHStatus.PENDING,):
            if commit_status_data.description == "tests started":
                print(
                    f"\n{commit_status_data.context} is pending with description {commit_status_data.description}"
                )
                if UserPrompt.confirm("You sure it can be ignored?"):
                    GH.post_commit_status(
                        commit_status_data.context,
                        Result.Status.OK,
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
    def process_mergeable_check_status(
        commit_status_data: Optional[GH.CommitStatus], sha: str
    ):
        override = False
        if commit_status_data and commit_status_data.state in (Result.GHStatus.SUCCESS,):
            pass
        elif not commit_status_data:
            override = True
        elif commit_status_data.state in (Result.GHStatus.FAILURE,):
            if UserPrompt.confirm("Do you want to override mergeable check?"):
                override = True
            else:
                sys.exit(0)
        else:
            raise Exception(
                f"Mergeable check commit status state: {commit_status_data.state} and description: {commit_status_data.description} - cannot proceed"
            )
        if override:
            GH.post_commit_status(
                CheckStatuses.MERGEABLE_CHECK,
                Result.Status.OK,
                "Manually overridden",
                "",
                sha=sha,
                repo="ClickHouse/ClickHouse",
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

        print("\nCommit statuses:")
        for check in required_checks:
            if check in status_map:
                state = status_map[check].state
                desc = status_map[check].description
                print(f"  - {check}: {state} - {desc}")
            else:
                print(f"  - {check}: unknown")
        print("")

        return status_map

    @staticmethod
    def get_sync_pr_info(public_pr_number):
        """Find the sync PR number and head SHA for a public PR."""
        raw = Shell.get_output(
            f"gh pr list --state all --head sync-upstream/pr/{public_pr_number} "
            f"--repo {SYNC_REPO} --json number,headRefOid --jq '.[0]'"
        )
        if not raw or raw.strip() in ("", "null"):
            print(f"WARNING: No sync PR found for PR #{public_pr_number}")
            return None
        data = json.loads(raw)
        return data["number"], data["headRefOid"]

    @staticmethod
    def get_sync_pr_result(sync_pr_number, sync_sha):
        """Fetch the CI result for a sync PR via the S3 proxy."""
        workflow = Utils.normalize_string("PR")
        report_url = (
            f"{S3_PROXY_BASE_URL}/{S3_PRIVATE_REPORT_BUCKET}"
            f"/PRs/{sync_pr_number}/{sync_sha}/{workflow}/result_{workflow}.json"
        )
        if not Shell.check(
            f"curl -f {report_url} -o /tmp/result_sync_pr.json > /dev/null 2>&1"
        ):
            print(f"WARNING: Failed to fetch sync PR result from {report_url}")
            return None
        return Result.from_file("/tmp/result_sync_pr.json")


ci_start_time = None


def load_issue_catalog(repo):
    catalog_name = (
        TestCaseIssueCatalog.name if repo == PUBLIC_REPO else "issue_catalog_private"
    )
    try:
        catalog = TestCaseIssueCatalog.from_fs(catalog_name)
    except Exception as e:
        print(f"Failed to load issue catalog: {e}")
        catalog = None
    if not catalog or catalog.updated_at < datetime.now().timestamp() - 10 * 60:
        catalog = TestCaseIssueCatalog.from_gh(verbose=False, repo=repo)
        catalog.name = catalog_name
        catalog.dump()
    print(f"Loaded {len(catalog.active_test_issues)} active issues from {repo}\n")
    return catalog


def process_workflow_failures(workflow_result, repo):
    """
    Process CI workflow failures: match them against open issues.

    Args:
        workflow_result: The workflow Result with failures (already filtered to failed)
        repo: GitHub repository name (e.g., "ClickHouse/ClickHouse")

    Returns:
        tuple: (known_failures, unknown_failures, not_finished_jobs)
    """
    workflow_has_unknown_failures = any(
        not leaf.has_label("issue")
        for job_result in workflow_result.results
        for leaf in (job_result.results or [job_result])
    )

    if workflow_has_unknown_failures:
        issue_catalog = load_issue_catalog(repo)
        # For non-public repos, also use public repo issues since the same flaky
        # tests may already have issues filed in the public repo
        if repo != PUBLIC_REPO:
            public_catalog = load_issue_catalog(PUBLIC_REPO)
            existing_numbers = {i.number for i in issue_catalog.active_test_issues}
            for issue in public_catalog.active_test_issues:
                if issue.number not in existing_numbers:
                    issue_catalog.active_test_issues.append(issue)

        print("Checking failures against open issues...\n")
        for issue in issue_catalog.active_test_issues:
            issue.check_result(workflow_result)

    not_finished_jobs = []
    known_failures = []
    unknown_failures = []
    for result in workflow_result.results:
        if result.has_label("issue"):
            known_failures.append((result.name, result))
            result.set_comment("ISSUE EXISTS")
            continue
        if not result.is_completed():
            not_finished_jobs.append((result.name, result))
            continue
        for sub_result in result.results or [result]:
            if sub_result.has_label("issue"):
                known_failures.append((result.name, sub_result))
                sub_result.set_comment("ISSUE EXISTS")
            else:
                unknown_failures.append((result.name, sub_result))

    if not_finished_jobs:
        if not UserPrompt.confirm(
            f"Proceed without waiting for {len(not_finished_jobs)} not finished job(s)?"
        ):
            sys.exit(0)

    return known_failures, unknown_failures, not_finished_jobs


def post_investigation_request(
    pr_number, head_sha, known_failures, unknown_failures, sync_failures_count
):
    """Ask groeneai in the PR to investigate and fix the failures, or link existing fixes."""

    def describe(job_name, failure):
        if failure.name == job_name:
            return f"- `{job_name}`"
        return f"- `{failure.name}` in `{job_name}`"

    def plural(n):
        return f"{n} failure{'s' if n != 1 else ''}"

    header = [
        f"@{GROENEAI_LOGIN}, please investigate the CI failures in this PR "
        f"(commit {head_sha}) and fix them, or link existing fixes.",
        "",
        f"CI report: {get_job_report_url(pr_number, head_sha)}",
    ]
    footer = []
    if sync_failures_count:
        footer = ["", f"Also, {plural(sync_failures_count)} in the private sync PR."]

    sections = []
    if unknown_failures:
        entries = []
        for job_name, failure in unknown_failures:
            cidb = failure.get_label_link(Result.Label.CIDB)
            line = describe(job_name, failure)
            if cidb:
                line += f" ([history]({cidb}))"
            entries.append(line)
        sections.append(("Failures without a known issue:", entries))
    if known_failures:
        entries = []
        for job_name, failure in known_failures:
            issue = failure.get_label_link(Result.Label.ISSUE)
            line = describe(job_name, failure)
            if issue:
                line += f": {issue}"
            entries.append(line)
        sections.append(("Failures with an open issue:", entries))

    # GitHub rejects comments longer than 65536 characters, and history links
    # embed SQL, so list failures only while the body fits in the budget
    # (leaving room for the comment tags and the line about omitted failures).
    budget = MAX_COMMENT_LENGTH - 1000 - len("\n".join(header + footer))
    lines = list(header)
    omitted = 0
    for title, entries in sections:
        title_lines = ["", title]
        for entry in entries:
            cost = len(entry) + 1 + (sum(len(x) + 1 for x in title_lines))
            if omitted or cost > budget:
                omitted += 1
                continue
            lines += title_lines + [entry]
            budget -= cost
            title_lines = []
    if omitted:
        lines += [
            "",
            f"{omitted} more failure{'s are' if omitted != 1 else ' is'} not listed "
            "because of the comment size limit, see the CI report.",
        ]
    lines += footer
    body = "\n".join(lines) + "\n"
    assert len(body) < MAX_COMMENT_LENGTH
    print(f"\nPosting investigation request to PR #{pr_number}:\n{body}")
    if not GH.post_fresh_comment(
        tag="investigate_failures",
        body=body,
        pr=pr_number,
        repo=PUBLIC_REPO,
        verbose=False,
    ):
        print("ERROR: failed to post investigation request")
        sys.exit(1)


def print_failure_summary(known_failures, unknown_failures, not_finished_jobs, label=""):
    """Print a summary of CI failures."""
    if not (known_failures or unknown_failures or not_finished_jobs):
        return
    prefix = f"{label} " if label else ""
    print(f"\n{prefix}CI failures:")
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


def main():
    global head_sha, pr_number
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
    args = parser.parse_args()

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
        for pr_dict in my_prs_number_and_title:
            pr_number = pr_dict["number"]
            pr_title = pr_dict["title"]
            pr_menu.append((f"#{pr_number}: {pr_title}", pr_number))

        def is_pr_number(s):
            try:
                num = int(s.lstrip("#"))
                return 10000 < num < 150_000
            except ValueError:
                return False

        def is_commit_sha(s):
            return len(s) == 40 and all(c in "0123456789abcdef" for c in s.lower())

        selected_pr = UserPrompt.select_from_menu(
            pr_menu,
            "Select a PR from the list, or manually enter a commit sha or PR number",
            validator=lambda x: is_pr_number(x) or is_commit_sha(x),
        )
        if isinstance(selected_pr, str):
            if is_commit_sha(selected_pr):
                is_master_commit = True
                commit_sha = selected_pr
                pr_number = None
            elif is_pr_number(selected_pr):
                pr_number = int(selected_pr.lstrip("#"))
            else:
                raise Exception(f"Unrecognized input: {selected_pr}")
        else:
            pr_number = selected_pr[1]

    if not is_master_commit:
        pr_url = f"https://github.com/ClickHouse/ClickHouse/pull/{pr_number}"
        pr_data = Shell.get_output(
            f"gh pr view {pr_number} --json headRefOid,headRefName --repo ClickHouse/ClickHouse"
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
    print(f"CI Report: {get_job_report_url(pr_number, head_sha)}")

    if not is_master_commit:
        status_map = CommitStatusCheck.get_commit_statuses(head_sha)
        sync_status = status_map.get(CheckStatuses.CH_INC_SYNC)
        mergeable_check_status = status_map.get(CheckStatuses.MERGEABLE_CHECK)
        if (
            status_map[CheckStatuses.PR].state
            not in (
                Result.GHStatus.SUCCESS,
                Result.GHStatus.FAILURE,
            )
            and not FORCE_MERGE
        ):
            pr_status = status_map[CheckStatuses.PR]
            raise Exception(
                f"Status for {pr_status.context} is not completed: {pr_status.state} - cannot proceed"
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

    known_failures, unknown_failures, not_finished_jobs = process_workflow_failures(
        workflow_result, PUBLIC_REPO
    )

    print_failure_summary(known_failures, unknown_failures, not_finished_jobs)

    if is_master_commit:
        sys.exit(0)

    # Process the sync PR result regardless of the "CH Inc sync" commit status as stress test failures are ignored by it.
    sync_known_failures = []
    sync_unknown_failures = []
    sync_not_finished = []
    sync_checked = False
    sync_status_text = (
        f"{sync_status.state} - {sync_status.description}" if sync_status else "unknown"
    )
    print(
        f"\n=== Processing Sync PR ({CheckStatuses.CH_INC_SYNC}: {sync_status_text}) ==="
    )

    # Check proxy connectivity before proceeding
    process_sync = False
    while True:
        if Shell.check(
            f"curl -sf --connect-timeout 5 {S3_PROXY_BASE_URL} -o /dev/null 2>&1"
        ):
            process_sync = True
            break
        print(
            f"WARNING: S3 proxy at {S3_PROXY_BASE_URL} is not reachable. "
            "Connect to VPN/Tailscale to access sync PR results."
        )
        answer = UserPrompt.select_from_menu(
            [
                ("Skip sync PR processing", "skip"),
                ("Retry (after connecting to VPN)", "retry"),
            ],
            question="S3 proxy is not available",
        )
        if answer[1] == "skip":
            print("Skipping sync PR processing")
            break

    if process_sync:
        sync_pr_info = CommitStatusCheck.get_sync_pr_info(pr_number)
        if sync_pr_info:
            sync_pr_num, sync_sha = sync_pr_info
            print(f"Sync PR: {SYNC_REPO}#{sync_pr_num} (sha: {sync_sha[:12]})")
            sync_workflow_result = CommitStatusCheck.get_sync_pr_result(
                sync_pr_num, sync_sha
            )
            if sync_workflow_result:
                sync_workflow_result = (
                    sync_workflow_result.to_failed_results_with_flat_leaves()
                )
                (
                    sync_known_failures,
                    sync_unknown_failures,
                    sync_not_finished,
                ) = process_workflow_failures(sync_workflow_result, SYNC_REPO)
                sync_checked = True
                print_failure_summary(
                    sync_known_failures,
                    sync_unknown_failures,
                    sync_not_finished,
                    label="Sync",
                )
            else:
                print("WARNING: Could not fetch sync PR result - skipping")
        else:
            print("WARNING: Could not find sync PR - skipping")

    public_failed = known_failures or unknown_failures
    sync_failed = sync_known_failures or sync_unknown_failures
    if public_failed or sync_failed or not_finished_jobs or sync_not_finished:
        summary = "CI status:\n"
        if not_finished_jobs:
            summary += f" - {len(not_finished_jobs)} not finished job{'s' if len(not_finished_jobs) != 1 else ''}\n"
        if unknown_failures:
            summary += f" - {len(unknown_failures)} unknown failure{'s' if len(unknown_failures) != 1 else ''}\n"
        if known_failures:
            summary += f" - {len(known_failures)} known failure{'s' if len(known_failures) != 1 else ''}\n"
        summary += " - all other public checks passed\n"
        summary += f" - Sync status: {sync_status_text}\n"
        if sync_not_finished:
            summary += f" - Sync not finished: {len(sync_not_finished)} job{'s' if len(sync_not_finished) != 1 else ''}\n"
        if sync_failed:
            summary += f" - Sync failures: {len(sync_known_failures)} known, {len(sync_unknown_failures)} unknown\n"
    elif sync_checked:
        summary = "All checks passed! Congratulations!\n"
    else:
        summary = "All public checks passed, sync PR was not checked\n"
    print(f"\n{summary}")

    if public_failed or sync_failed:
        post_investigation_request(
            pr_number,
            head_sha,
            known_failures,
            unknown_failures,
            len(sync_known_failures) + len(sync_unknown_failures),
        )

    if public_failed:
        if not UserPrompt.confirm("Do you want to update PR CI comment?"):
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
                print("ERROR: failed to post CI summary")
        except Exception as e:
            print(f"ERROR: failed to post CI summary, ex: {e}")
            traceback.print_exc()

    if not UserPrompt.confirm(
        f"Add PR #{pr_number} to the merge queue (y - continue, n - exit)?"
    ):
        sys.exit(0)

    if Shell.check(
        f"gh pr view {pr_number} --json isDraft --jq '.isDraft' --repo ClickHouse/ClickHouse | grep -q true"
    ):
        if UserPrompt.confirm("It's a draft PR. Do you want to undraft it?"):
            Shell.check(
                f"gh pr ready {pr_number} --repo ClickHouse/ClickHouse",
                strict=True,
                verbose=True,
            )
        else:
            sys.exit(0)

    CommitStatusCheck.process_sync_status(sync_status, sha=head_sha)
    CommitStatusCheck.process_mergeable_check_status(
        mergeable_check_status, sha=head_sha
    )

    if not Git.enqueue_pull_request(pr_number, "ClickHouse/ClickHouse"):
        sys.exit(1)


if __name__ == "__main__":
    main()
