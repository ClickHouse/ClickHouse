import json
import os
import re
import sys
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, List, Optional

sys.path.append(os.path.dirname(__file__) + "/../")

from praktika.gh import GH
from praktika.result import Result
from praktika.s3 import S3
from praktika.utils import MetaClasses, Shell, Utils

if TYPE_CHECKING:
    pass


def parse_issue_body_fields(body: str) -> dict:
    """
    Parse structured fields from issue body.

    Expected format in body:
    - Test name: <content>
    - Failure reason: <content>
    - Failure flags: <flag1>, <flag2>, ...
    - CI action: <content>
    - Test pattern: <content>
    - Job pattern: <content>

    Returns:
        Dictionary with parsed fields
    """
    fields = {
        "test_name": "",
        "failure_reason": "",
        "failure_flags": [],
        "ci_action": "",
        "test_pattern": "",
        "job_pattern": "",
    }

    if not body:
        return fields

    # Parse each field with regex
    # Pattern: field name followed by colon, then content until end of line
    patterns = {
        "test_name": r"Test name:\s*(.+?)(?:\n|$)",
        "failure_reason": r"Failure reason:\s*(.+?)(?:\n|$)",
        "ci_action": r"CI action:\s*(.+?)(?:\n|$)",
        "test_pattern": r"Test pattern:\s*(.+?)(?:\n|$)",
        "job_pattern": r"Job pattern:\s*(.+?)(?:\n|$)",
    }

    for field, pattern in patterns.items():
        match = re.search(pattern, body, re.IGNORECASE)
        if match:
            fields[field] = match.group(1).strip()

    # Parse failure flags (comma-separated list)
    flags_match = re.search(
        r"^Failure flags:[ \t]*(.*)$", body, re.IGNORECASE | re.MULTILINE
    )
    fields["failure_flags"] = []
    if flags_match:
        flags_str = flags_match.group(1).strip()
        # Split by comma and strip whitespace from each flag
        if flags_str:
            fields["failure_flags"] = [
                flag.strip() for flag in flags_str.split(",") if flag.strip()
            ]

    return fields


def extract_test_name(title: str) -> Optional[str]:
    pattern1 = r"\b(\d{5}_\S+)"
    match1 = re.search(pattern1, title)
    if match1:
        test_name = match1.group(1)
        # Strip trailing quotes, backticks, and other punctuation
        return test_name.rstrip("`'\",.;:!?)")

    # Pattern 2: pytest-style names that start with test_, allowing rich suffixes
    # Use a negated class to stop at whitespace or closing punctuation/quotes.
    pattern2 = r"\b(test_[^\s`'\",]+)"
    match2 = re.search(pattern2, title)
    if match2:
        test_name = match2.group(1)
        # Strip trailing quotes, backticks, and other punctuation
        return test_name.rstrip("`'\",.;:!?)")
    return None


def fetch_github_issues(
    label: str, state: str = "open", hours_back: float = None
) -> List[dict]:
    """
    Fetch issues from GitHub using gh CLI with manual pagination.

    Args:
        label: GitHub label to filter by
        state: Issue state (open or closed)
        hours_back: For closed issues, only fetch those closed within this many hours (default: None for all)

    Returns:
        List of issue dictionaries
    """
    if isinstance(label, str):
        label = [label]
    all_issues = []
    limit_per_request = 1000  # Maximum we'll fetch per request

    # Build base command
    if state == "closed" and hours_back:
        date_threshold = (datetime.now() - timedelta(hours=hours_back)).strftime(
            "%Y-%m-%dT%H:%M:%S"
        )
        label_query = " ".join([f'label:"{lbl}"' for lbl in label])
        search_query = f"{label_query} is:closed closed:>{date_threshold}"
        base_cmd = f"gh issue list --search '{search_query}' --json number,title,body,closedAt,labels --limit {limit_per_request}"
        print(
            f"Fetching {state} issues with label '{label}' closed in last {hours_back} hours (since {date_threshold})..."
        )
    else:
        label_args = " ".join([f'--label "{lbl}"' for lbl in label])
        base_cmd = f"gh issue list {label_args} --state {state} --json number,title,body,closedAt,labels --limit {limit_per_request}"
        print(f"Fetching {state} issues with label '{label}'...")

    try:
        output = Shell.get_output(base_cmd, verbose=True)

        if not output or not output.strip():
            print(f"  No issues found for label '{label}' with state '{state}'")
            return []

        issues = json.loads(output)
        if not issues:
            print(f"  No issues found for label '{label}' with state '{state}'")
            return []

        all_issues.extend(issues)
        print(f"  Found {len(all_issues)} issues")

        # If the limit was hit, print a warning — there may be more issues to page
        if len(issues) == limit_per_request:
            print(
                f"  WARNING: Reached limit of {limit_per_request} issues. There may be more issues not fetched."
            )

        return all_issues
    except json.JSONDecodeError as e:
        print(f"ERROR: Failed to parse JSON response for label '{label}': {e}")
        return []
    except Exception as e:
        print(f"ERROR: Failed to fetch issues with label '{label}': {e}")
        return []


class IssueLabels:
    CI_ISSUE = "testing"
    FLAKY_TEST = "flaky test"
    FUZZ = "fuzz"
    INFRASTRUCTURE = "infrastructure"


@dataclass
class Issue:
    """Represents a GitHub issue found/occurred in CI"""

    test_name: str
    closed_at: str
    number: int
    url: str
    title: str
    body: str
    labels: List[str]

    failure_reason: str = ""
    failure_flags: List[str] = field(default_factory=list)
    ci_action: str = ""
    test_pattern: str = ""
    job_pattern: str = ""

    def is_infrastructure(self):
        return "infrastructure" in self.labels

    def has_failure_reason(self):
        return bool(self.failure_reason)

    def has_failure_flags(self):
        return bool(self.failure_flags)

    def has_job_pattern(self):
        return bool(self.job_pattern) and self.job_pattern != "%"

    def has_test_pattern(self):
        return bool(self.test_pattern) and self.test_pattern != "%"

    def validate(self, verbose=True):
        if not self.test_name and not self.is_infrastructure():
            if verbose:
                print(f"WARNING: Invalid issue [{self.url}] must have test_name")
            return False
        if not self.url:
            if verbose:
                print(f"WARNING: Invalid issue [{self.url}] must have issue_url")
            return False
        if not self.title:
            print(f"WARNING: Invalid issue [{self.url}] must have title")
            return False
        if self.is_infrastructure():
            if (
                self.has_job_pattern()
                or self.has_test_pattern()
                or self.has_failure_reason()
            ):
                return True
            else:
                print(
                    f"WARNING: Invalid infrastructure issue [{self.url}] must have job_pattern, test_pattern or failure_reason"
                )
                return False
        return True

    def check_result(self, result: Result, job_name: str = "") -> bool:
        """
        Check if this issue matches the given result and mark it if it does.
        Recursively processes result tree.

        Args:
            result: The result to check
            job_name: The top-level job name for infrastructure job_pattern matching

        Returns:
            True if the result was marked with this issue, False otherwise
        """
        # Skip if result is OK or already marked
        if result.is_ok() or result.has_label("issue"):
            return False

        # Recursively check sub-results first.
        # If called for the workflow result, its sub-results are jobs and their names should be used
        # for job_pattern matching.
        if result.results:
            marked = False
            is_workflow_level_call = not job_name
            for sub_result in result.results:
                sub_job_name = sub_result.name if is_workflow_level_call else job_name
                if self.check_result(sub_result, job_name=sub_job_name):
                    marked = True
            return marked

        # Leaf result - check if it matches.
        return (
            self._check_infrastructure_match(result, job_name)
            if self.is_infrastructure()
            else self._check_flaky_test_match(result)
        )

    def _check_flaky_test_match(self, result: Result) -> bool:
        """Check if this flaky test issue matches the result."""
        # Only check flaky tests and fuzz issues
        if not any(
            l in self.labels for l in [IssueLabels.FLAKY_TEST, IssueLabels.FUZZ]
        ):
            return False

        name_in_report = result.name
        test_name = self.test_name

        # Normalize pytest parameterized names
        if ".py" in name_in_report:
            if "[" in test_name:
                # Issue mentions a specific parametrization: keep full name for exact match
                pass
            elif "[" in name_in_report:
                # Issue mentions only the base test: compare against the base part
                name_in_report = name_in_report.split("[")[0]

        # Check if test name matches
        if not name_in_report.endswith(test_name):
            return False

        # Check failure_reason if present
        if self.failure_reason and self.failure_reason not in result.info:
            print(
                "WARNING: matched test name, but failure reason not found in output\n"
                f"  test:   {test_name}\n"
                f"  reason: {self.failure_reason}\n"
                f"  issue:  #{self.number}\n"
                "  action: do not match"
            )
            return False

        print(
            f"Marking '{result.name}' as flaky (matched: {test_name}, issue: #{self.number})"
        )
        result.set_clickable_label(label="issue", link=self.url)
        return True

    def _check_infrastructure_match(
        self, result: Result, top_level_result_name: str
    ) -> bool:
        """Check if this infrastructure issue matches the result."""
        # Check failure_reason if present
        if self.failure_reason and self.failure_reason not in result.info:
            return False

        # Check failure_flags with result labels
        if self.failure_flags:
            if any(not result.has_label(flag) for flag in self.failure_flags):
                return False

        # Check test_pattern (SQL style %name%) with result.name
        if self.test_pattern and self.test_pattern != "%":
            pattern_parts = [p for p in self.test_pattern.split("%") if p]
            if not any(part in result.name for part in pattern_parts):
                return False

        # Check job_pattern (SQL style %name%) with top level result name
        if self.job_pattern and self.job_pattern != "%":
            job_pattern_parts = [p for p in self.job_pattern.split("%") if p]
            if not any(part in top_level_result_name for part in job_pattern_parts):
                return False

        print(
            f"  Marking '{result.name}' as infrastructure issue (issue: #{self.number})"
        )
        result.set_clickable_label(label="issue", link=self.url)
        return True

    @classmethod
    def create_from(
        cls,
        title,
        body,
        labels,
        closed_at="",
        number=0,
    ):
        body_fields = parse_issue_body_fields(body)
        test_name = body_fields["test_name"]
        if not test_name:
            if IssueLabels.FLAKY_TEST in labels:
                # Extract test name from title or body
                test_name = extract_test_name(title)
            else:
                test_name = title
        issue_url = (
            f"https://github.com/ClickHouse/ClickHouse/issues/{number}"
            if number
            else ""
        )
        return Issue(
            test_name=test_name,
            closed_at=closed_at if closed_at else "",
            number=int(number),
            url=issue_url,
            title=title,
            body=body if body else "",
            labels=labels,
            failure_reason=body_fields["failure_reason"],
            failure_flags=body_fields["failure_flags"],
            ci_action=body_fields["ci_action"],
            test_pattern=body_fields["test_pattern"],
            job_pattern=body_fields["job_pattern"],
        )

    def create_on_gh(self, repo_name):
        assert self.number == 0 and self.url == ""
        self.url = GH.create_issue(
            self.title, self.body, self.labels, repo=repo_name, verbose=True
        )
        self.number = int(self.url.split("/")[-1]) if self.url else 0
        return self

    @classmethod
    def from_dict(cls, issue: dict) -> "Issue":
        """
        Process raw GitHub issue into TestCaseIssue objects.

        Args:
            issue: raw issue dictionary from GitHub

        Returns:
            TestCaseIssue object
        """

        number = int(issue.get("number", "0"))
        title = issue.get("title", "")
        body = issue.get("body", "")
        closed_at = issue.get("closedAt", "")
        assert number > 0, f"Issue {number} has no number"
        assert title, f"Issue {number} has no title"
        # Extract label names from the labels array
        labels = [label.get("name", "") for label in issue.get("labels", [])]
        return cls.create_from(title, body, labels, closed_at, number)


@dataclass
class TestCaseIssueCatalog(MetaClasses.Serializable):
    """Catalog of all flaky test issues"""

    name: str = "issue_catalog"
    active_test_issues: List[Issue] = field(default_factory=list)
    updated_at: float = 0.0

    @classmethod
    def file_name_static(cls, name):
        return f"/tmp/{Utils.normalize_string(name)}.json"

    @classmethod
    def from_dict(cls, obj: dict):
        """Custom deserialization to handle nested TestCaseIssue objects"""
        active_issues = [
            Issue(**issue) if isinstance(issue, dict) else issue
            for issue in obj.get("active_test_issues", [])
        ]
        return cls(
            name=obj.get("name", cls.name),
            active_test_issues=active_issues,
            updated_at=obj.get("updated_at", 0.0),
        )

    @classmethod
    def process_issue(cls, issues_raw, verbose=True):
        res = []
        for issue_ in issues_raw:
            issue = Issue.from_dict(issue_)
            if issue.validate(verbose=verbose):
                res.append(issue)
        return res

    @classmethod
    def from_gh(cls, verbose=True):
        """
        Fetch and organize all CI test issues from GitHub.

        Returns:
            TestCaseIssueCatalog with active issues (including recently closed)
        """
        catalog = cls()
        catalog.updated_at = datetime.now().timestamp()

        # Fetch open issues with label "testing" (CI_ISSUE) first
        if verbose:
            print("\n--- Fetching active testing issues ---")
        testing_issues = fetch_github_issues(label=IssueLabels.CI_ISSUE, state="open")
        catalog.active_test_issues = cls.process_issue(testing_issues, verbose=verbose)
        if verbose:
            print(f"Processed {len(catalog.active_test_issues)} testing issues")

        # Fetch closed issues with label "testing" from the last 8 hours and add to active issues
        if verbose:
            print("--- Fetching closed testing issues from last 8 hours ---")
        closed_testing_issues = fetch_github_issues(
            label=IssueLabels.CI_ISSUE, state="closed", hours_back=8
        )
        closed_testing_processed = cls.process_issue(
            closed_testing_issues, verbose=verbose
        )
        added_closed_testing = 0
        existing_issue_numbers = {issue.number for issue in catalog.active_test_issues}
        for issue in closed_testing_processed:
            if issue.number not in existing_issue_numbers:
                catalog.active_test_issues.append(issue)
                existing_issue_numbers.add(issue.number)
                added_closed_testing += 1
        if verbose:
            print(
                f"Added {added_closed_testing} closed testing issues (total: {len(catalog.active_test_issues)})"
            )

        if verbose:
            # Print summary by label for all issues
            print("=== Issues Summary by Label ===")
            label_counts = {}
            for issue in catalog.active_test_issues:
                for label in issue.labels:
                    label_counts[label] = label_counts.get(label, 0) + 1

            # Sort by count descending
            sorted_labels = sorted(
                label_counts.items(), key=lambda x: x[1], reverse=True
            )
            for label, count in sorted_labels:
                print(f"  {label}: {count}")
            print()

        return catalog

    def to_s3(self):
        """
        Upload catalog to S3.

        Returns:
            S3 URL of the uploaded catalog
        """
        local_name = self.file_name()
        compressed_name = Utils.compress_gz(local_name)
        link = S3.copy_file_to_s3(
            local_path=compressed_name,
            s3_path=f"clickhouse-test-reports/statistics",
            content_type="application/json",
            content_encoding="gzip",
        )
        return link

    @classmethod
    def from_s3(cls):
        """
        Download catalog from S3.


        Returns:
            TestCaseIssueCatalog instance or None if download failed
        """
        local_catalog_gz = cls.file_name_static(cls.name) + ".gz"
        local_catalog_json = cls.file_name_static(cls.name)
        s3_catalog_path = f"clickhouse-test-reports/statistics/{Utils.normalize_string(cls.name)}.json.gz"

        if not S3.copy_file_from_s3(
            s3_catalog_path, local_catalog_gz, _skip_download_counter=True
        ):
            print(f"  WARNING: Could not download catalog from S3: {s3_catalog_path}")
            return None

        # Decompress the file
        Shell.check(f"gunzip -f {local_catalog_gz}", verbose=True)

        # Load from decompressed file
        if not Path(local_catalog_json).exists():
            print(
                f"  WARNING: Decompressed catalog file not found: {local_catalog_json}"
            )
            return None

        return cls.from_file(local_catalog_json)


if __name__ == "__main__":
    temp_path = f"{Utils.cwd()}/ci/tmp"
    Path(temp_path).mkdir(exist_ok=True)

    def collect_and_upload():
        TestCaseIssueCatalog.from_gh().dump()  # .to_s3()
        return True

    def download():
        TestCaseIssueCatalog.from_s3().dump()
        return True

    results = []

    if "--collect-and-upload" in sys.argv:
        results.append(
            Result.from_commands_run(
                name="Collect and upload flaky test issues",
                command=lambda: collect_and_upload(),
            )
        )
    elif "--download" in sys.argv:
        results.append(
            Result.from_commands_run(
                name="Download flaky test issues", command=lambda: download()
            )
        )
    else:
        print("ERROR: No action specified")
        raise

    Result.create_from(status=Result.Status.SUCCESS).complete_job()
