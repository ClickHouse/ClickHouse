from dataclasses import dataclass, field
from typing import List

from .settings import Settings
from .utils import MetaClasses, Utils


@dataclass
class TestingIssue:
    """Represents a GitHub issue found/occurred in CI"""

    test_name: str
    closed_at: str
    issue: int
    issue_url: str
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

    def validate(self):
        if not self.test_name or not self.issue_url or not self.title:
            print(
                f"WARNING: Invalid issue [{self.issue_url}] must have test_name, issue_url and title"
            )
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
                    f"WARNING: Invalid infrastructure issue [{self.issue_url}] must have job_pattern, test_pattern or failure_reason"
                )
                return False
        return True


@dataclass
class TestCaseIssueCatalog(MetaClasses.Serializable):
    """Catalog of all flaky test issues, both active and resolved"""

    name: str = "issue_catalog"
    active_test_issues: List[TestingIssue] = field(default_factory=list)
    resolved_test_issues: List[TestingIssue] = field(default_factory=list)

    @classmethod
    def file_name_static(cls, name):
        return f"{Settings.TEMP_DIR}/{Utils.normalize_string(name)}.json"

    @classmethod
    def from_dict(cls, obj: dict):
        """Custom deserialization to handle nested TestCaseIssue objects"""
        active_issues = [
            TestingIssue(**issue) if isinstance(issue, dict) else issue
            for issue in obj.get("active_test_issues", [])
        ]
        resolved_issues = [
            TestingIssue(**issue) if isinstance(issue, dict) else issue
            for issue in obj.get("resolved_test_issues", [])
        ]
        return cls(
            name=obj.get("name", "flaky_test_catalog"),
            active_test_issues=active_issues,
            resolved_test_issues=resolved_issues,
        )
