from dataclasses import dataclass, field
from typing import List

from .settings import Settings
from .utils import MetaClasses, Utils


@dataclass
class TestingIssue:
    """Represents a single flaky test issue from GitHub"""

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


@dataclass
class TestCaseIssueCatalog(MetaClasses.Serializable):
    """Catalog of all flaky test issues, both active and resolved"""

    name: str = "flaky_test_catalog"
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
