import sys

from praktika.info import Info
from praktika.utils import Shell

TRUSTED_CONTRIBUTORS = {
    e.lower()
    for e in [
        "amosbird",
        "den-crane",  # Documentation contributor
        "taiyang-li",
        "ucasFL",  # Amos Bird's friend
        "canhld94",
        "uladzislauNestsiaruk",  # Student working on https://github.com/ClickHouse/ClickHouse/pull/91416, remove by 05/2026
    ]
}

CAN_BE_TESTED = "can be tested"


def user_in_trusted_org(user_name: str) -> bool:
    """Check if the user is in a trusted organization."""
    lines = Shell.get_output(
        "gh api orgs/ClickHouse/members --paginate --cache=1h --jq='.[].login'",
        verbose=True,
    )
    return user_name in [line.strip() for line in lines.splitlines() if line.strip()]


def can_be_tested():
    info = Info()
    if info.repo_name == Info().fork_name:
        print("It's an internal contributor")
        return ""
    if info.user_name.lower() in TRUSTED_CONTRIBUTORS:
        print("It's a trusted contributor")
        return ""
    # we need runtime labels info, info.pr_labels might be non relevant in case of job rerun
    if CAN_BE_TESTED in Shell.get_output(
        f"gh pr view {info.pr_number} --json labels --jq '.labels[].name'"
    ):
        print("It's approved by 'can be tested' label")
        return ""
    if user_in_trusted_org(info.user_name):
        print("It's an internal contributor using fork")
        return ""

    return "'can be tested' label is required"


if __name__ == "__main__":
    if can_be_tested() != "":
        sys.exit(1)
