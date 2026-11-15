import json
import sys
from pathlib import Path

from praktika.info import Info
from praktika.utils import Shell

from ci.jobs.scripts.workflow_hooks.pr_labels_and_category import agent_pr_rules

TRUSTED_CONTRIBUTORS_CONFIG = Path(__file__).parents[3] / "defs" / "trusted_contributors.json"
TRUSTED_CONTRIBUTORS = {
    login.lower()
    for login in json.loads(TRUSTED_CONTRIBUTORS_CONFIG.read_text(encoding="utf-8"))
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

    # A registered automated agent (AGENT_PRS in pr_labels_and_category.py): trusted by
    # account and branch prefix. This hook runs before the label hook, so the label
    # `can be tested` that the label hook adds would come too late for the first run.
    if agent_pr_rules(info):
        print("It's a PR of a registered automated agent")
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
