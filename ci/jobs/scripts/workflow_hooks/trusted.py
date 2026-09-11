import json
import shlex
import sys
from pathlib import Path

from praktika.info import Info
from praktika.utils import Shell

TRUSTED_CONTRIBUTORS_CONFIG = Path(__file__).parents[3] / "defs" / "trusted_contributors.json"
TRUSTED_CONTRIBUTORS = {
    login.lower()
    for login in json.loads(TRUSTED_CONTRIBUTORS_CONFIG.read_text(encoding="utf-8"))
}

CAN_BE_TESTED = "can be tested"

EXTERNAL_LABEL = "external"

INTERNAL_BOTS = {"groeneai", "oranjeai", "clickgapai"}


def get_org_members() -> set:
    """The logins (lowercased) of the ClickHouse GitHub organization members.

    Cached by `gh` for an hour so repeated calls within a job - and across the
    issue-labeling job that iterates over many authors - hit the API once.
    """
    lines = Shell.get_output(
        "gh api orgs/ClickHouse/members --paginate --cache=1h --jq='.[].login'",
        verbose=True,
    )
    return {line.strip().lower() for line in lines.splitlines() if line.strip()}


def user_in_trusted_org(user_name: str) -> bool:
    """Check if the user is in a trusted organization."""
    return user_name.lower() in get_org_members()


def label_pr_external(info) -> None:
    """Attach the `external` label to a fork pull request. Idempotent."""
    Shell.check(
        f"gh pr edit {info.pr_number} --repo {shlex.quote(info.repo_name)} "
        f"--add-label {EXTERNAL_LABEL}",
        verbose=True,
    )


def can_be_tested():
    info = Info()
    if info.repo_name == Info().fork_name:
        print("It's an internal contributor")
        return ""

    labels = info.pr_labels or []
    trusted = info.user_name.lower() in TRUSTED_CONTRIBUTORS

    in_org = None
    if EXTERNAL_LABEL not in labels and info.user_name.lower() not in INTERNAL_BOTS:
        if trusted or not (in_org := user_in_trusted_org(info.user_name)):
            label_pr_external(info)

    if trusted:
        print("It's a trusted contributor")
        return ""
    if CAN_BE_TESTED in labels:
        print("It's approved by 'can be tested' label")
        return ""
    if in_org is None:
        in_org = user_in_trusted_org(info.user_name)
    if in_org:
        print("It's an internal contributor using fork")
        return ""

    return "'can be tested' label is required"


if __name__ == "__main__":
    if can_be_tested() != "":
        sys.exit(1)
