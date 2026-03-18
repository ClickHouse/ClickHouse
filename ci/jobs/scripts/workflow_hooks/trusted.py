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
        "tonickkozlov",  # Cloudflare
        "canhld94",
    ]
}

CAN_BE_TESTED = "can be tested"


def can_be_trusted():
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
    orgs = Shell.get_output(
        f"gh api users/{Info().user_name}/orgs --jq '.[].login'", verbose=True
    )
    if "ClickHouse" in [line.strip() for line in orgs.splitlines() if line.strip()]:
        print("It's an internal contributor using fork")
        return ""

    return "'can be tested' label is required"


if __name__ == "__main__":
    if can_be_trusted() != "":
        sys.exit(1)
