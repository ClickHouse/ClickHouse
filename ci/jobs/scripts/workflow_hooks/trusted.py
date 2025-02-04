import sys

from praktika.info import Info
from praktika.utils import Shell

TRUSTED_CONTRIBUTORS = {
    e.lower()
    for e in [
        "amosbird",
        "azat",  # SEMRush
        "bharatnc",  # Many contributions.
        "den-crane",  # Documentation contributor
        "taiyang-li",
        "ucasFL",  # Amos Bird's friend
        "tonickkozlov",  # Cloudflare
    ]
}

CAN_BE_TESTED = "can be tested"


def can_be_trusted():
    if Info().repo_name == Info().fork_name:
        print("It's an internal contributor")
        return ""
    if Info().user_name.lower() in TRUSTED_CONTRIBUTORS:
        print("It's a trusted contributor")
        return ""
    if CAN_BE_TESTED in Info().pr_labels:
        print("It's approved by 'can be tested' label")
        return ""
    org = Shell.check(f"gh api users/{Info().user_name}/orgs --jq '.[].login'")
    if org == "ClickHouse":
        print("It's an internal contributor using fork - why?")
        return ""

    return "'can be tested' label is required"


if __name__ == "__main__":
    if can_be_trusted() != "":
        sys.exit(1)
