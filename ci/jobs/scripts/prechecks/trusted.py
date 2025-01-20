from praktika.info import Info

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
        print("It's not a fork")
        return ""
    if Info().user_name.lower() in TRUSTED_CONTRIBUTORS:
        print("It's trusted contributor")
        return ""
    if CAN_BE_TESTED in Info().pr_labels:
        print("It's approved by label")
        return ""

    return "'can be tested' label is required"
