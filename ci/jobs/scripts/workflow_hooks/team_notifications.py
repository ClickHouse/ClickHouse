import sys

from ci.praktika.gh import GH
from ci.praktika.info import Info

INTEGRATIONS_ECOSYSTEM_FILES = ("src/Core/TypeId.h",)
PRAKTIKA_PREFIX = "ci/praktika/"
PRAKTIKA_REVIEWERS = ("maxknv", "leshikus")


def normalize_path(file):
    return file.removeprefix(".").removeprefix("/")


def has_praktika_changes(changed_files):
    directory = PRAKTIKA_PREFIX.removesuffix("/")
    return any(
        file == directory or file.startswith(PRAKTIKA_PREFIX)
        for file in (normalize_path(file) for file in changed_files)
    )


def request_praktika_reviewers(info, changed_files):
    if not has_praktika_changes(changed_files):
        print(f"No [{PRAKTIKA_PREFIX}] changes found, skip reviewer requests")
        return True

    author = info.user_name.lower()
    reviewers = [
        reviewer for reviewer in PRAKTIKA_REVIEWERS if reviewer.lower() != author
    ]
    if not reviewers:
        print("Skip reviewer requests: every configured reviewer is the PR author")
        return True

    print(
        f"Requesting [{', '.join(reviewers)}] as reviewers for "
        f"[{PRAKTIKA_PREFIX}] changes"
    )
    return GH.request_user_reviews(reviewers, pr=info.pr_number, repo=info.repo_name)


def check():
    info = Info()

    changed_files = info.get_kv_data("changed_files")
    assert changed_files is not None, (
        "changed_files is not populated in JOB_KV_DATA: the store_data pre-hook "
        "most likely failed to fetch the PR file list from the GitHub API. "
        "See the Config Workflow logs for the underlying error."
    )

    if any(
        file.startswith(prefix)
        for file in changed_files
        for prefix in INTEGRATIONS_ECOSYSTEM_FILES
    ):
        GH.post_updateable_comment(
            comment_tags_and_bodies={
                "team_notification": "@ClickHouse/integrations team,  please, take a look"
            }
        )

    return request_praktika_reviewers(info, changed_files)


if __name__ == "__main__":
    if not check():
        sys.exit(1)
