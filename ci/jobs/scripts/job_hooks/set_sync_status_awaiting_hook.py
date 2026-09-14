from ci.defs.defs import SYNC
from ci.praktika.gh import GH
from ci.praktika.info import Info
from ci.praktika.result import Result

DOCS_ONLY_SYNC_STATUS_DESCRIPTION = "skipped: documentation-only change"
RETAINED_DOCS_PATH_PREFIXES = (
    "docs/changelogs/",
    "docs/private-changelogs/",
)

# This status is a marker that the sync process can be started. We set it from
# the `Code Review` job because that job always runs for PRs.


def can_skip_sync(changed_files):
    """Return whether all changes can wait for the periodic master sync."""
    normalized_files = [
        file.removeprefix(".").removeprefix("/") for file in changed_files or []
    ]
    return bool(normalized_files) and all(
        file.startswith("docs/")
        and not any(
            file.startswith(prefix) for prefix in RETAINED_DOCS_PATH_PREFIXES
        )
        for file in normalized_files
    )


def main():
    info = Info()
    if info.repo_name != "ClickHouse/ClickHouse":
        print(f"Not applicable for repo [{info.repo_name}], skipping")
        return

    statuses = GH.get_commit_statuses()
    if statuses is None:
        print(f"Failed to fetch commit statuses, skip setting [{SYNC}]")
        return

    if SYNC in statuses:
        print(
            f"Commit status [{SYNC}] already exists with description "
            f"[{statuses[SYNC].description}], skipping"
        )
        return

    changed_files = info.get_changed_files()
    if can_skip_sync(changed_files):
        GH.post_commit_status(
            name=SYNC,
            status=Result.Status.OK,
            description=DOCS_ONLY_SYNC_STATUS_DESCRIPTION,
            url="",
        )
    else:
        GH.post_commit_status(
            name=SYNC,
            status=Result.Status.PENDING,
            description="awaiting",
            url="",
        )


if __name__ == "__main__":
    main()
