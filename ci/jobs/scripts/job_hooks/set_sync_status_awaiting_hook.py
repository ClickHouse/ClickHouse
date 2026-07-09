from ci.defs.defs import SYNC
from ci.praktika.gh import GH
from ci.praktika.result import Result

# This status is a marker that the sync process can be started. We set it from
# the `Code Review` job because that job always runs for PRs.


def main():
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

    GH.post_commit_status(
        name=SYNC,
        status=Result.Status.PENDING,
        description="awaiting",
        url="",
    )


if __name__ == "__main__":
    main()
