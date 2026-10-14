import traceback

from praktika.info import Info
from praktika.utils import Shell

SYNC_REPO = "ClickHouse/clickhouse-private"


def check():
    info = Info()

    if not info.linked_pr_number > 0:
        print(f"WARNING: Invalid or unknown pr number: {info.linked_pr_number}")
        return

    raw = Shell.get_output(
        f"gh pr list --state open --head sync-upstream/pr/{info.linked_pr_number} --repo {SYNC_REPO} --json number --jq '.[].number'",
        verbose=True,
        retries=5,
    )
    if not raw:
        print("WARNING: Failed to retrieve Sync PR list after retries - skipping merge")
        return

    sync_pr_numbers = [n.strip() for n in raw.splitlines() if n.strip()]

    if len(sync_pr_numbers) == 0:
        print("WARNING: No open Sync PR found - skipping merge")
        return

    if len(sync_pr_numbers) > 1:
        print(
            f"WARNING: Expected at most one open Sync PR for branch sync-upstream/pr/{info.linked_pr_number}, "
            f"found {len(sync_pr_numbers)}: {sync_pr_numbers} - skipping merge"
        )
        return

    sync_pr_number = sync_pr_numbers[0]
    if not sync_pr_number.isdigit() or not int(sync_pr_number):
        print("WARNING: Failed to retrieve Sync PR number")
        return

    if not Shell.check(
        f"gh pr ready {sync_pr_number} --repo {SYNC_REPO}", verbose=True, retries=5
    ):
        print("WARNING: Failed to set Sync PR as ready")
        return

    if not Shell.check(
        f"gh pr merge {sync_pr_number} --repo {SYNC_REPO} --merge",
        verbose=True,
        retries=5,
    ):
        print("WARNING: Failed to merge Sync PR")
        return

    print("Sync PR merged")
    return


if __name__ == "__main__":
    try:
        check()
    except Exception:
        traceback.print_exc()
