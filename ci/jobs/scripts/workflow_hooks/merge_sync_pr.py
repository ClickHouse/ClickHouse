import traceback

from praktika.info import Info
from praktika.utils import Shell

SYNC_REPO = "ClickHouse/clickhouse-private"


def check():
    info = Info()

    if not info.linked_pr_number > 0:
        print(f"WARNING: Invalid or unknown pr number: {info.linked_pr_number}")
        return

    sync_pr_number = Shell.get_output(
        f"gh pr list --state open --head sync-upstream/pr/{info.linked_pr_number} --repo {SYNC_REPO} --json number --jq '.[].number'",
        verbose=True,
    ).splitlines()[0]
    if not sync_pr_number.isdigit() or not int(sync_pr_number):
        print("WARNING: Failed to retrieve Sync PR number")
        return

    if not Shell.check(
        f"gh pr ready {sync_pr_number} --repo {SYNC_REPO}", verbose=True
    ):
        print("WARNING: Failed to set Sync PR as ready")
        return

    if not Shell.check(
        f"gh pr merge {sync_pr_number} --repo {SYNC_REPO} --merge", verbose=True
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
