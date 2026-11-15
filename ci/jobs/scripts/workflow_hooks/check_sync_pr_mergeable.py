import sys
import time
import traceback

from praktika.info import Info
from praktika.utils import Shell

SYNC_REPO = "ClickHouse/clickhouse-private"


def check():
    info = Info()

    if not info.linked_pr_number > 0:
        print(f"WARNING: Invalid or unknown pr number: {info.linked_pr_number}")
        return True

    command = (
        f"gh pr list --state open --head sync-upstream/pr/{info.linked_pr_number} "
        f"--repo {SYNC_REPO} --json number --jq '.[].number'"
    )
    retries = 3
    delay = 2
    returncode, raw, stderr = 1, "", ""
    for attempt in range(retries):
        returncode, raw, stderr = Shell.get_res_stdout_stderr(command, verbose=True)
        if returncode == 0:
            break
        print(
            f"WARNING: command failed (attempt {attempt + 1}/{retries}), "
            f"exit_code {returncode}, stderr:\n>>>\n{stderr}\n<<<"
        )
        if attempt < retries - 1:
            print(f"Retrying in {delay}s...")
            time.sleep(delay)
            delay = min(2 * delay, 60)

    if returncode != 0:
        # A non-zero exit means `gh` could not query the Sync repo at all - most
        # likely the minted token lacks access to the private repo. Fail loudly
        # instead of silently skipping the check.
        print(
            f"ERROR: Failed to retrieve Sync PR list from {SYNC_REPO} after "
            f"{retries} attempts (exit_code {returncode}), stderr:\n>>>\n{stderr}\n<<<"
        )
        return False

    sync_pr_numbers = [n for n in raw.splitlines() if n.strip()]

    if len(sync_pr_numbers) == 0:
        print("WARNING: No open Sync PR found - skipping check")
        return True

    if len(sync_pr_numbers) > 1:
        print(
            f"ERROR: Expected at most one open Sync PR for branch sync-upstream/pr/{info.linked_pr_number}, "
            f"found {len(sync_pr_numbers)}: {sync_pr_numbers}"
        )
        return False

    sync_pr_number = sync_pr_numbers[0]

    mergeable = Shell.get_output(
        f"gh pr view {sync_pr_number} --repo {SYNC_REPO} --json mergeable --jq .mergeable",
        verbose=True,
    )
    if not mergeable or mergeable == "UNKNOWN":
        print(
            f"WARNING: Sync PR #{sync_pr_number} mergeable state is '{mergeable}', retrying in 5s..."
        )
        time.sleep(5)
        mergeable = Shell.get_output(
            f"gh pr view {sync_pr_number} --repo {SYNC_REPO} --json mergeable --jq .mergeable",
            verbose=True,
        )
    if not mergeable or mergeable == "UNKNOWN":
        print(
            f"WARNING: Sync PR #{sync_pr_number} mergeable state is still '{mergeable}' - skipping check"
        )
        return True

    if mergeable == "CONFLICTING":
        print(
            f"ERROR: Sync PR #{sync_pr_number} in {SYNC_REPO} has conflicts and cannot be merged"
        )
        return False

    print(
        f"Sync PR #{sync_pr_number} in {SYNC_REPO} is mergeable (state: {mergeable})"
    )
    return True


if __name__ == "__main__":
    try:
        if not check():
            sys.exit(1)
    except Exception:
        traceback.print_exc()
        sys.exit(1)
