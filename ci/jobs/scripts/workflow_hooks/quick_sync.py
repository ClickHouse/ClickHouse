import json
import subprocess
import sys
import traceback

from ci.defs.defs import SYNC
from ci.praktika.gh import GH
from ci.praktika.info import Info


def trigger_private_sync():
    info = Info()

    # Optional allow-list
    if info.user_name not in ("yariks5s", "maxknv"):
        print(f"Quick sync not enabled for user [{info.user_name}]")
        return

    # Completely safe payload – no shlex, no --field, no injection possible
    inputs = {
        "pr_number": str(info.pr_number),
        "branch_name": str(info.git_branch),
        "title": str(info.pr_title),   # can be anything, 100% safe now
        "sha": str(info.sha),
    }

    cmd = [
        "gh", "workflow", "run", "private_quick_sync.yml",
        "--repo", "ClickHouse/clickhouse-private",
        "--ref", "master",
        "-F", f"inputs={json.dumps(inputs)}"
    ]

    try:
        subprocess.check_call(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        GH.post_commit_status(
            name=SYNC,
            status="pending",
            description="sync started",
            url=""
        )
        print("Quick sync triggered successfully")
    except subprocess.CalledProcessError as e:
        GH.post_commit_status(
            name=SYNC,
            status="error",
            description="failed to start the sync",
            url=""
        )
        print("Failed to trigger quick sync")
        sys.exit(1)
    except Exception as e:
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    trigger_private_sync()
