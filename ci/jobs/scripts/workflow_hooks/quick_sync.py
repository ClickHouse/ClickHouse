import shlex
import sys
import traceback

from ci.defs.defs import SYNC
from ci.praktika.gh import GH
from ci.praktika.info import Info
from ci.praktika.utils import Shell


def check():
    info = Info()
    if info.user_name not in ("yariks5s", "maxknv"):
        print(f"Not enabled for [{info.user_name}]")
        return
    cmd = (
        "gh workflow run private_quick_sync.yml --repo ClickHouse/clickhouse-private --ref master "
        + f"--field pr_number={shlex.quote(str(info.pr_number))} "
        + f"--field branch_name={shlex.quote(str(info.git_branch))} "
        + f"--field title={shlex.quote(str(info.pr_title))} "
        + f"--field sha={shlex.quote(str(info.sha))}"
    )

    if not Shell.check(cmd):
        GH.post_commit_status(
            name=SYNC, status="error", description="failed to start the sync", url=""
        )
    else:
        GH.post_commit_status(
            name=SYNC, status="pending", description="sync started", url=""
        )


if __name__ == "__main__":
    try:
        check()
    except Exception as e:
        print("Failed to initiate sync")
        traceback.print_exc()
        sys.exit(1)
