import sys
import traceback

from praktika.info import Info
from praktika.utils import Shell


def check():
    info = Info()
    Shell.check(
        f"gh workflow run private_quick_sync.yml --repo ClickHouse/clickhouse-private --ref ci_quick_sync --field pr_number={info.pr_number} --field branch_name={info.git_branch} --field title='{info.pr_title}' --field sha={info.sha}",
        verbose=True,
        strict=True,
    )


if __name__ == "__main__":
    try:
        check()
    except Exception as e:
        print("Failed to initiate sync")
        traceback.print_exc()
        sys.exit(1)
