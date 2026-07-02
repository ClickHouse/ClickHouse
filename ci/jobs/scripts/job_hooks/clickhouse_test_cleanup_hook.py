"""Post-hook: kill any test processes that outlived fast_test.py.

fast_test.py normally invokes `clickhouse-test --cleanup` in its finally block.
But if fast_test.py itself is killed with SIGKILL (e.g. OOM), that block never
runs.  This hook calls `clickhouse-test --cleanup` to perform the same cleanup:
`clickhouse-test` writes a group pid file on startup, and `--cleanup` reads it
to kill any orphaned test process groups.
"""

import subprocess
import sys
from pathlib import Path

repo_path = Path(__file__).resolve().parent.parent.parent.parent.parent

subprocess.run(
    [sys.executable, str(repo_path / "tests" / "clickhouse-test"), "--cleanup"],
    check=False,
)
