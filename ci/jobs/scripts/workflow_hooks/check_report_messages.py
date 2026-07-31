"""
Workflow hook that blocks merge whenever any job has posted an error or
warning to the workflow-level report (``Result.ext["errors"]`` /
``Result.ext["warnings"]`` on the top-level workflow result).

Warnings are treated as blocking on purpose: if a job decides to surface a
warning on the workflow page, it is something a human should acknowledge
before the PR is merged.  After the issue is reviewed (and, if appropriate,
fixed in a follow-up), the merge can proceed by re-running CI once the
warning is no longer produced.
"""

import sys

from ci.praktika.info import Info
from ci.praktika.result import Result


def check():
    info = Info()
    workflow_result = Result.from_fs(info.workflow_name)
    ext = workflow_result.ext

    errors = ext.get("errors", [])
    warnings = ext.get("warnings", [])

    ok = True
    for item in errors:
        print(f"ERROR: {item.get('message', '')} (from: {item.get('from', '')})")
        ok = False
    for item in warnings:
        print(f"WARNING: {item.get('message', '')} (from: {item.get('from', '')})")
        ok = False

    return ok


if __name__ == "__main__":
    if not check():
        sys.exit(1)
