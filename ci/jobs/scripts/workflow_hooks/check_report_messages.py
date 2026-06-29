"""
Workflow hook that posts a GH commit status summarising errors and warnings
from the workflow-level report (``Result.ext["errors"]`` /
``Result.ext["warnings"]`` on the top-level workflow result).

The hook itself never fails so that it does not block the post-hook step;
the commit status it posts (FAIL when issues are present) is what blocks
the merge.  After the issue is reviewed and fixed, re-running CI will post
a new OK status once the error/warning is no longer produced.
"""

from ci.praktika.gh import GH
from ci.praktika.info import Info
from ci.praktika.result import Result

STATUS_NAME = "Report messages"


def check():
    try:
        info = Info()
        workflow_result = Result.from_fs(info.workflow_name)
        ext = workflow_result.ext

        errors = ext.get("errors", [])
        warnings = ext.get("warnings", [])

        for item in errors:
            print(f"ERROR: {item.get('message', '')} (from: {item.get('from', '')})")
        for item in warnings:
            print(f"WARNING: {item.get('message', '')} (from: {item.get('from', '')})")

        if errors or warnings:
            parts = []
            if errors:
                parts.append(f"{len(errors)} error(s)")
            if warnings:
                parts.append(f"{len(warnings)} warning(s)")
            description = ", ".join(parts)
            GH.post_commit_status(
                name=STATUS_NAME, status=Result.Status.FAIL, description=description, url=""
            )
    except Exception as e:
        print(f"WARNING: check_report_messages failed: {e}")


if __name__ == "__main__":
    check()
