"""
Workflow hook that posts an informational GH commit status summarising the
errors and warnings from the workflow-level report (``Result.ext["errors"]`` /
``Result.ext["warnings"]`` on the top-level workflow result).

The status is always posted as success (green) and the hook always exits 0:
it is purely informational and must never show up as red or block the merge.
Its description carries the error/warning counts and it links to the report
page, where the messages are listed in the notification panels at the top.
When there are no messages, no status is posted.
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
            # Link the status to the workflow report page, where the error and
            # warning messages are listed in the notification panels at the top.
            try:
                url = info.get_report_url()
            except Exception as e:
                print(f"WARNING: failed to build report url: {e}")
                url = ""
            # Always green: this status is informational and must not block the
            # merge or show up as red, even when errors/warnings are present.
            GH.post_commit_status(
                name=STATUS_NAME,
                status=Result.Status.OK,
                description=description,
                url=url,
            )
    except Exception as e:
        print(f"WARNING: check_report_messages failed: {e}")


if __name__ == "__main__":
    check()
