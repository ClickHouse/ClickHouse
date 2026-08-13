import sys

from ci.jobs.scripts.workflow_hooks.pr_labels_and_category import Labels
from ci.jobs.scripts.workflow_hooks.review_threads import (
    KV_OVERRIDE,
    KV_UNRESOLVED_COUNT,
    REVIEW_THREADS_STATUS_NAME,
    fetch_thread_state,
    merge_gate_verdict,
    should_limit_pipeline,
)
from ci.praktika.gh import GH
from ci.praktika.info import Info
from ci.praktika.result import Result


def check():
    info = Info()
    forbidden_labels = [
        Labels.CI_PERFORMANCE,
        Labels.CI_INTEGRATION_FLAKY,
        Labels.CI_FUNCTIONAL_FLAKY,
        Labels.CI_INTEGRATION,
        Labels.CI_FUNCTIONAL,
        Labels.CI_BUILD,
    ]

    for label in forbidden_labels:
        if label in info.pr_labels:
            print(f"WARNING: {label} label is set, merge not allowed")
            return False

    return True


def check_review_threads():
    """The unresolved-review-threads merge gate
    (https://github.com/ClickHouse/ClickHouse/issues/114724).

    Blocks the merge while the PR has unresolved review threads, and also when
    the threads were resolved only after the pipeline had already been limited
    at config time (the full test suite did not run, so a re-run is required -
    rerun_on_review_threads.yml triggers it automatically). Posts the final
    `Review Threads` commit status, which that workflow reads as the recorded
    verdict. Fails close: an API failure propagates, failing the post-hook and
    thereby the Mergeable Check.
    """
    info = Info()
    if info.pr_number <= 0:
        return True

    config_limited = should_limit_pipeline(
        info.get_kv_data(KV_UNRESOLVED_COUNT) or 0,
        bool(info.get_kv_data(KV_OVERRIDE)),
    )
    unresolved_now, override_now = fetch_thread_state(info)
    blocked, description = merge_gate_verdict(
        config_limited, unresolved_now, override_now
    )
    print(
        f"Review threads gate: config_limited [{config_limited}], unresolved now "
        f"[{unresolved_now}], override [{override_now}] -> blocked [{blocked}] ({description})"
    )

    if blocked:
        GH.post_commit_status(
            name=REVIEW_THREADS_STATUS_NAME,
            status=Result.Status.FAIL,
            description=description,
            url=info.get_report_url(),
        )
        print("WARNING: unresolved review threads, merge not allowed")
        return False

    # Post a success status only to clear a previously posted failure, so PRs
    # the gate never limited do not grow an extra status line.
    statuses = GH.get_commit_statuses()
    previous = (statuses or {}).get(REVIEW_THREADS_STATUS_NAME)
    if previous and previous.state != "success":
        GH.post_commit_status(
            name=REVIEW_THREADS_STATUS_NAME,
            status=Result.Status.OK,
            description=description,
            url=info.get_report_url(),
        )
    return True


if __name__ == "__main__":
    # Run both checks unconditionally: check_review_threads also refreshes the
    # `Review Threads` commit status, which must stay in sync with the live
    # thread state even when a forbidden label already blocks the merge.
    ok = check()
    ok = check_review_threads() and ok
    if not ok:
        sys.exit(1)
