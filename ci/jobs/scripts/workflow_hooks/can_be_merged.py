import sys

from ci.jobs.scripts.workflow_hooks.pr_labels_and_category import Labels
from ci.jobs.scripts.workflow_hooks.review_threads import (
    KV_PIPELINE_LIMITED,
    REVIEW_THREADS_STATUS_NAME,
    fetch_thread_state,
    merge_gate_verdict,
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


def check_review_threads(other_merge_gate_blocked=False):
    """The unresolved-review-threads merge gate
    (https://github.com/ClickHouse/ClickHouse/issues/114724).

    Blocks the merge while the PR has unresolved review threads, and also when
    the threads were resolved only after the pipeline had already been limited
    at config time (the full test suite did not run, so a re-run is required).
    `rerun_on_review_threads.yml` detects this when threads resolve while the
    PR workflow is still running; otherwise CI must be re-run manually. Posts
    the final `Review Threads` commit status, which that workflow reads as the
    recorded verdict. Fails close: an API failure propagates, failing the
    post-hook and thereby the Mergeable Check.
    """
    info = Info()
    if info.pr_number <= 0:
        return True

    config_limited = bool(info.get_kv_data(KV_PIPELINE_LIMITED))
    unresolved_now, override_now, _ = fetch_thread_state(info)
    blocked, description = merge_gate_verdict(
        config_limited, unresolved_now, override_now
    )
    if blocked and other_merge_gate_blocked:
        # The rerun workflow clears the aggregate `Mergeable Check` only when
        # this status proves the review-thread hook was its sole blocker.
        # Keep this distinct from the count-only marker even if the other
        # gate's failure has the same aggregate `Finish Workflow` job.
        description = "review threads and another merge gate blocked"
    print(
        f"Review threads gate: config_limited [{config_limited}], unresolved now "
        f"[{unresolved_now}], override [{override_now}] -> blocked [{blocked}] ({description})"
    )

    status = Result.Status.FAIL if blocked else Result.Status.OK
    if not GH.post_commit_status(
        name=REVIEW_THREADS_STATUS_NAME,
        status=status,
        description=description,
        url=info.get_report_url(),
    ):
        raise RuntimeError("Failed to post the `Review Threads` commit status")

    if blocked:
        print("WARNING: unresolved review threads, merge not allowed")
        return False

    return True


if __name__ == "__main__":
    # Run both checks unconditionally: check_review_threads also refreshes the
    # `Review Threads` commit status, which must stay in sync with the live
    # thread state even when a forbidden label already blocks the merge.
    other_merge_gate_blocked = not check()
    ok = check_review_threads(other_merge_gate_blocked) and not other_merge_gate_blocked
    if not ok:
        sys.exit(1)
