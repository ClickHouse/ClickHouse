import argparse
import sys

from ci.jobs.scripts.workflow_hooks.pr_labels_and_category import Labels
from ci.jobs.scripts.workflow_hooks.review_threads import (
    KV_PIPELINE_LIMITED,
    POLICY_FAILURE_MARKER,
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


def check_review_threads():
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
        # Emitted only here, after the verdict was computed and posted: an
        # infrastructure failure above raises instead, and `native_jobs.py`
        # must not mistake it for this policy verdict.
        print(POLICY_FAILURE_MARKER)
        return False

    return True


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--review-threads", action="store_true")
    args = parser.parse_args()

    # Keep these as separate post-hooks. `Finish Workflow` aggregates its
    # post-hooks, and the reconciliation workflow must know whether a review
    # thread was the sole failed hook instead of conflating it with `check`.
    ok = check_review_threads() if args.review_threads else check()
    if not ok:
        sys.exit(1)
