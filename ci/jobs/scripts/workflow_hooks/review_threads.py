"""Unresolved-review-threads CI gate (https://github.com/ClickHouse/ClickHouse/issues/114724).

When a PR has unresolved review threads, the full test suite is a waste: the
code is going to change again once the review comments are addressed. This
module implements the gate in three places:

- As a `Config Workflow` pre-hook (`__main__` here), it stores the number of
  unresolved review threads and whether the `ignore-unresolved-threads` label
  is set in the workflow kv data, and posts the `Review Threads` commit status
  when the pipeline is going to be limited.
- `should_skip_job` in `filter_job.py` reads that kv data and skips everything
  but builds and the preliminary checks (style check, fast test) while threads
  are unresolved.
- `can_be_merged.py` re-checks the live thread state at finish time and turns
  the `Mergeable Check` status red while threads are unresolved or while only
  the limited pipeline ran; it also posts the final `Review Threads` status.

The `Review Threads` commit status doubles as the marker that
`.github/workflows/rerun_on_review_threads.yml` reads to decide whether the
recorded verdict disagrees with the live thread state and the last `PR`
workflow run must be re-run (previously succeeded jobs are restored from the
CI cache, so only the previously skipped jobs actually run).
"""

import json
import sys

from ci.jobs.scripts.workflow_hooks.pr_labels_and_category import Labels
from ci.praktika.gh import GH
from ci.praktika.info import Info
from ci.praktika.result import Result

# Also hard-coded in .github/workflows/rerun_on_review_threads.yml and
# .github/workflows/retry_infra_failures.yml - keep them in sync.
REVIEW_THREADS_STATUS_NAME = "Review Threads"

KV_UNRESOLVED_COUNT = "unresolved_review_threads"
KV_OVERRIDE = "unresolved_review_threads_override"
KV_FORCE_ALL = "unresolved_review_threads_force_all"
# This is written by the workflow filter after it has made the actual
# limited/full decision. The config pre-hook cannot determine this by itself:
# `ci-force-all` bypasses workflow filters altogether.
KV_PIPELINE_LIMITED = "unresolved_review_threads_pipeline_limited"

# Printed by `can_be_merged.py` only when the gate actually concluded "blocked
# by unresolved review threads", so that `ci/praktika/native_jobs.py` can tell
# that verdict apart from an infrastructure failure inside the same hook (the
# thread and label queries and the commit-status write all raise). Only the
# policy verdict may be rewritten into the `Failed: review threads only`
# aggregate status that the reconciliation workflow is allowed to clear.
# Also hard-coded in `ci/praktika/native_jobs.py` - keep them in sync.
POLICY_FAILURE_MARKER = "REVIEW_THREADS_GATE: blocked by unresolved review threads"


def get_unresolved_review_threads_count(pr=None, repo=None) -> int:
    """The number of unresolved review threads on the PR. Raises on API failure."""
    threads = GH.list_pr_review_threads(pr=pr, repo=repo)
    return sum(1 for thread in threads if not thread["isResolved"])


def review_threads_gate_bypassed(labels) -> bool:
    """Whether a label bypasses the unresolved-review-threads gate."""
    return Labels.IGNORE_UNRESOLVED_THREADS in labels


def should_limit_pipeline(unresolved_count, bypassed) -> bool:
    return unresolved_count > 0 and not bypassed


def merge_gate_verdict(config_limited, unresolved_now, override_now):
    """The final per-run verdict of the gate, computed at finish time.

    :param config_limited: the pipeline ran in limited mode (decided at config time)
    :param unresolved_now: the number of unresolved review threads right now
    :param override_now: the `ignore-unresolved-threads` label is set right now
    :return: (blocked, description); `description` must fit a GH commit status
             (`GH.post_commit_status` truncates it to 80 characters).
    """
    if config_limited:
        # The full test suite did not run, so the PR must not be merged even if
        # the threads were resolved (or the label was added) while it was
        # running: a re-run is needed, and rerun_on_review_threads.yml
        # triggers it when it sees this verdict disagree with the live state.
        if unresolved_now == 0 or override_now:
            return True, "resolved during the run - the full CI suite will be re-run"
        return (
            True,
            f"{unresolved_now} unresolved review thread(s) - the full CI suite was skipped",
        )
    if override_now:
        return False, "review threads gate bypassed by CI label"
    if unresolved_now > 0:
        return True, f"{unresolved_now} unresolved review thread(s)"
    return False, "all review threads resolved"


def fetch_live_labels(info):
    """The live label set of the PR. Raises on API failure.

    The labels are fetched fresh instead of using `info.pr_labels`: the event
    payload is stale on job re-runs (see the same pattern in `trusted.py`), and
    re-running CI after adding `ignore-unresolved-threads` is exactly how the
    label is meant to be used. The fetch is strict on purpose - a helper like
    `GH.get_pr_title_body_labels` degrades an API failure into an empty label
    list, which would silently drop the override.
    """
    output = GH.get_output_with_retries(
        f"gh pr view {info.pr_number} --repo {info.repo_name} --json labels",
        verbose=True,
        strict=True,
    )
    return [label["name"] for label in json.loads(output)["labels"]]


def fetch_thread_state(info):
    """(unresolved_count, override, force_all) from live GitHub state."""
    labels = fetch_live_labels(info)
    override = review_threads_gate_bypassed(labels)
    force_all = Labels.CI_FORCE_ALL in labels
    unresolved_count = get_unresolved_review_threads_count(
        pr=info.pr_number, repo=info.repo_name
    )
    return unresolved_count, override, force_all


def record_limited_pipeline_status(info, unresolved_count):
    """Record the marker needed to replay a limited pipeline.

    Without this status, the reconciliation workflow cannot distinguish a
    limited pipeline from a full one after the threads are resolved. Returning
    false lets the config hook avoid the filters, so the full suite runs.
    """
    return GH.post_commit_status(
        name=REVIEW_THREADS_STATUS_NAME,
        status=Result.Status.FAIL,
        description=f"{unresolved_count} unresolved review thread(s) - running the limited CI suite",
        url=info.get_report_url(),
    )


def store_gate_state(info):
    """Record the gate state for the rest of the config run.

    The live label state is fetched and stored *before* the (independent)
    unresolved-thread count: `ci-force-all` must reach the fallbacks in
    `ci/praktika/native_jobs.py` even when the thread query fails, otherwise
    they consult the stale event payload and can keep the workflow filter
    hooks, the changed-file filtering and the CI cache lookup enabled on a
    re-run that was meant to bypass all of them.
    """
    # No graceful degradation when the live labels cannot be fetched (the
    # fetch already retries): there is no safe default. Falling back to the
    # event payload keeps labels that are stale on re-runs, so a rerun that
    # removed `do not test` and added `ci-force-all` would still skip most
    # jobs and could finish green without ever running the full suite; and
    # any invented value either lets stale narrowing labels filter the run
    # (unset / `False`) or impersonates `ci-force-all` and *widens* it past
    # a normal full PR run (`True`: opt-in jobs such as `Build Toolchain
    # (PGO, BOLT)`, ignored `do not test` / `ci-build`). Fail the config run
    # instead and let a re-run retry the fetch.
    labels = fetch_live_labels(info)
    override = review_threads_gate_bypassed(labels)
    force_all = Labels.CI_FORCE_ALL in labels
    # All later config decisions must use this live value. `info.pr_labels`
    # comes from the original workflow event and is stale on GitHub reruns.
    info.store_kv_data(KV_FORCE_ALL, force_all)
    info.store_kv_data(KV_OVERRIDE, override)
    # Publish the same live set as *the* label state of this run, so that every
    # other label consumer agrees with the two values above. The earlier
    # refresh in `ci/praktika/native_jobs.py` uses the non-strict
    # `GH.get_pr_title_body_labels`, which degrades an API failure into no
    # refresh at all; a re-run could then take `ci-force-all` /
    # `ignore-unresolved-threads` from here while `filter_job.py` still narrows
    # the run with a `do not test` that has since been removed, and finish
    # green without ever running the full suite.
    if list(info.pr_labels) != labels:
        print(f"NOTE: refreshing stale PR labels {list(info.pr_labels)} -> {labels}")
        info.set_pr_labels(labels, reset=True)

    try:
        unresolved_count = get_unresolved_review_threads_count(
            pr=info.pr_number, repo=info.repo_name
        )
    except Exception as e:
        # Fail toward more testing: without the thread state the full suite
        # runs as before. The merge gate in can_be_merged.py re-checks at
        # finish time and fails close there.
        print(
            f"WARNING: failed to fetch the review threads state [{e}] - the full CI suite will run"
        )
        return

    print(
        f"Unresolved review threads: {unresolved_count}, "
        f"'{Labels.IGNORE_UNRESOLVED_THREADS}' label: {override}, "
        f"'{Labels.CI_FORCE_ALL}' label: {force_all}"
    )

    if should_limit_pipeline(unresolved_count, override) and not force_all:
        # Immediate feedback on the PR; can_be_merged.py posts the final
        # verdict at finish time.
        if not record_limited_pipeline_status(info, unresolved_count):
            print(
                "WARNING: failed to post the review-threads marker - "
                "the full CI suite will run"
            )
            return

    info.store_kv_data(KV_UNRESOLVED_COUNT, unresolved_count)


if __name__ == "__main__":
    info = Info()
    if info.pr_number <= 0:
        print("Not a pull request run - skipping the review threads check")
        sys.exit(0)

    store_gate_state(info)
