"""Tests for the `pipeline_status` GH Actions output and the non-blocking flag.

`pipeline_status` is the only signal GitHub has for whether a finished job
blocks the rest of the pipeline: every dependent job's generated `if:` is
`!contains(needs.*.outputs.pipeline_status, 'failure')`
(`yaml_generator.py`), and `needs` is made transitive, so one `failure` token
skips a job's whole downstream closure.

Two consumers read `do_not_block_pipeline_on_failure` and must reach the same
verdict for the same result: `Runner._pipeline_status` decides what GitHub
sees, and `HtmlRunnerHooks.post_run` decides whether dependees are marked
`DROPPED` in the report. When they disagree, a job is non-blocking in the
report and blocking in GitHub - a job whose only non-ok child is a synthetic
harness row then skips ~110 downstream jobs, including every
`Bugfix validation (*)`, which `new_tests_check.py` reports back to the author
as "the test either passes on master HEAD on every arch (so it's not actually
a regression test for the fix) or every arch errored out".

Both integration and functional test jobs pass `force_ok_exit` into
`complete_job(do_not_block_pipeline_on_failure=...)` after a path that can
leave the job-level result `ERROR` rather than `FAIL`, so the exemption has to
hold for every non-ok status.
"""

import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from ci.praktika.result import Result
from ci.praktika.runner import Runner


def _result(status, non_blocking):
    """A finished job result, as `complete_job` leaves it.

    `Result.complete_job` records the flag only when the result is not ok
    (`result.py`), which is mirrored here so a test can never assert on a
    combination the production setter cannot produce.
    """
    r = Result(name="Integration tests (amd_asan_ubsan, db disk, old analyzer, 1/6)", status=status)
    if non_blocking and not r.is_ok():
        r.ext["do_not_block_pipeline_on_failure"] = True
    return r


# Every non-ok status a job-level result can carry. DROPPED is included even
# though it is only set on a job that never ran: if such a result ever reached
# the output, "did not run" must not be laundered into a green pipeline unless
# the job asked for it.
NON_OK_STATUSES = [
    Result.Status.FAIL,
    Result.Status.ERROR,
    Result.Status.XPASS,
    Result.Status.DROPPED,
    Result.Status.UNKNOWN,
    Result.Status.PENDING,
    Result.Status.RUNNING,
]

OK_STATUSES = [Result.Status.OK, Result.Status.SKIPPED, Result.Status.XFAIL]


@pytest.mark.parametrize("status", NON_OK_STATUSES)
def test_non_blocking_job_does_not_block_the_pipeline_for_any_non_ok_status(status):
    """The flag exempts every non-ok status, not only `FAIL`.

    A session-timeout leaves the integration job `ERROR` (a child `Timeout`
    row rolls up to `FAIL`, then the job sets `ERROR` for the error flag) while
    `force_ok_exit` is True, so an exemption restricted to `FAIL` never applies
    to the case that actually occurs.
    """
    assert Runner._pipeline_status(_result(status, non_blocking=True)) == "success"


@pytest.mark.parametrize("status", NON_OK_STATUSES)
def test_a_job_that_did_not_ask_to_be_non_blocking_still_blocks(status):
    """Without the flag every non-ok status blocks the downstream closure.

    This is the half that must not move: a real failure has to keep skipping
    dependent jobs, so an exemption that ignores the flag is not a fix.
    """
    assert Runner._pipeline_status(_result(status, non_blocking=False)) == "failure"


@pytest.mark.parametrize("status", OK_STATUSES)
@pytest.mark.parametrize("non_blocking", [True, False])
def test_ok_statuses_never_block(status, non_blocking):
    assert Runner._pipeline_status(_result(status, non_blocking)) == "success"


def _html_hook_drops_dependees(result):
    """The dependee-drop predicate from `HtmlRunnerHooks.post_run`.

    Kept in sync by `test_both_consumers_read_the_flag_with_the_same_predicate`,
    which asserts the source of the production site rather than trusting this
    copy.
    """
    return not result.is_ok() and not result.do_not_block_pipeline_on_failure()


@pytest.mark.parametrize("status", NON_OK_STATUSES + OK_STATUSES)
@pytest.mark.parametrize("non_blocking", [True, False])
def test_the_two_consumers_of_the_flag_agree(status, non_blocking):
    """GitHub and the report must not disagree about whether a job blocks.

    Anything else is visible to an author as a red `Finish Workflow` with no
    failing test and a report that shows nothing dropped.
    """
    result = _result(status, non_blocking)
    blocks_in_github = Runner._pipeline_status(result) == "failure"
    assert blocks_in_github == _html_hook_drops_dependees(result)


def test_both_consumers_read_the_flag_with_the_same_predicate():
    """Pin the `hook_html` predicate to the copy asserted above.

    `test_the_two_consumers_of_the_flag_agree` compares against a local copy of
    the report-side predicate, which would keep passing if the production one
    were changed. Assert the production source too, so the pair cannot drift
    apart silently.
    """
    import inspect

    from ci.praktika.hook_html import HtmlRunnerHooks

    source = inspect.getsource(HtmlRunnerHooks.post_run)
    assert (
        "not result.is_ok() and not result.do_not_block_pipeline_on_failure()" in source
    ), "hook_html dependee-drop predicate changed - re-check Runner._pipeline_status"


def test_the_output_site_delegates_to_the_helper():
    """The emitted `pipeline_status` must be the helper's return value.

    Every other test here calls `Runner._pipeline_status` directly, so a call
    site that computed its own status would leave them all green while GitHub
    went back to seeing `failure` for a non-blocking `ERROR`. The predicate is
    only observable through the line that writes this output, so pin the whole
    hop: the helper is called, its result is what gets written, and no verdict
    is hardcoded anywhere in between.
    """
    import inspect

    source = inspect.getsource(Runner._post_run)
    assert "pipeline_status = self._pipeline_status(result)" in source
    assert "is_failure()" not in source, (
        "the output site must not re-derive the status - call _pipeline_status"
    )
    assert 'f"pipeline_status={pipeline_status}"' in source, (
        "the emitted value must be the helper's result, not a literal"
    )
    for literal in (
        '"pipeline_status=failure"',
        '"pipeline_status=success"',
        'pipeline_status = "failure"',
        'pipeline_status = "success"',
    ):
        assert literal not in source, f"hardcoded pipeline verdict: {literal}"


def test_the_merge_gate_never_reads_the_non_blocking_flag():
    """Merge readiness must stay independent of the flag.

    A non-blocking job is exempted from downstream dispatch only. Were the
    merge computation to honour the same flag, a job could go green for merging
    on a status it merely asked not to propagate.
    """
    import inspect

    from ci.praktika import native_jobs

    source = inspect.getsource(native_jobs._finish_workflow)
    assert "do_not_block_pipeline_on_failure" not in source
    assert "allow_failure" in source


def test_the_reported_shard_shape_does_not_block_the_pipeline():
    """End to end on the measured production result.

    903 `OK` + 17 `SKIPPED` + 1 `FAIL` named `Timeout` is the published row set
    of `Integration tests (amd_asan_ubsan, db disk, old analyzer, 1/6)` on
    PR #107115 at `937dffffc320`: every real test passed and the only non-ok
    child is the synthetic session-timeout row.
    """
    children = [Result(name=f"test_ok_{i}", status=Result.Status.OK) for i in range(903)]
    children += [
        Result(name=f"test_skipped_{i}", status=Result.Status.SKIPPED) for i in range(17)
    ]
    children.append(
        Result(
            name="Timeout",
            status=Result.Status.FAIL,
            info="ERROR: session-timeout occurred during test execution",
        )
    )

    # integration_test_job.py: a single non-ok test means "do not block pipeline"
    failures_cnt = len([r for r in children if not r.is_ok()])
    assert failures_cnt == 1
    force_ok_exit = 0 < failures_cnt < 2
    assert force_ok_exit

    result = Result.create_from(name="Integration tests", results=children)
    assert result.status == Result.Status.FAIL
    # integration_test_job.py: the error flag set for a session-timeout in a
    # non-targeted, non-flaky check makes the job-level status ERROR
    result.set_error()
    assert result.status == Result.Status.ERROR
    assert not result.is_failure(), "an ERROR is outside the is_failure() domain"

    if force_ok_exit and not result.is_ok():
        result.ext["do_not_block_pipeline_on_failure"] = True

    assert Runner._pipeline_status(result) == "success"
    assert not _html_hook_drops_dependees(result)


def test_the_old_analyzer_shards_still_block_the_merge():
    """The merge gate must stay independent of this flag.

    `ready_for_merge` is computed from `job.allow_failure` alone
    (`native_jobs.py`) and never reads `do_not_block_pipeline_on_failure`, so a
    genuinely failing shard still blocks the merge. Assert the shards are
    `allow_failure=False`: were they flipped, a non-blocking status would be
    the only thing left holding the gate.
    """
    from ci.defs.job_configs import JobConfigs

    shards = [
        job
        for job in JobConfigs.integration_test_jobs_required
        if "_asan_ubsan, db disk, old analyzer" in job.name
    ]
    assert shards, "old-analyzer integration shards not found"
    assert all(job.allow_failure is False for job in shards)
