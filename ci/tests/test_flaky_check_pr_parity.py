"""
Guards that the stateless flaky check reports a bad test in PR CI rather than
in the merge queue.

Background: a PR was bounced from the merge queue by
`Stateless tests (amd_binary, flaky check)` failing with
"Test runs too long (> 180s)" while all four PR-side flaky checks
(`amd_asan_ubsan`, `amd_tsan`, `amd_msan`, `amd_debug`) were green on the very
same test. Two properties made the queue stricter than the PR:

  * the flaky check ran on a build that PR CI had no flaky check for, so a test
    that is only too slow / only flaky without a sanitizer was first seen in the
    queue;
  * the `amd_binary` job took the "plain binary job runs fast" branch and
    oversubscribed the runner (`cpu_count * 1.2`), so the same test ran with 18
    workers on a 16-vCPU runner against 8 workers for `amd_asan_ubsan` on the
    identical runner type. In flaky-check mode every worker runs the *same*
    test, so that is a 2.25x self-contention factor on the wall-clock time the
    check then judges.

Both are pinned here.
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from ci.jobs.functional_tests import allow_oversubscription

# The flaky check the merge queue runs, and which this change also runs in PR CI.
MQ_FLAKY_CHECK_JOB_NAME = "Stateless tests (amd_binary, flaky check)"


def test_plain_binary_job_still_oversubscribes():
    # The full-suite binary jobs are what the higher concurrency was meant for:
    # each worker picks a different test and most tests are light.
    assert allow_oversubscription("amd_binary, parallel", ["amd_binary", "parallel"], False, False)
    assert allow_oversubscription("arm_binary, sequential", ["arm_binary", "sequential"], False, False)


def test_binary_flaky_check_does_not_oversubscribe():
    # Every worker runs the same changed test here, and the check fails a test
    # by wall-clock time - concurrency must not decide the verdict.
    assert not allow_oversubscription(
        "amd_binary, flaky check", ["amd_binary", "flaky check"], True, False
    )


def test_binary_targeted_check_does_not_oversubscribe():
    assert not allow_oversubscription(
        "arm_binary, targeted", ["arm_binary", "targeted"], False, True
    )


def test_sanitizer_jobs_are_unaffected():
    # No "binary" in the options - these never took the branch to begin with.
    assert not allow_oversubscription(
        "amd_asan_ubsan, flaky check", ["amd_asan_ubsan", "flaky check"], True, False
    )
    assert not allow_oversubscription(
        "amd_tsan, parallel", ["amd_tsan", "parallel"], False, False
    )


def _stateless_flaky_jobs(workflow):
    return [
        job
        for job in workflow.jobs
        if "flaky" in job.name.lower() and "stateless" in job.name.lower()
    ]


def test_every_merge_queue_flaky_check_also_runs_in_pr_ci():
    # A flaky check that exists only in the merge queue can only ever report a
    # bad test once the merge is already in progress. Pin that each merge-queue
    # flaky check also runs in PR CI - as the same job config, sharing name,
    # build, and runner - so the PR sees the same configuration first (and with
    # the larger iteration count and time budget - see `is_merge_queue_event`
    # in functional_tests.py).
    from ci.workflows.merge_queue import workflow as mq_workflow
    from ci.workflows.pull_request import workflow as pr_workflow

    mq_jobs = _stateless_flaky_jobs(mq_workflow)
    pr_jobs_by_name = {job.name: job for job in _stateless_flaky_jobs(pr_workflow)}
    assert mq_jobs, "merge queue lost its stateless flaky check"
    for mq_job in mq_jobs:
        pr_job = pr_jobs_by_name.get(mq_job.name)
        assert pr_job is not None, f"{mq_job.name} runs in the merge queue but not in PR CI"
        assert pr_job.runs_on == mq_job.runs_on
        assert pr_job.requires == mq_job.requires
        assert pr_job.command == mq_job.command


def _flaky_check_cache_digest(workflow_name):
    """The praktika cache key of the shared flaky check, per workflow.

    `Digest.calc_job_digest` hashes the job config *after* per-workflow
    mangling, so the two workflows must be loaded through `_get_workflows`
    rather than imported as the raw `Workflow.Config`. The file-content half of
    the digest comes from the one `digest_config` both lanes share and is equal
    by construction; it is stubbed out here so the test does not hash
    `tests/queries` twice. Only the config half can differ, which is exactly
    what is under test.
    """
    from copy import deepcopy

    from ci.praktika.digest import Digest
    from ci.praktika.mangle import _get_workflows

    workflow = _get_workflows(name=workflow_name)[0]
    jobs = [job for job in workflow.jobs if job.name == MQ_FLAKY_CHECK_JOB_NAME]
    assert len(jobs) == 1, f"{MQ_FLAKY_CHECK_JOB_NAME} not found once in {workflow_name}"
    job = deepcopy(jobs[0])
    job.digest_config.include_paths = []
    return Digest().calc_job_digest(
        job_config=job,
        docker_digests={docker.name: "" for docker in workflow.dockers},
        artifact_configs={},
    )


def test_pr_run_does_not_cache_away_the_merge_queue_drift_guard():
    # The PR and merge-queue lanes are one job config, but they must not be one
    # cache record. `ci/praktika/cache.py` keys a record by job name and digest
    # only - no workflow name - so if the digests coincided, the PR's green run
    # would satisfy the merge-queue lookup and the queue would skip the check as
    # "reused from cache". That would silently disable the drift guard: the
    # whole point of the merge-queue run is to exercise the merge group state
    # (the PR merged with the current `master`), which the PR-side run never saw.
    #
    # They stay separate because the PR workflow mangles the config differently
    # (`runs_on_label_prefix="pr-"` and its own `run_after`), and both fields
    # feed the config half of the digest. This pins that; if praktika ever stops
    # hashing them, the merge-queue lane needs its own cache identity again.
    assert _flaky_check_cache_digest("PR") != _flaky_check_cache_digest("MergeQueueCI")
