"""Scheduling and caching invariant of the `Build profile diff` job.

The job compares the PR's `arm_release` build profile against master and posts a PR
comment. It is only useful when the profiled build can produce data for this head, and
its result must never be reused from cache (the comment names one concrete commit).

Those are two separate properties and praktika expresses them with two separate fields:
`digest_config` selects the job from the changed files, `enable_cache` allows result
reuse. The tests below pin both, plus the fact that adding `enable_cache` to
`Job.Config` leaves every existing job's digest untouched.

Everything is asserted against the production functions (`_filter_unaffected_jobs`,
`Digest.calc_job_digest`, `CacheRunnerHooks.post_run`) on the real PR workflow, so a
change to the job's declaration is caught here rather than in CI.
"""

import io
import os
import sys
from contextlib import redirect_stdout
from dataclasses import replace

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))
# `ci/defs/*.py` and `ci/workflows/*.py` do `from praktika import ...` rather than
# `from ci.praktika import ...`, so `ci/` itself must be on the path as well.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pytest

from ci.defs.defs import JobNames
from ci.praktika.digest import Digest
from ci.praktika.docker import Docker
from ci.praktika.hook_cache import CacheRunnerHooks
from ci.praktika.job import Job
from ci.praktika.native_jobs import _filter_unaffected_jobs
from ci.praktika.runtime import RunConfig

BPD = JobNames.BUILD_PROFILE_DIFF
PROFILED_BUILD = "Build (arm_release)"


def _pr_workflow():
    from ci.workflows.pull_request import workflow

    return workflow


def _make_run_config():
    return RunConfig(
        name="Test",
        digest_jobs={},
        digest_dockers={},
        cache_success=[],
        cache_success_base64=[],
        cache_artifacts={},
        cache_jobs={},
        filtered_jobs={},
        sha="",
        submodule_cache_hash="",
        custom_data={},
    )


def _schedule(changed_files):
    """Return the set of job names the PR workflow would skip for `changed_files`.

    Mirrors `check_affected_jobs` in ci/praktika/native_jobs.py, including the
    `find_affected_docker_images` argument: without it a change to a Dockerfile looks
    unaffecting, which would make the inherited-carrier assertions below pass for the
    wrong reason.
    """
    workflow = _pr_workflow()
    workflow_config = _make_run_config()
    with redirect_stdout(io.StringIO()):
        affected_dockers = Docker.find_affected_docker_images(
            workflow.dockers, changed_files
        )
        _filter_unaffected_jobs(
            workflow.jobs, workflow_config, changed_files, affected_dockers
        )
    return set(workflow_config.filtered_jobs)


def _job(name):
    for job in _pr_workflow().jobs:
        if job.name == name:
            return job
    raise AssertionError(f"job [{name}] not in the PR workflow")


# Diffs that cannot change the profiled build's output. The job has nothing to compare,
# so scheduling it consumes a runner for a comparison that cannot exist.
BUILD_FREE_DIFFS = [
    pytest.param(["utils/exclude-authors.txt"], id="utils-text-file"),
    pytest.param(["docs/en/development/continuous-integration.md"], id="docs-only"),
    pytest.param(["tests/queries/0_stateless/0001_x.sql"], id="stateless-test-only"),
    pytest.param([".github/workflows/pull_request.yml"], id="generated-yaml"),
    pytest.param(["ci/defs/job_configs.py"], id="job-registry"),
    pytest.param(["ci/defs/defs.py"], id="defs-registry"),
    pytest.param(["ci/jobs/scripts/workflow_hooks/filter_job.py"], id="filter-hook"),
]

# The job's own scripts: it runs them, so a change to either must schedule it even when
# the build itself is unaffected.
BUILD_AFFECTING_DIFFS_OWN_PIPELINE = [
    pytest.param(["ci/jobs/build_profile_diff_job.py"], id="own-job-body"),
    pytest.param(["ci/jobs/scripts/log_cluster.py"], id="own-log-cluster"),
]

# Diffs that can change the profiled build's output, or the diff job's own scripts.
BUILD_AFFECTING_DIFFS = [
    pytest.param(["src/Core/Settings.cpp"], id="server-source"),
] + BUILD_AFFECTING_DIFFS_OWN_PIPELINE

# Diffs that reach the job only through `requires=["Build (arm_release)"]`: none of them
# is in the job's own digest, so these are the assertions that prove the inheritance is
# what schedules the job, rather than a copied path list.
INHERITED_CARRIER_DIFFS = [
    pytest.param(["ci/docker/binary-builder/Dockerfile"], id="builder-docker"),
    pytest.param(["ci/docker/fasttest/Dockerfile"], id="transitive-docker"),
    pytest.param([".gitmodules"], id="submodule-list"),
    pytest.param(["contrib/zstd"], id="submodule"),
    pytest.param(["packages/clickhouse-common-static.yaml"], id="packaging"),
    pytest.param(
        ["utils/prepare-time-trace/prepare-time-trace.sh"], id="time-trace-producer"
    ),
    pytest.param(
        ["ci/jobs/scripts/job_hooks/build_profile_hook.py"], id="profile-uploader"
    ),
]


@pytest.mark.parametrize("changed_files", BUILD_FREE_DIFFS)
def test_not_scheduled_when_no_build_is_affected(changed_files):
    assert BPD in _schedule(changed_files)


@pytest.mark.parametrize(
    "changed_files", BUILD_AFFECTING_DIFFS + INHERITED_CARRIER_DIFFS
)
def test_scheduled_when_the_build_or_its_own_scripts_change(changed_files):
    assert BPD not in _schedule(changed_files)


@pytest.mark.parametrize("changed_files", BUILD_AFFECTING_DIFFS_OWN_PIPELINE)
def test_own_pipeline_files_are_matched_by_the_job_s_own_digest(changed_files):
    """Asserted on the job's own digest, not only on the schedule.

    `log_cluster.py` is also in the build's digest, so a change to it schedules the job
    through `requires` as well. Removing it from this job's `include_paths` would
    therefore leave the scheduling assertion above passing while the job stops tracking
    a script it runs, which is what this pins.
    """
    assert _job(BPD).is_affected_by(changed_files)


@pytest.mark.parametrize("changed_files", INHERITED_CARRIER_DIFFS)
def test_inherited_carriers_are_not_listed_on_the_job_itself(changed_files):
    """The carrier must arrive through `requires`, not through a copied path.

    Asserted as "the job's own digest does not match this file, yet the job runs", so a
    future attempt to fix a missed carrier by pasting build paths onto the consumer
    fails here instead of silently forking from the build's own digest policy.
    """
    assert not _job(BPD).is_affected_by(changed_files)
    assert BPD not in _schedule(changed_files)


def test_registry_edits_do_not_schedule_a_run_without_profile_data():
    """A change to the job registry does not affect the build, so the build stays
    cache-reusable and uploads no profile for this head. Scheduling the diff job then
    reproduces the waste this filtering exists to prevent.

    Phrased as "these paths do not schedule the job" rather than as a literal copy of
    the digest list, so adding a genuine consumer input later does not fail this test.
    """
    for path in ("ci/defs/job_configs.py", "ci/defs/defs.py"):
        skipped = _schedule([path])
        assert PROFILED_BUILD in skipped, path
        assert BPD in skipped, path


def test_the_job_result_is_never_reused_from_cache():
    assert _job(BPD).enable_cache is False


def test_no_other_job_opts_out_of_cache():
    """`enable_cache` defaults to True, so this pins that the new field changes the
    behaviour of exactly one job.
    """
    opted_out = [j.name for j in _pr_workflow().jobs if not j.enable_cache]
    assert opted_out == [BPD]


def test_a_cache_opted_out_job_is_not_looked_up(monkeypatch):
    """Seam 1: a job with `enable_cache=False` must not reach the cache lookup.

    `cache_success_base64` is what makes the generated `if:` skip the runner, and it is
    populated only from the records fetched for `eligible_jobs`, so being absent from
    that set is what guarantees the job runs.
    """
    workflow = _pr_workflow()
    fetched = []

    class _FakeCache:
        class digest:
            @staticmethod
            def get_null_digest():
                return "f" * 20

            @staticmethod
            def calc_job_digest(job_config, docker_digests, artifact_configs):
                return "d" * 20

        def fetch_success(self, job_name, job_digest):
            fetched.append(job_name)
            return None

    monkeypatch.setattr("ci.praktika.hook_cache.Cache", _FakeCache)
    monkeypatch.setattr(
        RunConfig, "from_fs", classmethod(lambda cls, name: _make_run_config())
    )
    monkeypatch.setattr(RunConfig, "dump", lambda self: None)

    with redirect_stdout(io.StringIO()):
        CacheRunnerHooks.configure(workflow)

    assert fetched, "no job was looked up at all - the seam is not being exercised"
    assert BPD not in fetched


def test_a_cache_opted_out_job_pushes_no_success_record(monkeypatch):
    """Seam 2: a successful run of the job must not publish a cache record.

    A published record would be read by a later run of an unchanged head and skip the
    job, and with it the only writer of the `build-profile-diff` comment.
    """
    pushed = []
    monkeypatch.setattr(
        "ci.praktika.hook_cache.Cache.push_success_record",
        staticmethod(lambda *a, **kw: pushed.append(a[0])),
    )
    monkeypatch.setattr(
        RunConfig,
        "from_workflow_data",
        classmethod(
            lambda cls: replace(
                _make_run_config(),
                digest_jobs={BPD: "d" * 20, PROFILED_BUILD: "e" * 20},
            )
        ),
    )
    workflow = _pr_workflow()

    with redirect_stdout(io.StringIO()):
        CacheRunnerHooks.post_run(workflow, _job(BPD))
        CacheRunnerHooks.post_run(workflow, _job(PROFILED_BUILD))

    # The control arm: an ordinary cacheable job with the same digest shape does push,
    # so an assertion that nothing was pushed cannot pass by accident.
    assert pushed == [PROFILED_BUILD]


def _config_digest(job_config):
    with redirect_stdout(io.StringIO()):
        digest = Digest().calc_job_digest(
            job_config=job_config,
            docker_digests={d.name: "0" * 20 for d in _pr_workflow().dockers},
            artifact_configs={a.name: a for a in _pr_workflow().artifacts},
        )
    return digest


def test_enable_cache_does_not_enter_the_job_digest():
    """`enable_cache` governs result reuse, not job output, so it must be in
    `drop_fields`. Without that entry every job's digest changes and the whole CI cache
    is invalidated on the first push - which presents as an unrelated mass rebuild
    rather than as a bug in the job declaration.
    """
    job = Job.Config(
        name="digest probe",
        runs_on=[],
        command="true",
        digest_config=Job.CacheDigestConfig(include_paths=["./ci/praktika/job.py"]),
    )
    assert _config_digest(replace(job, enable_cache=True)) == _config_digest(
        replace(job, enable_cache=False)
    )


def test_an_output_affecting_field_still_enters_the_job_digest():
    """Control for the test above: `drop_fields` must not be so broad that a field
    which does change what the job produces stops re-keying it.
    """
    job = Job.Config(
        name="digest probe",
        runs_on=[],
        command="true",
        digest_config=Job.CacheDigestConfig(include_paths=["./ci/praktika/job.py"]),
    )
    assert _config_digest(job) != _config_digest(replace(job, command="false"))
