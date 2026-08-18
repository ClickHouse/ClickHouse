"""Scheduling and caching invariant of the `Build profile diff` job.

The job compares the PR's `arm_release` build profile against master and posts a PR
comment. It is only useful when the profiled build can produce data for this head.

`digest_config` selects the job from the changed files. `requires` then folds the
profiled build's digest into this job's cache key, so both the selection and the
result reuse follow the source state: a head with different sources gets a different
key, and a head with identical sources may legitimately reuse the earlier result.
`requires` rather than `run_after` is what makes that hold, and the tests below pin it.

Everything is asserted against the production functions (`_filter_unaffected_jobs`,
`Digest.calc_job_digest`, `CacheRunnerHooks.configure`) on the real PR workflow, so a
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
from ci.praktika.cache import Cache
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


def test_the_job_requires_the_profiled_build_rather_than_running_after_it():
    """`requires` is what folds the build's digest into this job's cache key.

    With `run_after` the job would keep only its own two near-invariant script paths in
    the key, so one published record would be reused across unrelated heads.
    """
    assert _job(BPD).requires == [PROFILED_BUILD]


def _cache_keys(monkeypatch, per_job_digest):
    """Return the per-job cache keys `CacheRunnerHooks.configure` computes.

    `digest_jobs` is what `Cache.fetch_success` and `push_success_record` are keyed by,
    so this is the real key, not the job's own `calc_job_digest` value.

    `per_job_digest(job) -> str` stands in for the file hashing. Hashing for real would
    walk every job's include paths (~29k files for the stress tests) and take ~25 min,
    which does not fit the `CI Tests` budget. The composition of the keys is what is
    under test here, and it consumes those hashes opaquely; that the build's own hash
    really does track the sources is pinned separately by
    `test_the_builds_digest_tracks_the_source_tree`.
    """
    captured = {}

    def _dump(self):
        captured.update(self.digest_jobs)

    monkeypatch.setattr(
        "ci.praktika.digest.Digest.calc_job_digest",
        lambda self, job_config, docker_digests, artifact_configs: per_job_digest(
            job_config
        ),
    )
    monkeypatch.setattr(
        RunConfig, "from_fs", classmethod(lambda cls, name: _make_run_config())
    )
    monkeypatch.setattr(RunConfig, "dump", _dump)
    with redirect_stdout(io.StringIO()):
        CacheRunnerHooks.configure(_pr_workflow(), skip_lookup=True)
    assert captured, "configure() computed no digests - the seam is not exercised"
    return captured


def _fake_digests(job_config):
    """A distinct digest per job, so a prefix relationship in the composed key can only
    come from `configure()` prepending a dependency, never from two jobs colliding."""
    if job_config.name == BPD:
        return "bbbbbbbbbbbbbbbbbbbb-bbbb"
    if job_config.name == PROFILED_BUILD:
        return "aaaaaaaaaaaaaaaaaaaa-aaaa"
    return Digest().get_null_digest()


def test_the_cache_key_contains_the_profiled_builds_digest(monkeypatch):
    """The reuse invariant: a head whose sources differ gets a different key.

    Asserted structurally because the build's digest is the key's prefix; a change under
    `./src` or a submodule bump moves it, and the diff job's key moves with it.
    """
    keys = _cache_keys(monkeypatch, _fake_digests)
    assert keys[BPD].startswith(keys[PROFILED_BUILD] + "-")


def test_the_cache_key_is_not_just_the_jobs_own_scripts(monkeypatch):
    """Control for the test above.

    The job's own digest covers only two rarely-changed scripts. If that were the whole
    key, the first PR to run the job would publish a record that later PRs reuse
    regardless of their sources - the failure mode `requires` prevents.
    """
    keys = _cache_keys(monkeypatch, _fake_digests)
    own = _fake_digests(_job(BPD))
    assert keys[BPD] != own
    assert keys[BPD].endswith(own)


def _lookup(monkeypatch, per_job_digest, records):
    """Run the real cache lookup and return (keys looked up, jobs marked reusable).

    `records` maps a cache key to whether S3 holds a success record for it. Being in
    `cache_success_base64` is what makes the generated `if:` skip the runner, so that
    list is the actual reuse decision.
    """
    looked_up = {}
    captured = {}

    class _FakeCache:
        digest = Digest()

        def fetch_success(self, job_name, job_digest):
            looked_up[job_name] = job_digest
            if not records.get(job_digest):
                return None
            return Cache.CacheRecord(
                type=Cache.CacheRecord.Type.SUCCESS,
                sha="deadbeef",
                pr_number=1,
                branch="a-branch",
                workflow="PR",
            )

    monkeypatch.setattr(
        "ci.praktika.digest.Digest.calc_job_digest",
        lambda self, job_config, docker_digests, artifact_configs: per_job_digest(
            job_config
        ),
    )
    monkeypatch.setattr("ci.praktika.hook_cache.Cache", _FakeCache)
    monkeypatch.setattr(
        RunConfig, "from_fs", classmethod(lambda cls, name: _make_run_config())
    )
    monkeypatch.setattr(RunConfig, "dump", lambda self: captured.update(vars(self)))
    with redirect_stdout(io.StringIO()):
        CacheRunnerHooks.configure(_pr_workflow())
    return looked_up, captured["cache_success"]


def test_the_job_is_not_even_looked_up_when_the_build_is_not_reused(monkeypatch):
    """The lookup seam, not just the digest arithmetic.

    Because the composed key makes this job a dependent of the build, `configure` fetches
    it only after the build's own record was found. With no build record it is never
    queried, so no record of any provenance can grant it reuse.
    """
    looked_up, reusable = _lookup(monkeypatch, _fake_digests, records={})
    assert (
        PROFILED_BUILD in looked_up
    ), "the build was not looked up - seam not exercised"
    assert BPD not in looked_up
    assert BPD not in reusable


def test_a_record_for_the_jobs_own_digest_alone_does_not_grant_reuse(monkeypatch):
    """A record stored under the job's own two-script digest - what a PR at a different
    source state would leave behind if `requires` were dropped - must not be reused."""
    own = _fake_digests(_job(BPD))
    looked_up, reusable = _lookup(monkeypatch, _fake_digests, records={own: True})
    assert BPD not in reusable
    assert looked_up.get(BPD) != own


def test_the_composed_key_does_grant_reuse(monkeypatch):
    """Control for the two tests above: the lookup is wired up and does reuse a matching
    record, so their negative results are about the key rather than about a lookup that
    never reuses anything. Identical sources reusing a result is what the cache is for.
    """
    arm = _fake_digests(_job(PROFILED_BUILD))
    composed = arm + "-" + _fake_digests(_job(BPD))
    looked_up, reusable = _lookup(
        monkeypatch, _fake_digests, records={arm: True, composed: True}
    )
    assert looked_up[BPD] == composed
    assert BPD in reusable


def test_a_successful_run_publishes_a_record_under_the_composed_key(monkeypatch):
    """The publication seam. The tests above inject records; this one pins that the job
    really does publish, and under the same composed key it is looked up by.

    Without this, they would all still pass if publication stopped or wrote a different
    key, and the job would silently never be reused.
    """
    pushed = {}
    arm = _fake_digests(_job(PROFILED_BUILD))
    composed = arm + "-" + _fake_digests(_job(BPD))

    monkeypatch.setattr(
        "ci.praktika.hook_cache.Cache.push_success_record",
        staticmethod(lambda name, digest, *a, **kw: pushed.__setitem__(name, digest)),
    )
    monkeypatch.setattr(
        RunConfig,
        "from_workflow_data",
        classmethod(
            lambda cls: replace(
                _make_run_config(),
                digest_jobs={BPD: composed, PROFILED_BUILD: arm},
            )
        ),
    )
    workflow = _pr_workflow()
    with redirect_stdout(io.StringIO()):
        CacheRunnerHooks.post_run(workflow, _job(BPD))
        # An ordinary cacheable job as the control, so an assertion about the diff job's
        # record cannot pass because publication is broken for everything.
        CacheRunnerHooks.post_run(workflow, _job(PROFILED_BUILD))

    assert pushed == {BPD: composed, PROFILED_BUILD: arm}


def test_the_builds_digest_tracks_the_source_tree():
    """The other half of the reuse invariant, and what the stubbed tests above take on
    faith: the profiled build's own digest really does move when the sources move.

    Together with the composition tests, this is what makes a cache hit on the diff job
    mean "identical sources" rather than "some earlier PR already ran it".
    """
    build = _job(PROFILED_BUILD)
    paths = build.digest_config.include_paths
    assert "./src" in paths
    assert build.digest_config.with_git_submodules is True
    # And the diff job's own digest deliberately does not duplicate them.
    assert "./src" not in _job(BPD).digest_config.include_paths


def _config_digest(job_config):
    with redirect_stdout(io.StringIO()):
        digest = Digest().calc_job_digest(
            job_config=job_config,
            docker_digests={d.name: "0" * 20 for d in _pr_workflow().dockers},
            artifact_configs={a.name: a for a in _pr_workflow().artifacts},
        )
    return digest


def test_requires_does_not_enter_the_jobs_own_digest():
    """`requires` is in `drop_fields`, so it shapes the key only through the dependency
    prefix that `CacheRunnerHooks.configure` prepends, never through the job's own hash.

    Pinned because the two tests above read the key as `<dep digest>-<own digest>`; if
    `requires` also re-keyed the own half, that decomposition would silently stop holding.
    """
    job = Job.Config(
        name="digest probe",
        runs_on=[],
        command="true",
        digest_config=Job.CacheDigestConfig(include_paths=["./ci/praktika/job.py"]),
    )
    assert _config_digest(job) == _config_digest(
        replace(job, requires=[PROFILED_BUILD])
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
