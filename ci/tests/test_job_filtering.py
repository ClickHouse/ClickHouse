"""
Regression tests for the job-filtering logic in ci/praktika/native_jobs.py.

These tests document three bugs in the check_affected_jobs algorithm and
assert the *correct* behaviour.  They currently FAIL against the unfixed code
and should PASS after the three fixes are applied.

Bug 1 — fnmatch '*' crosses '/' directory boundaries
    Python's fnmatch.fnmatch treats '*' as matching any character including
    '/'.  A digest_config include_path like './tests/*.txt' therefore matches
    'tests/ci/lambda/requirements.txt', incorrectly marking jobs as affected
    by unrelated changes.

Bug 2 — Already-filtered jobs pollute all_required_artifacts
    Jobs filtered by a workflow_filter_hook (e.g. "do not run cloud tests")
    are still processed by check_affected_jobs.  Their 'requires' entries are
    added to all_required_artifacts, rescuing their dependencies from
    filtering even though those dependencies will never actually run.

Bug 3 — Transitive dependency rescue is not propagated
    When an unaffected job is rescued from filtering because an affected job
    requires it (by job name), that rescued job's own 'requires' are not also
    added to all_required_artifacts.  Build jobs that provide artifacts for
    the rescued job remain filtered, causing a missing-artifact failure at
    runtime.

Workflow topology used in the tests
------------------------------------

    BUILD  ──provides──> BUILD_ARTIFACT
      ^
      │ requires(BUILD_ARTIFACT)
    ──┼──────────────────────────────
    TEST         digest: tests/queries, tests/*.txt
    DOCKER       digest: docker/
      ^
      │ requires("DOCKER" job name, no CI artifact)
    TEST_ON_DOCKER   digest: tests/integration/

An optional OPTIONAL_JOB (no digest_config, always affected) filtered by
a custom hook is used to expose Bug 2.
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

import pytest
from ci.praktika.job import Job
from ci.praktika.native_jobs import _filter_unaffected_jobs
from ci.praktika.runtime import RunConfig
from ci.praktika.utils import Utils


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


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


def _make_job(name, *, requires=(), provides=(), include_paths=None):
    """Create a minimal Job.Config for filtering tests."""
    digest_config = (
        Job.CacheDigestConfig(include_paths=list(include_paths))
        if include_paths is not None
        else None  # no digest_config → always affected
    )
    return Job.Config(
        name=name,
        runs_on=[],
        command="true",
        requires=list(requires),
        provides=list(provides),
        digest_config=digest_config,
    )


def _apply_filters(jobs, changed_files, filter_hook=None):
    """
    Simulate the two filtering phases that _config_workflow applies:

      Phase 1 — workflow_filter_hooks
          Calls filter_hook(job.name) for every job; marks any job for which
          the hook returns (True, reason) as filtered.

      Phase 2 — _filter_unaffected_jobs (production function from native_jobs)

    Returns the filtered_jobs dict from the resulting RunConfig.
    """
    workflow_config = _make_run_config()

    # Phase 1: workflow filter hook
    if filter_hook is not None:
        for job in jobs:
            should_skip, reason = filter_hook(job.name)
            if should_skip:
                workflow_config.set_job_as_filtered(job.name, reason)

    # Phase 2: production filtering logic
    _filter_unaffected_jobs(jobs, workflow_config, changed_files)

    return workflow_config.filtered_jobs


# ---------------------------------------------------------------------------
# Shared job definitions
# ---------------------------------------------------------------------------

BUILD = _make_job(
    "BUILD",
    provides=["BUILD_ARTIFACT"],
    include_paths=["./src"],
)
TEST = _make_job(
    "TEST",
    requires=["BUILD_ARTIFACT"],
    include_paths=["./tests/queries", "./tests/*.txt"],
)
DOCKER = _make_job(
    "DOCKER",
    requires=["BUILD_ARTIFACT"],
    include_paths=["./docker"],
)
TEST_ON_DOCKER = _make_job(
    "TEST_ON_DOCKER",
    requires=["DOCKER"],  # job-name dependency — no CI artifact produced by DOCKER
    include_paths=["./tests/integration"],
)

ALL_JOBS = [BUILD, TEST, DOCKER, TEST_ON_DOCKER]


# ---------------------------------------------------------------------------
# Bug 1: fnmatch '*' crosses '/' directory boundaries
# ---------------------------------------------------------------------------


class TestBug1FnmatchCrossesDirectories:
    """
    'tests/*.txt' is intended to match files directly under tests/ (e.g.
    tests/list_of_tests.txt).  Python's fnmatch.fnmatch treats '*' as matching
    any character including '/', so it also matches deeply nested paths such as
    tests/ci/lambda/requirements.txt — pulling in completely unrelated changes.
    """

    def test_txt_file_in_subdirectory_does_not_match_shallow_glob(self):
        """
        A .txt file two levels deep must NOT trigger 'tests/*.txt'.
        """
        assert not TEST.is_affected_by(["tests/ci/lambda/requirements.txt"]), (
            "tests/*.txt should only match files directly under tests/, "
            "not tests/ci/lambda/requirements.txt"
        )

    def test_txt_file_directly_under_tests_matches_shallow_glob(self):
        """
        A .txt file directly under tests/ SHOULD trigger 'tests/*.txt'.
        This verifies the intended use-case still works after the fix.
        """
        assert TEST.is_affected_by(["tests/list_of_tests.txt"])

    def test_unrelated_extension_in_subdirectory_does_not_match(self):
        """
        A non-.txt file in a subdirectory must not match 'tests/*.txt'.
        """
        assert not TEST.is_affected_by(["tests/ci/terraform/lambdas.tf"])

    def test_txt_file_under_same_named_subdir_does_not_match(self):
        """
        PurePosixPath.match is not anchored by default, so 'a/tests/foo.txt'
        would match 'tests/*.txt' via right-side matching.  Anchoring both
        sides with '/' prevents this false positive.
        """
        assert not TEST.is_affected_by(["a/tests/list_of_tests.txt"]), (
            "tests/*.txt must not match a/tests/list_of_tests.txt — "
            "the match must be anchored to the repository root"
        )

    def test_query_file_directly_under_tests_queries_matches(self):
        """
        A file under tests/queries/ is matched by the 'tests/queries' include
        path (directory prefix), which is the intended behaviour.
        """
        assert TEST.is_affected_by(["tests/queries/0_stateless/01234_foo.sql"])


# ---------------------------------------------------------------------------
# Bug 2: Already-filtered jobs must not contribute to all_required_artifacts
# ---------------------------------------------------------------------------


class TestBug2FilteredJobsPollutingRequiredArtifacts:
    """
    OPTIONAL_JOB has no digest_config (so is_affected_by always returns True)
    and is filtered by the workflow hook.  Because it is never going to run,
    its requires=["DOCKER"] must not rescue DOCKER from being filtered.
    """

    # No digest_config → always treated as affected regardless of changed files.
    OPTIONAL_JOB = _make_job(
        "OPTIONAL_JOB",
        requires=["DOCKER"],
        include_paths=None,  # always affected
    )

    @staticmethod
    def _hook(job_name):
        if job_name == "OPTIONAL_JOB":
            return True, "not needed for this PR type"
        return False, ""

    def test_docker_is_filtered_when_only_requirer_is_hook_filtered(self):
        """
        OPTIONAL_JOB is the only job that requires DOCKER.
        OPTIONAL_JOB is filtered by the hook, so it will never run.
        DOCKER (and transitively BUILD) must therefore be filtered.
        """
        jobs = ALL_JOBS + [self.OPTIONAL_JOB]
        changed = ["ci/lambda/deploy.sh"]  # nothing in ALL_JOBS is affected

        filtered = _apply_filters(jobs, changed, filter_hook=self._hook)

        assert "DOCKER" in filtered, (
            "DOCKER must be filtered: its only requirer (OPTIONAL_JOB) is "
            "filtered by the workflow hook and will never run"
        )
        assert "BUILD" in filtered, (
            "BUILD must be filtered: nothing that actually runs requires it"
        )

    def test_docker_is_not_filtered_when_unfiltered_job_requires_it(self):
        """
        Sanity check: when an *unfiltered* job requires DOCKER, DOCKER must stay.
        """
        jobs = ALL_JOBS + [self.OPTIONAL_JOB]
        # TEST_ON_DOCKER is directly affected → it requires DOCKER → DOCKER must stay
        changed = ["tests/integration/test_something.py"]

        filtered = _apply_filters(jobs, changed, filter_hook=self._hook)

        assert "DOCKER" not in filtered
        assert "TEST_ON_DOCKER" not in filtered


# ---------------------------------------------------------------------------
# Bug 3: Transitive dependency rescue must be propagated
# ---------------------------------------------------------------------------


class TestBug3TransitiveDepRescueNotPropagated:
    """
    TEST_ON_DOCKER (affected) requires DOCKER (rescued).
    DOCKER requires BUILD_ARTIFACT (provided by BUILD).
    BUILD must therefore also be rescued — but the current algorithm only
    does a single-level rescue and leaves BUILD filtered.
    """

    def test_build_is_rescued_when_docker_is_rescued(self):
        """
        TEST_ON_DOCKER is affected by a change in tests/integration/.
        TEST_ON_DOCKER requires DOCKER (by job name) → DOCKER is rescued.
        DOCKER requires BUILD_ARTIFACT → BUILD must also be rescued.
        """
        changed = ["tests/integration/test_something.py"]

        filtered = _apply_filters(ALL_JOBS, changed)

        assert "TEST_ON_DOCKER" not in filtered, "TEST_ON_DOCKER is directly affected"
        assert "DOCKER" not in filtered, (
            "DOCKER must not be filtered: TEST_ON_DOCKER requires it"
        )
        assert "BUILD" not in filtered, (
            "BUILD must not be filtered: DOCKER (kept alive to serve "
            "TEST_ON_DOCKER) requires BUILD_ARTIFACT which BUILD provides"
        )

    def test_unrelated_jobs_are_still_filtered(self):
        """
        Jobs that are neither affected nor transitively required must be filtered.
        TEST is not affected by tests/integration/ and nothing requires it,
        so it must be filtered.
        """
        changed = ["tests/integration/test_something.py"]

        filtered = _apply_filters(ALL_JOBS, changed)

        assert "TEST" in filtered, (
            "TEST is unaffected and not required — it must be filtered"
        )

    def test_all_jobs_filtered_when_nothing_is_affected(self):
        """
        When no changed file touches any job's digest_config, all jobs are filtered.
        """
        changed = ["ci/lambda/deploy.sh"]

        filtered = _apply_filters(ALL_JOBS, changed)

        assert set(filtered) == {"BUILD", "TEST", "DOCKER", "TEST_ON_DOCKER"}, (
            "All jobs must be filtered when no changed file is relevant"
        )

    def test_direct_artifact_dependency_rescue_already_works(self):
        """
        Verify the single-level rescue that *does* work: when DOCKER is directly
        affected (its docker/ files changed), it requires BUILD_ARTIFACT, so BUILD
        is added to all_required_artifacts immediately (first pass, not second
        pass rescue) and is therefore correctly kept alive.
        """
        changed = ["docker/Dockerfile"]

        filtered = _apply_filters(ALL_JOBS, changed)

        assert "DOCKER" not in filtered, "DOCKER is directly affected"
        assert "BUILD" not in filtered, (
            "BUILD provides BUILD_ARTIFACT which is directly in all_required_artifacts "
            "because the affected DOCKER job requires it"
        )
