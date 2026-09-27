"""
Regression test: the per-arch Bugfix Validation jobs must NOT be cacheable.

The Bugfix Validation verdict depends on inputs that are not files in the
repository - the PR's source fix and the master-HEAD binary that the runner
downloads at run time (see `bugfix_validation.find_master_builds`), and master
HEAD advances independently of the PR. No `CacheDigestConfig` can capture those
inputs, so any cached verdict is unsound.

Concretely, a per-arch job legitimately reports a top-level `SKIPPED` when the
bug does not reproduce on master HEAD on that arch. The CI cache decides what to
persist as a reusable "success" with `Result.is_ok`, which treats `SKIPPED` as
success, so that `SKIPPED` is pushed as a cache-success record. On a later
commit whose test content hashes to the same digest, the config job reuses it
("reused from cache") instead of re-running - even after the fix or master HEAD
changed. The merge-blocking `new_tests_check.py` post-hook uses strict
`is_success` (a reused `SKIPPED` does not count), so it then fails with "No
per-arch Bugfix Validation job validated the bug".

Leaving `digest_config` unset makes `calc_job_digest` return the null digest,
which `hook_cache.py` excludes from both cache lookup and cache push, so the
job re-runs on every eligible commit (still gated to bug-fix PRs with test
changes by `filter_job.py`). This test pins that so the digest is not
reintroduced. See ClickHouse/ClickHouse#109229.
"""

import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))
# `ci/defs/job_configs.py` does `from praktika import ...` rather than
# `from ci.praktika import ...`, so the `ci/` directory itself must be on the
# path for `import praktika` to resolve to `ci/praktika`.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from ci.defs.job_configs import JobConfigs
from ci.praktika.digest import Digest


def _bugfix_validation_jobs():
    return list(JobConfigs.bugfix_validation_ft_pr_jobs) + list(
        JobConfigs.bugfix_validation_it_jobs
    )


def test_all_four_per_arch_jobs_present():
    """Guard against a rename/refactor silently dropping a per-arch variant."""
    names = {j.name for j in _bugfix_validation_jobs()}
    assert names == {
        "Bugfix validation (functional tests, amd64)",
        "Bugfix validation (functional tests, aarch64)",
        "Bugfix validation (integration tests, amd64)",
        "Bugfix validation (integration tests, aarch64)",
    }


def test_bugfix_validation_jobs_have_no_digest_config():
    """No `digest_config` => the job is not cacheable (see module docstring)."""
    for job in _bugfix_validation_jobs():
        assert (
            job.digest_config is None
        ), f"Bugfix Validation job [{job.name}] must not have a digest_config"


def test_bugfix_validation_jobs_resolve_to_null_digest():
    """The null digest is what `hook_cache.py` skips for both lookup and push."""
    digest = Digest()
    null = Digest.get_null_digest()
    for job in _bugfix_validation_jobs():
        job_digest = digest.calc_job_digest(
            job_config=job, docker_digests={}, artifact_configs={}
        )
        assert (
            job_digest == null
        ), f"Bugfix Validation job [{job.name}] must resolve to the null digest"


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
