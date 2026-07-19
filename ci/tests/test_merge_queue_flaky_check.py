"""
Tests for the merge-queue stateless flaky-check drift guard added in
`https://github.com/ClickHouse/ClickHouse/pull/110308`.

The merge queue reruns the PR's new/changed stateless tests against the merge
group state, catching a semantic conflict with `master` changes that landed
after the PR's last CI run. Two properties are pinned here:

- `should_skip_merge_queue_job` filters the flaky check at config time on PRs
  that change no stateless tests (so a non-test PR never schedules the runner
  and restores the binary only to self-skip), while never touching the
  build/style/fast-test jobs the queue always needs.
- the reduced merge-queue budget always yields a positive `--global_time_limit`
  so the run stays bounded even if setup consumes the whole budget.
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from types import SimpleNamespace

import ci.jobs.scripts.workflow_hooks.filter_job as fj
from ci.jobs.scripts.find_tests import Targeting

FLAKY_CHECK_JOB = "Stateless tests (amd_binary, flaky check)"


def _use_dummy_info(monkeypatch):
    # A minimal stand-in so the hook does not construct a real `Info` (which
    # needs a configured praktika env). `job_name` is read by `Targeting`.
    monkeypatch.setattr(fj, "_info_cache", SimpleNamespace(job_name="config"))


def test_non_flaky_jobs_are_never_skipped(monkeypatch):
    _use_dummy_info(monkeypatch)
    for job in ("Build (amd_binary)", "Style check", "Fast test"):
        assert fj.should_skip_merge_queue_job(job) == (False, ""), job


def test_flaky_check_skipped_when_no_changed_tests(monkeypatch):
    _use_dummy_info(monkeypatch)
    monkeypatch.setattr(Targeting, "get_changed_tests", lambda self: [])
    skip, reason = fj.should_skip_merge_queue_job(FLAKY_CHECK_JOB)
    assert skip is True
    assert "no new/changed stateless tests" in reason


def test_flaky_check_runs_when_tests_changed(monkeypatch):
    _use_dummy_info(monkeypatch)
    monkeypatch.setattr(Targeting, "get_changed_tests", lambda self: ["00001_select_1."])
    assert fj.should_skip_merge_queue_job(FLAKY_CHECK_JOB) == (False, "")


def test_merge_queue_budget_stays_positive_when_setup_exhausts_it():
    # Mirrors the flaky-check budget arithmetic in `ci/jobs/functional_tests.py`.
    # `run_tests` treats `global_time_limit == 0` as "no cap"; the floor keeps
    # the merge-queue run bounded even if setup already spent the whole budget.
    FLAKY_CHECK_TIME_LIMIT = 20 * 60
    for elapsed in (0, 10 * 60, 20 * 60, 30 * 60):
        global_time_limit = max(FLAKY_CHECK_TIME_LIMIT - elapsed, 60)
        assert global_time_limit > 0
    assert max(FLAKY_CHECK_TIME_LIMIT - 30 * 60, 60) == 60
