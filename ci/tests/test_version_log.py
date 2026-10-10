"""
Tests for the PR tweak-pinning contract in
`ci.jobs.scripts.workflow_hooks.version_log`.

In a PR, `HEAD` is the ephemeral merge commit whose first-parent commit count
(the tweak) changes across close/reopen and re-runs as `master` advances, while
artifacts stay keyed by the head SHA. So the recorded version must pin the tweak
to `1` regardless of what `git rev-list` returns, otherwise one artifact prefix
would accumulate diverging version strings. These tests lock that behaviour in
both sinks: the `version_history` row and the pipeline kv data.
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))
# `version_log` imports `praktika` by bare name, so put `ci/` on the path too.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import ci.jobs.scripts.clickhouse_version as chv
import ci.jobs.scripts.workflow_hooks.version_log as version_log
from ci.jobs.scripts.clickhouse_version import CHVersion

# The tweak the git history would yield on the merge ref: anything but 1, so a
# test that expects 1 is really exercising the pin, not the default.
_GIT_TWEAK = 57


class _FakeInfo:
    """Stand-in for `praktika.info.Info`, shared by `version_log` and
    `clickhouse_version` (the latter calls `Info().store_kv_data` from
    `store_version_data_in_ci_pipeline`)."""

    kv = {}

    def __init__(self, pr_number):
        self.pr_number = pr_number
        self.git_branch = "feature-branch"
        self.pr_url = "http://example/pr"
        self.sha = "headsha"
        self.commit_url = "http://example/commit/headsha"

    def store_kv_data(self, key, value):
        _FakeInfo.kv[key] = value


def _patch_get_current_version(monkeypatch):
    # `get_current_version` reads the file + git history; here it always yields
    # the git tweak, so the pin (or its absence) is the only thing under test.
    def fake(cls, with_tweak=True, no_strict=False):
        return CHVersion(
            26, 6, 1, "54511", tweak=_GIT_TWEAK, githash="deadbeef", version_type="testing"
        )

    monkeypatch.setattr(CHVersion, "get_current_version", classmethod(fake))


def test_build_version_pins_tweak_in_pr(monkeypatch):
    _patch_get_current_version(monkeypatch)
    version = version_log._build_version(_FakeInfo(pr_number=12345))
    assert version.tweak == 1
    assert version.string == "26.6.1.1"
    assert version.describe == "v26.6.1.1-testing"


def test_build_version_keeps_git_tweak_outside_pr(monkeypatch):
    _patch_get_current_version(monkeypatch)
    version = version_log._build_version(_FakeInfo(pr_number=0))
    assert version.tweak == _GIT_TWEAK
    assert version.string == f"26.6.1.{_GIT_TWEAK}"


def test_pr_run_records_pinned_tweak_in_both_sinks(monkeypatch):
    _patch_get_current_version(monkeypatch)
    _FakeInfo.kv = {}
    monkeypatch.setattr(version_log, "Info", lambda: _FakeInfo(pr_number=12345))
    monkeypatch.setattr(chv, "Info", lambda: _FakeInfo(pr_number=12345))
    monkeypatch.setattr(version_log.Shell, "check", staticmethod(lambda *a, **k: True))
    monkeypatch.setattr(
        version_log.Shell, "get_output", staticmethod(lambda *a, **k: "p1 p2")
    )

    inserts = []

    class _FakeCIDB:
        def insert_json(self, table, json_str):
            inserts.append((table, json_str))

    monkeypatch.setattr(version_log, "CIDBCluster", lambda: _FakeCIDB())

    version_log._add_build_to_version_history()

    # version_history row
    assert len(inserts) == 1
    table, payload = inserts[0]
    assert table == "version_history"
    assert payload["version"] == "26.6.1.1"
    # pipeline kv data
    assert _FakeInfo.kv["version"]["string"] == "26.6.1.1"
    assert _FakeInfo.kv["version"]["tweak"] == 1
