"""
Tests for `ci.jobs.scripts.clickhouse_version.CHVersion.get_current_version_as_dict`.

In PRs the version tweak is pinned to 1 instead of being derived from the git
commit count. Closing and reopening a PR makes GitHub rebuild the ephemeral
merge commit on an advanced master, so `git rev-list --count ...` returns a
different tweak for the same HEAD commit. Two builds for one HEAD sha would
then upload artifacts with diverging version strings to the same S3 prefix and
the "Install packages (*_release)" job would later abort with
"cannot install both clickhouse-keeper-...-250 and ...-185". Pinning tweak=1 in
PRs keeps the version stable per HEAD commit and removes that precondition.

On master / release branches the tweak is still the real commit count.
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from ci.jobs.scripts.clickhouse_version import CHVersion

_RELEASE = {
    "major": 26,
    "minor": 6,
    "patch": 1,
    "revision": "54511",
    "githash": "cdee4c67ae5d33f5d1853693325b23ed88a36cfc",
    "describe": "v26.6.1.1-testing",
    "string": "26.6.1.1",
}


class _FakeInfo:
    def __init__(self, pr_number, sha="deadbeef", git_branch="some-branch"):
        self.pr_number = pr_number
        self.sha = sha
        self.git_branch = git_branch


def _patch(monkeypatch, pr_number, commit_count="123"):
    monkeypatch.setattr(
        CHVersion, "get_release_version_as_dict", classmethod(lambda cls: dict(_RELEASE))
    )
    monkeypatch.setattr(
        "ci.jobs.scripts.clickhouse_version.Info",
        lambda: _FakeInfo(pr_number),
    )
    calls = []

    def fake_get_output(command, verbose=False):
        calls.append(command)
        return commit_count

    monkeypatch.setattr(
        "ci.jobs.scripts.clickhouse_version.Shell.get_output", fake_get_output
    )
    return calls


def test_pr_pins_tweak_to_one(monkeypatch):
    # A PR build: tweak is pinned regardless of the real commit count.
    calls = _patch(monkeypatch, pr_number=107138, commit_count="250")
    version = CHVersion.get_current_version_as_dict()
    assert version["tweak"] == 1
    assert version["string"] == "26.6.1.1"
    assert version["describe"] == "v26.6.1.1-testing"
    # git rev-list must not be consulted in PRs.
    assert calls == []


def test_pr_tweak_stable_across_reopen(monkeypatch):
    # Same HEAD, different merge-commit count after a close/reopen: still 1.
    v1 = (
        _patch(monkeypatch, pr_number=107138, commit_count="185"),
        CHVersion.get_current_version_as_dict()["string"],
    )[1]
    v2 = (
        _patch(monkeypatch, pr_number=107138, commit_count="250"),
        CHVersion.get_current_version_as_dict()["string"],
    )[1]
    assert v1 == v2 == "26.6.1.1"


def test_master_uses_real_commit_count(monkeypatch):
    # pr_number == 0 (master / release branch): tweak is the real count.
    calls = _patch(monkeypatch, pr_number=0, commit_count="106")
    version = CHVersion.get_current_version_as_dict()
    assert version["tweak"] == 106
    assert version["string"] == "26.6.1.106"
    assert len(calls) == 1
    assert "git rev-list --count --first-parent" in calls[0]


def test_master_shallow_checkout_falls_back_to_one(monkeypatch):
    # pr_number == 0 but git rev-list returns garbage: keep the tweak=1 fallback.
    monkeypatch.setattr(
        CHVersion, "get_release_version_as_dict", classmethod(lambda cls: dict(_RELEASE))
    )
    monkeypatch.setattr(
        "ci.jobs.scripts.clickhouse_version.Info", lambda: _FakeInfo(0)
    )
    monkeypatch.setattr(
        "ci.jobs.scripts.clickhouse_version.Shell.get_output",
        lambda command, verbose=False: "not-a-number",
    )
    version = CHVersion.get_current_version_as_dict()
    assert version["tweak"] == 1
    assert version["string"] == "26.6.1.1"
