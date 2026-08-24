"""
Tests for the loom code.refresh push pre_hook (master + release branches).

Only the push workflows run this hook, so PR CI never exercises it end to
end; these tests pin the things a silent regression would break: the request
carries the *resolved* SSM values (not `Secret.Config` handles) and the
namespace derived from the pushed branch, an unindexed branch's 404 is a
quiet skip, and a secret-resolution failure degrades to a workflow warning
instead of failing the workflow.
"""

import json
import os
import sys
import urllib.error
import urllib.request

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from ci.jobs.scripts.workflow_hooks import loom_code_refresh
from ci.praktika.secret import Secret


class _FakeInfo:
    def __init__(self):
        self.warnings = []

    def get_secret(self, name):
        return Secret.Config(
            name=name, type=Secret.Type.AWS_SSM_PARAMETER, region="us-east-1"
        )

    def add_workflow_warning(self, message):
        self.warnings.append(message)


class _FakeResponse:
    status = 200

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


def _setup(monkeypatch, tmp_path, ref):
    event = tmp_path / "event.json"
    event.write_text(json.dumps({"after": "deadbeef" * 5, "ref": ref}))
    monkeypatch.setenv("GITHUB_EVENT_PATH", str(event))
    info = _FakeInfo()
    monkeypatch.setattr(loom_code_refresh, "Info", lambda: info)
    # Resolve the batched SSM fetch without AWS: values in request order.
    monkeypatch.setattr(
        Secret.Config, "get_value", lambda self: ["https://loom.example/", "tok123"]
    )
    return info


def test_master_push_sends_resolved_secrets_and_push_head(monkeypatch, tmp_path):
    info = _setup(monkeypatch, tmp_path, "refs/heads/master")

    sent = {}

    def fake_urlopen(req, timeout=None):
        sent["url"] = req.full_url
        sent["auth"] = req.get_header("Authorization")
        sent["body"] = json.loads(req.data)
        sent["timeout"] = timeout
        return _FakeResponse()

    monkeypatch.setattr(urllib.request, "urlopen", fake_urlopen)

    loom_code_refresh.refresh()

    assert sent["url"] == "https://loom.example/v1/code.refresh"
    assert sent["auth"] == "Bearer tok123"
    assert sent["body"]["expected_head"] == "deadbeef" * 5
    # The token is namespace-scoped: a body without org/namespace would 403.
    assert sent["body"]["org"] == "clickhouse"
    assert sent["body"]["namespace"] == "code-clickhouse"
    assert sent["timeout"] == 10
    assert info.warnings == []


def test_unindexed_release_branch_404_is_a_quiet_skip(monkeypatch, tmp_path):
    info = _setup(monkeypatch, tmp_path, "refs/heads/25.8")

    sent = {}

    def fake_urlopen(req, timeout=None):
        sent["body"] = json.loads(req.data)
        raise urllib.error.HTTPError(req.full_url, 404, "Not Found", {}, None)

    monkeypatch.setattr(urllib.request, "urlopen", fake_urlopen)

    loom_code_refresh.refresh()

    assert sent["body"]["namespace"] == "code-clickhouse-25-8"
    assert info.warnings == []


def test_master_403_emits_auth_warning(monkeypatch, tmp_path):
    # The token must always cover master's namespace: a 403 there is auth
    # drift (expired/revoked token), never an unindexed branch.
    info = _setup(monkeypatch, tmp_path, "refs/heads/master")

    def fake_urlopen(req, timeout=None):
        raise urllib.error.HTTPError(req.full_url, 403, "Forbidden", {}, None)

    monkeypatch.setattr(urllib.request, "urlopen", fake_urlopen)

    loom_code_refresh.refresh()

    assert len(info.warnings) == 1
    assert "403" in info.warnings[0]


def test_release_branch_403_is_a_quiet_skip(monkeypatch, tmp_path):
    # The token only covers loom-side indexed namespaces, so a 403 on a
    # release branch is the expected not-indexed case - no warning spam.
    info = _setup(monkeypatch, tmp_path, "refs/heads/25.8")

    def fake_urlopen(req, timeout=None):
        raise urllib.error.HTTPError(req.full_url, 403, "Forbidden", {}, None)

    monkeypatch.setattr(urllib.request, "urlopen", fake_urlopen)

    loom_code_refresh.refresh()

    assert info.warnings == []


def test_non_branch_push_sends_no_request(monkeypatch, tmp_path):
    info = _setup(monkeypatch, tmp_path, "refs/tags/v26.9.1.1")

    def fail_urlopen(*args, **kwargs):
        raise AssertionError("no request may be sent without a pushed branch")

    monkeypatch.setattr(urllib.request, "urlopen", fail_urlopen)

    loom_code_refresh.refresh()

    assert info.warnings == []


def test_secret_failure_degrades_to_warning_without_request(monkeypatch):
    class _BrokenInfo(_FakeInfo):
        def get_secret(self, name):
            raise RuntimeError(f"no secret [{name}]")

    info = _BrokenInfo()
    monkeypatch.setattr(loom_code_refresh, "Info", lambda: info)

    def fail_urlopen(*args, **kwargs):
        raise AssertionError("no request may be sent when secrets are unavailable")

    monkeypatch.setattr(urllib.request, "urlopen", fail_urlopen)

    loom_code_refresh.refresh()

    assert info.warnings == ["loom secrets unavailable - skipping code index refresh"]
