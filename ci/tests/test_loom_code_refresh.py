"""
Tests for the loom code.refresh master pre_hook.

Only the master workflow runs this hook, so PR CI never exercises it end to
end; these tests pin the two things a silent regression would break: the
request carries the *resolved* SSM values (not `Secret.Config` handles), and
a secret-resolution failure degrades to a workflow warning instead of failing
master.
"""

import json
import os
import sys
import urllib.request

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from ci.jobs.scripts.workflow_hooks import loom_code_refresh
from ci.praktika.secret import Secret


class _FakeInfo:
    def __init__(self, secrets):
        self._secrets = secrets
        self.warnings = []

    def get_secret(self, name):
        return self._secrets[name]

    def add_workflow_warning(self, message):
        self.warnings.append(message)


class _FakeResponse:
    status = 200

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


def _ssm_config(name):
    return Secret.Config(
        name=name, type=Secret.Type.AWS_SSM_PARAMETER, region="us-east-1"
    )


def test_request_carries_resolved_secrets_and_push_head(monkeypatch, tmp_path):
    event = tmp_path / "event.json"
    event.write_text(json.dumps({"after": "deadbeef" * 5}))
    monkeypatch.setenv("GITHUB_EVENT_PATH", str(event))

    info = _FakeInfo(
        {"loom-url": _ssm_config("loom-url"), "loom-ci-token": _ssm_config("loom-ci-token")}
    )
    monkeypatch.setattr(loom_code_refresh, "Info", lambda: info)
    # Resolve the batched SSM fetch without AWS: values in request order.
    monkeypatch.setattr(
        Secret.Config, "get_value", lambda self: ["https://loom.example/", "tok123"]
    )

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


def test_secret_failure_degrades_to_warning_without_request(monkeypatch):
    class _BrokenInfo(_FakeInfo):
        def get_secret(self, name):
            raise RuntimeError(f"no secret [{name}]")

    info = _BrokenInfo({})
    monkeypatch.setattr(loom_code_refresh, "Info", lambda: info)

    def fail_urlopen(*args, **kwargs):
        raise AssertionError("no request may be sent when secrets are unavailable")

    monkeypatch.setattr(urllib.request, "urlopen", fail_urlopen)

    loom_code_refresh.refresh()

    assert info.warnings == ["loom secrets unavailable - skipping code index refresh"]
