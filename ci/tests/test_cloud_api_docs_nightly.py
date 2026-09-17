import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from ci.jobs import cloud_api_docs_nightly


def test_push_uses_app_token_without_verbose_logging(monkeypatch):
    checks = []
    outputs = []
    pushes = []

    monkeypatch.setenv("GITHUB_REPOSITORY", cloud_api_docs_nightly.REPOSITORY)

    def fake_check(command, verbose=False, **_kwargs):
        checks.append((command, verbose))
        return True

    monkeypatch.setattr(cloud_api_docs_nightly.Shell, "check", fake_check)
    monkeypatch.setattr(
        cloud_api_docs_nightly.Shell,
        "get_res_stdout_stderr",
        lambda command, verbose=False: pushes.append((command, verbose)) or (0, "", ""),
    )
    monkeypatch.setattr(
        cloud_api_docs_nightly.Shell,
        "get_output",
        lambda command, **_kwargs: outputs.append(command) or "123",
    )

    assert cloud_api_docs_nightly.open_or_refresh_pr()
    assert len(checks) == 1
    assert len(pushes) == 1

    prepare = checks[0]
    push = pushes[0]
    assert prepare[1] is True
    assert "git commit" in prepare[0]
    assert "git push" not in prepare[0]

    assert push[1] is False
    assert 'token="$(gh auth token)"' in push[0]
    assert "http.https://github.com/.extraheader=" in push[0]
    assert "ClickHouse/ClickHouse.git" in push[0]
    assert "robot/cloud-api-docs:refs/heads/robot/cloud-api-docs" in push[0]
    assert len(outputs) == 1
    assert "--repo ClickHouse/ClickHouse" in outputs[0]


def test_pr_create_targets_upstream_repository(monkeypatch):
    checks = []

    monkeypatch.setenv("GITHUB_REPOSITORY", cloud_api_docs_nightly.REPOSITORY)
    monkeypatch.setattr(
        cloud_api_docs_nightly.Shell,
        "check",
        lambda command, **_kwargs: checks.append(command) or True,
    )
    monkeypatch.setattr(
        cloud_api_docs_nightly.Shell,
        "get_res_stdout_stderr",
        lambda *_args, **_kwargs: (0, "", ""),
    )
    monkeypatch.setattr(
        cloud_api_docs_nightly.Shell,
        "get_output",
        lambda *_args, **_kwargs: "",
    )

    assert cloud_api_docs_nightly.open_or_refresh_pr()
    assert len(checks) == 2
    assert "gh pr create" in checks[1]
    assert "--repo ClickHouse/ClickHouse" in checks[1]


def test_rejects_non_upstream_repository_before_mutation(monkeypatch, capsys):
    monkeypatch.setenv("GITHUB_REPOSITORY", "Blargian/ClickHouse")

    def unexpected_call(*_args, **_kwargs):
        raise AssertionError("workflow mutated state before validating the repository")

    monkeypatch.setattr(cloud_api_docs_nightly.Shell, "check", unexpected_call)
    monkeypatch.setattr(
        cloud_api_docs_nightly.Shell,
        "get_res_stdout_stderr",
        unexpected_call,
    )
    monkeypatch.setattr(cloud_api_docs_nightly.Shell, "get_output", unexpected_call)

    assert not cloud_api_docs_nightly.open_or_refresh_pr()
    output = capsys.readouterr().out
    assert "must run only in ClickHouse/ClickHouse" in output
    assert "got Blargian/ClickHouse" in output


def test_failed_push_reports_redacted_stderr(monkeypatch, capsys):
    monkeypatch.setenv("GITHUB_REPOSITORY", cloud_api_docs_nightly.REPOSITORY)
    monkeypatch.setattr(cloud_api_docs_nightly.Shell, "check", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(
        cloud_api_docs_nightly.Shell,
        "get_res_stdout_stderr",
        lambda *_args, **_kwargs: (
            128,
            "",
            "fatal: unable to access "
            "https://x-access-token:secret-token@github.com/ClickHouse/ClickHouse.git",
        ),
    )

    assert not cloud_api_docs_nightly.open_or_refresh_pr()
    output = capsys.readouterr().out
    assert "failed to push the Cloud API docs bot branch" in output
    assert "fatal: unable to access" in output
    assert "secret-token" not in output
    assert "https://x-access-token:***@github.com/ClickHouse/ClickHouse.git" in output
