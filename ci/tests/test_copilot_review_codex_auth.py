"""
Tests for the identity each `ci/jobs/copilot_review_job.py` backend uses.

The contract this pins down:
  - the Codex backend runs on the ambient GitHub App token the runner mints
    (`enable_gh_auth=True`), so it neither reads a robot secret, nor runs
    `gh auth login`, nor scopes a `GH_CONFIG_DIR` for the agent;
  - a robot token that cannot authenticate therefore cannot fail a Codex run,
    and the retry budget is spent on agent failures instead;
  - the Copilot backend still authenticates a robot token, which its CLI uses
    directly.

Everything external is scripted: no secret is read, no `gh`/`codex` process is
started, and no attempt sleeps.
"""

import os
import subprocess
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))
# `ci/defs/job_configs.py` does `from praktika import ...` rather than
# `from ci.praktika import ...`, so the `ci/` directory itself must be on the
# path for `import praktika` to resolve to `ci/praktika`.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from ci.jobs import copilot_review_job as job
from ci.praktika.result import Result

_ROBOT_PREFIX = "/ci/robot-ch-test-poll"


class Recorder:
    """Collects every scripted external interaction of one `_run` call."""

    def __init__(self):
        self.secret_names = []
        self.argvs = []
        self.commands = []

    @property
    def robot_secret_names(self):
        return [n for n in self.secret_names if str(n).startswith(_ROBOT_PREFIX)]

    @property
    def gh_auth_login_argvs(self):
        return [a for a in self.argvs if list(a[:3]) == ["gh", "auth", "login"]]


@pytest.fixture
def harness(monkeypatch, tmp_path):
    """Script the job's whole outside world and return the Recorder.

    `agent_outcomes` on the recorder drives the fake agent: each entry is a
    `Result.Status` for one invocation, defaulting to OK forever.
    """
    monkeypatch.chdir(tmp_path)
    os.makedirs("./ci/tmp", exist_ok=True)
    review_file = str(tmp_path / "copilot_review.md")
    monkeypatch.setattr(job, "REVIEW_FILE", review_file)
    monkeypatch.setattr(job.time, "sleep", lambda seconds: None)

    rec = Recorder()
    rec.agent_outcomes = []
    rec.gh_auth_login_raises = False

    def fake_get_value(self):
        rec.secret_names.append(self.name)
        return f"token-for-{self.name}"

    def fake_run(argv, *args, **kwargs):
        rec.argvs.append(list(argv))
        if list(argv[:3]) == ["gh", "auth", "login"] and rec.gh_auth_login_raises:
            raise subprocess.CalledProcessError(returncode=1, cmd=list(argv))
        return subprocess.CompletedProcess(argv, 0)

    def fake_from_commands_run(name, command, **kwargs):
        rec.commands.append(command)
        status = rec.agent_outcomes.pop(0) if rec.agent_outcomes else Result.Status.OK
        if status == Result.Status.OK:
            with open(review_file, "w", encoding="utf-8") as f:
                f.write("#### AI Review\nfindings\n")
        return Result(name=name, status=status)

    monkeypatch.setattr(job.Secret.Config, "get_value", fake_get_value)
    monkeypatch.setattr(job.subprocess, "run", fake_run)
    monkeypatch.setattr(job.Result, "from_commands_run", fake_from_commands_run)
    return rec


def _run_codex(rec):
    job._run("review this", job._run_codex_once, "Codex")


def _run_copilot(rec):
    job._run("review this", job._run_copilot_once, "Copilot")


def test_codex_survives_every_robot_token_being_rejected(harness):
    # Harsher than the observed outage, where only one of the two robots was dead.
    harness.gh_auth_login_raises = True

    _run_codex(harness)

    assert harness.commands, "the agent never ran"


def test_codex_reads_no_robot_secret(harness):
    _run_codex(harness)

    assert harness.robot_secret_names == []
    assert job.OPENAI_KEY_SECRET in harness.secret_names


def test_codex_runs_no_gh_auth_login(harness):
    _run_codex(harness)

    assert harness.gh_auth_login_argvs == []


def test_codex_command_scopes_codex_home_but_not_gh_config_dir(harness):
    _run_codex(harness)

    assert len(harness.commands) == 1
    assert "GH_CONFIG_DIR=" not in harness.commands[0]
    assert "CODEX_HOME=" in harness.commands[0]


def test_copilot_still_authenticates_a_robot_token(harness):
    _run_copilot(harness)

    assert len(harness.robot_secret_names) == 1
    assert harness.robot_secret_names[0] in job.ROBOT_NAMES
    assert len(harness.gh_auth_login_argvs) == 1


def test_codex_retry_budget_is_spent_on_agent_failures(harness):
    harness.agent_outcomes = [Result.Status.FAIL, Result.Status.FAIL]

    _run_codex(harness)

    assert len(harness.commands) == job.MAX_ATTEMPTS == 3
    assert harness.gh_auth_login_argvs == []


def test_code_review_job_pre_authenticates_the_github_app():
    """The Codex backend has no identity of its own: every `gh` call it makes
    reads the ambient config the runner authenticates before the job command
    starts, which the agent reaches only because the job runs natively. A
    containerized job gets that config as a `/ghconfig` mount named by
    `GH_CONFIG_DIR`, and every agent command unsets that variable, so
    containerizing this job would leave the agent unauthenticated. The tests
    above pass either way because they stop at the job's own boundary."""
    from ci.workflows.pull_request import workflow

    code_review = next(j for j in workflow.jobs if j.name == "Code Review")
    assert code_review.enable_gh_auth is True
    assert code_review.run_in_docker == ""
