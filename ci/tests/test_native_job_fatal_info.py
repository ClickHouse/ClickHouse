"""A praktika native job that dies by exception must report that exception.

`Config Workflow` is the first job of the PR workflow, and praktika marks every
dependee `DROPPED` when it errors, so its report is the only description of a voided
matrix. The `__main__` handler in `ci/praktika/native_jobs.py` is the only frame that
holds the fatal traceback: the runner's log-tail fallback is gated on
`not result.is_completed()`, and an ERROR result is already completed, so nothing
downstream recovers it.

The handler is driven for real here (`runpy` with `run_name="__main__"`) with only the
boundary that raises replaced, so the assertions read the `Result` the job dumps.
"""

import json
import os
import runpy
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

import pytest

import ci.praktika.mangle as mangle
from ci.praktika._environment import _Environment
from ci.praktika.job import Job
from ci.praktika.result import Result
from ci.praktika.runner import Runner
from ci.praktika.settings import Settings
from ci.praktika.workflow import Workflow

JOB_NAME = "Config Workflow"
SENTINEL = "Failed to set both GH commit status and PR comment, cannot proceed"


def _make_env(tmp_path):
    """A real _Environment, so everything the handler, `complete_job` and `_post_run`
    read off it (WORKFLOW_NAME, SHA, JOB_OUTPUT_STREAM, ...) behaves as in production.
    """
    env = _Environment.from_env()
    env.WORKFLOW_NAME = "PR"
    env.JOB_NAME = JOB_NAME
    env.SHA = "cbac4cec7c4dac888f916ef80f163e63e810af37"
    env.PR_NUMBER = 90117
    env.BRANCH = "master"
    env.JOB_OUTPUT_STREAM = str(tmp_path / "gh_output")
    return env


def _run_native_job(monkeypatch, tmp_path, message=SENTINEL, env=None):
    """Run the handler over a raising `_get_workflows` and return the dumped Result."""

    def _raise(*_args, **_kwargs):
        raise RuntimeError(message)

    env = env if env is not None else _make_env(tmp_path)

    monkeypatch.setattr(mangle, "_get_workflows", _raise)
    monkeypatch.setattr(_Environment, "get", classmethod(lambda cls: env))
    monkeypatch.setattr(Settings, "TEMP_DIR", str(tmp_path))
    monkeypatch.setattr(sys, "argv", ["native_jobs.py", JOB_NAME])

    with pytest.raises(SystemExit) as exit_info:
        runpy.run_module("ci.praktika.native_jobs", run_name="__main__")
    assert exit_info.value.code == 1, "a failed native job must exit non-zero"

    dumped = tmp_path / f"result_{JOB_NAME.lower().replace(' ', '_')}.json"
    assert (
        dumped.is_file()
    ), f"the job must dump its own result, found {list(tmp_path.iterdir())}"
    result = Result.from_dict(json.loads(dumped.read_text(encoding="utf8")))
    assert result.status == Result.Status.ERROR
    return result


def _drive_post_run(result):
    """Run the production post-run step that appends `env.TRACEBACKS` to the info.

    Every remote-facing step of `_post_run` is opt-in per workflow, so a bare
    `Workflow.Config` reaches the append with no network and no artifacts.
    """
    job = Job.Config(name=JOB_NAME, runs_on=["x"], command="")
    workflow = Workflow.Config(name="PR", event=Workflow.Event.PULL_REQUEST, jobs=[job])
    assert Runner()._post_run(result, workflow, job, run_exit_code=1)


def test_fatal_exception_reaches_the_job_report(monkeypatch, tmp_path):
    result = _run_native_job(monkeypatch, tmp_path)

    assert SENTINEL in result.info
    assert "RuntimeError" in result.info
    assert "Traceback (most recent call last)" in result.info


def test_fatal_exception_is_read_before_an_earlier_handled_one(monkeypatch, tmp_path):
    """A handled traceback stored earlier in the job must not precede the cause.

    `Info.store_traceback` accumulates handled tracebacks in `env.TRACEBACKS`, which
    `Runner._post_run` appends to the same info. That append is executed here, so the
    ordering is observed rather than modelled.
    """
    handled = (
        "Traceback (most recent call last):\n"
        '  File "ci/praktika/gh.py", line 912, in get_pr_title_body_labels\n'
        "json.decoder.JSONDecodeError: Expecting value: line 1 column 1 (char 0)\n"
    )
    env = _make_env(tmp_path)
    env.TRACEBACKS = [handled]

    result = _run_native_job(monkeypatch, tmp_path, env=env)
    _drive_post_run(result)

    assert "JSONDecodeError" in result.info, "the handled traceback must still be kept"
    assert result.info.index(SENTINEL) < result.info.index("JSONDecodeError")


def test_attached_traceback_is_bounded(monkeypatch, tmp_path):
    """An exception message can carry command output of any size.

    Top-truncation discards the leading lines that name the exception, so the type and
    the first message line are re-stated ahead of the traceback.
    """
    message = "\n".join([f"line {i} " + "x" * 4000 for i in range(400)])
    result = _run_native_job(monkeypatch, tmp_path, message=message)

    lines = result.info.splitlines()
    assert len(lines) <= 102, f"info kept {len(lines)} lines"
    assert max(len(line) for line in lines) <= 1003
    assert "RuntimeError" in result.info, "the exception type must survive truncation"
    assert "line 0 " in result.info, "the first message line must survive truncation"
    assert "line 399 " in result.info, "the message tail must be kept"
