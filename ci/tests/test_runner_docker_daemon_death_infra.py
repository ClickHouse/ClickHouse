"""
Test for Runner._is_docker_daemon_death.

When the host docker daemon dies mid-run, the main test container returns
exit 125 and the job is truncated (all already-executed tests OK). That is a
runner-host infrastructure event, not a test or code failure, so the job
result must be labeled infra to let the auto-retry re-run it on a fresh runner
instead of reddening the merge check. This test pins the signature detection
in both directions.
"""

import json
import os
import shlex
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from ci.praktika.runner import Runner
from ci.praktika.result import Result
from ci.praktika.settings import Settings
from ci.praktika.job import Job
from ci.praktika._environment import _Environment

# A representative teardown tail from a real signature-A truncation: the last
# test passed, then the docker daemon connection was canceled and the main
# container exited 125.
_DAEMON_DEATH_LOG = (
    "[5658 / 5683] 02437_drop_mv_restart_replicas:  [ OK ] 26.29 sec.\n"
    'level=error msg="Error waiting for container: Canceled: grpc: the client '
    'connection is closing: context canceled"\n'
    "--- Fixing file ownership after running docker as root\n"
    'error during connect: Get "http://%2Fvar%2Frun%2Fdocker.sock/_ping": EOF\n'
    "ERROR: command failed after 1/1 attempt(s), exit code: 1\n"
    "ERROR: Job killed, exit code [125]\n"
)


def test_docker_daemon_death_is_detected():
    assert Runner._is_docker_daemon_death(125, _DAEMON_DEATH_LOG)


def test_cancellation_conjunction_is_detected():
    # The full cancellation line carries BOTH markers together; that is the
    # mid-run daemon-death signature.
    log = 'msg="Error waiting for container: Canceled: grpc: the client connection is closing: context canceled"'
    assert Runner._is_docker_daemon_death(125, log)


def test_plain_docker_sock_connect_failure_is_not_infra():
    # A deterministic docker-connectivity regression also exits 125 and leaves
    # the result unfinished, but it is a real regression that must surface
    # rather than be auto-retried forever. It carries neither cancellation
    # marker, so it must NOT be classified as transient infra.
    log = (
        "ERROR: command failed after 1/1 attempt(s), exit code: 1\n"
        "Cannot connect to the Docker daemon at unix:///var/run/docker.sock. "
        "Is the docker daemon running?\n"
        "ERROR: Job killed, exit code [125]\n"
    )
    assert not Runner._is_docker_daemon_death(125, log)


def test_single_cancellation_marker_alone_is_not_infra():
    # Require the conjunction: neither marker alone counts.
    only_canceled = 'msg="Error waiting for container: Canceled"'
    only_grpc = "grpc: the client connection is closing"
    assert not Runner._is_docker_daemon_death(125, only_canceled)
    assert not Runner._is_docker_daemon_death(125, only_grpc)


def test_real_test_failure_is_not_infra():
    # Exit 125 but the log carries a genuine test failure, not a daemon death.
    log = (
        "[5000 / 5683] 01234_some_test:  [ FAIL ] 1.11 sec.\n"
        "Received exception from server: Code: 47. DB::Exception: Unknown identifier\n"
    )
    assert not Runner._is_docker_daemon_death(125, log)


def test_non_125_exit_code_is_not_infra():
    # The daemon-death signature only counts when the exit code is 125.
    assert not Runner._is_docker_daemon_death(1, _DAEMON_DEATH_LOG)


def test_empty_log_is_not_infra():
    assert not Runner._is_docker_daemon_death(125, "")
    assert not Runner._is_docker_daemon_death(125, None)


def test_infra_label_stored_as_bare_string_for_retry_jq():
    # retry_infra_failures.yml matches infra with `any(. == "infra")`, which
    # only ever sees a plain string. set_label() stores a dict, which that jq
    # would never match, so the runner must append the bare string. Drive the
    # production method, not a copy of it, so the assertion fails if the
    # runner stops labeling or switches back to the dict form.
    result = Result(name="job", status=Result.Status.ERROR)
    assert Runner._label_infra_on_docker_daemon_death(
        result, 125, _DAEMON_DEATH_LOG, False
    )
    stored = result.ext["labels"]
    assert stored == ["infra"]
    # This is exactly what the workflow jq evaluates.
    assert any(item == "infra" for item in stored)


def test_infra_label_is_idempotent():
    # Two labeling passes must not duplicate the entry.
    result = Result(name="job", status=Result.Status.ERROR)
    for _ in range(2):
        assert Runner._label_infra_on_docker_daemon_death(
            result, 125, _DAEMON_DEATH_LOG, False
        )
    assert result.ext["labels"] == ["infra"]


def test_infra_label_preserves_existing_labels():
    # Labeling must append, never clobber labels an earlier step recorded
    # (including the dict form other code paths still write).
    result = Result(name="job", status=Result.Status.ERROR)
    result.ext["labels"] = [{"name": "flaky"}]
    assert Runner._label_infra_on_docker_daemon_death(
        result, 125, _DAEMON_DEATH_LOG, False
    )
    assert result.ext["labels"] == [{"name": "flaky"}, "infra"]


def test_no_infra_label_for_timeout_or_non_daemon_death():
    # A timed-out run keeps its own timeout handling, and a plain docker.sock
    # connect failure is a real regression: neither may be labeled infra.
    timed_out = Result(name="job", status=Result.Status.ERROR)
    assert not Runner._label_infra_on_docker_daemon_death(
        timed_out, 125, _DAEMON_DEATH_LOG, True
    )
    assert "labels" not in timed_out.ext

    connect_failure = Result(name="job", status=Result.Status.ERROR)
    assert not Runner._label_infra_on_docker_daemon_death(
        connect_failure,
        125,
        "Cannot connect to the Docker daemon at unix:///var/run/docker.sock\n",
        False,
    )
    assert "labels" not in connect_failure.ext


class _FakeEnv:
    """_run only reads/writes these three members of the environment."""

    JOB_NAME = ""
    WORKFLOW_CONFIG = None

    def dump(self):
        pass


class _FakeProcess:
    """Stands in for TeePopen: only the two members the branch reads."""

    def __init__(self, log_tail, timeout_exceeded=False):
        self._log_tail = log_tail
        self.timeout_exceeded = timeout_exceeded

    def get_latest_log(self, max_lines=20):
        return self._log_tail


class _FakeJob:
    name = "fake job"
    timeout = 9000


def _finalize(tmp_path, monkeypatch, log_tail, timeout_exceeded=False, exit_code=125):
    """Drive the real Runner finalization branch and return the persisted JSON.

    The result file is what retry_infra_failures.yml reads, so asserting on the
    file (not on the in-memory object) covers the ordering too: a label applied
    after the dump would not appear here.
    """
    monkeypatch.setattr(Settings, "TEMP_DIR", str(tmp_path))
    job = _FakeJob()
    # A truncated run: the runner wrote RUNNING and never reached completion.
    Result(name=job.name, status=Result.Status.RUNNING).dump()
    Runner._finalize_job_result(
        job, _FakeProcess(log_tail, timeout_exceeded), exit_code, None
    )
    with open(Result.file_name_static(job.name)) as f:
        return json.load(f)


def test_run_persists_bare_infra_label_on_daemon_death(tmp_path, monkeypatch):
    # End-to-end through the production branch: the persisted result must carry
    # the bare string, which is what the retry workflow jq matches.
    persisted = _finalize(tmp_path, monkeypatch, _DAEMON_DEATH_LOG)
    assert persisted["ext"]["labels"] == ["infra"]
    assert persisted["status"] == Result.Status.ERROR


def test_run_does_not_label_timeout_or_connect_failure(tmp_path, monkeypatch):
    # A timed-out run keeps its own handling, and a plain docker.sock connect
    # failure is a real regression: neither may be auto-retried.
    timed_out = _finalize(
        tmp_path, monkeypatch, _DAEMON_DEATH_LOG, timeout_exceeded=True
    )
    assert "infra" not in timed_out["ext"].get("labels", [])

    connect_failure = _finalize(
        tmp_path,
        monkeypatch,
        "Cannot connect to the Docker daemon at unix:///var/run/docker.sock\n",
    )
    assert "infra" not in connect_failure["ext"].get("labels", [])


def _run_no_docker_job(tmp_path, monkeypatch, script, timeout=60):
    """Drive Runner._run end to end without Docker and return the result JSON.

    `run_in_docker` is empty, so `_run` executes `job.command` directly through
    TeePopen. The script writes the RUNNING result the real job would leave
    behind, emits the teardown tail on stdout, and exits 125, which is exactly
    the truncated daemon-death shape.
    """
    monkeypatch.setattr(Settings, "TEMP_DIR", str(tmp_path))
    monkeypatch.setattr(Runner, "LOCAL_ENV_FILE", str(tmp_path / "no-such-env"))
    monkeypatch.setattr(_Environment, "get", classmethod(lambda cls: _FakeEnv()))
    job = Job.Config(name="fake job", runs_on=[], command=script, timeout=timeout)
    Runner()._run(workflow=None, job=job, no_docker=True)
    with open(Result.file_name_static(job.name)) as f:
        return json.load(f)


def test_run_end_to_end_persists_bare_infra_label(tmp_path, monkeypatch):
    """The sole production call site must apply the label.

    The helper-level tests above cover the decision in detail; this one pins
    that `_run` still routes a truncated daemon-death run through it, so
    removing or misguarding that call cannot pass silently.
    """
    # The job leaves a RUNNING result behind, prints the teardown tail and
    # exits 125: the truncated daemon-death shape.
    seed = tmp_path / "seed.json"
    monkeypatch.setattr(Settings, "TEMP_DIR", str(tmp_path))
    Result(name="fake job", status=Result.Status.RUNNING).dump()
    os.replace(Result.file_name_static("fake job"), seed)
    script = (
        f"cp {shlex.quote(str(seed))} "
        f"{shlex.quote(Result.file_name_static('fake job'))}; "
        f"printf '%s' {shlex.quote(_DAEMON_DEATH_LOG)}; exit 125"
    )
    persisted = _run_no_docker_job(tmp_path, monkeypatch, script)
    # Membership, not equality: host-metrics labels may sit alongside ours and
    # depend on this machine's load. A dict-stored "infra" still fails this,
    # which is the property retry_infra_failures.yml's jq needs.
    assert "infra" in persisted["ext"]["labels"]
    assert persisted["status"] == Result.Status.ERROR


def test_run_does_not_label_a_completed_or_successful_run(tmp_path, monkeypatch):
    # The label belongs only to a truncated failure. A zero exit code must not
    # be labeled even when the daemon-death text is in the log tail.
    monkeypatch.setattr(Settings, "TEMP_DIR", str(tmp_path))
    job = _FakeJob()
    Result(name=job.name, status=Result.Status.RUNNING).dump()
    Runner._finalize_job_result(job, _FakeProcess(_DAEMON_DEATH_LOG), 0, None)
    with open(Result.file_name_static(job.name)) as f:
        assert "infra" not in json.load(f)["ext"].get("labels", [])
