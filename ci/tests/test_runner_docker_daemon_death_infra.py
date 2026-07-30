"""
Test for Runner._is_docker_daemon_death.

When the host docker daemon dies mid-run, the main test container returns
exit 125 and the job is truncated (all already-executed tests OK). That is a
runner-host infrastructure event, not a test or code failure, so the job
result must be labeled infra to let the auto-retry re-run it on a fresh runner
instead of reddening the merge check. This test pins the signature detection
in both directions.
"""

import os
import sys
import textwrap

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from ci.praktika.runner import Runner
from ci.praktika.result import Result

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


def test_run_still_labels_via_the_production_helper():
    # The helper above is only worth testing if _run actually calls it. Pin the
    # wiring so removing the call from _run fails here instead of silently
    # disabling the auto-retry.
    import ast
    import inspect

    tree = ast.parse(textwrap.dedent(inspect.getsource(Runner._run)))
    called = {
        node.func.attr
        for node in ast.walk(tree)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
    }
    assert "_label_infra_on_docker_daemon_death" in called
