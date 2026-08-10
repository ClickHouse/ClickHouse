"""
Tests for `install_thread_pool_fault_injection` in
`ci/jobs/scripts/stress/stress.py`.

The verify query
`SELECT value FROM system.server_settings WHERE name = 'cannot_allocate_thread_fault_injection_probability'`
runs right after `SYSTEM RELOAD CONFIG`, where a debug server under
ThreadFuzzer can be slow enough to exceed the client's 15 s
`receive_timeout`. A single such timeout used to take the whole stress job
down with "Test script failed, script exit code: 1" before any stress
testing started (seen on `Stress test (arm_debug)` in PR #110970 and
PR #114029 on 2026-08-09).

The contract this pins down:
  - a transient `TimeoutExpired` (or `CalledProcessError`) is retried and,
    on a later success, the job continues;
  - persistent failure still raises - fail-close, no silent skip of the
    fault injection;
  - a zero/empty probability after a successful reload still raises, so an
    inactive injector is never mistaken for a working one.

See https://github.com/ClickHouse/ClickHouse/pull/114063
"""

import importlib.util
import os
import subprocess

import pytest

# Load the script directly from its file: `ci/jobs/scripts/stress` is not a
# package, and putting it on `sys.path` for the whole pytest session would
# risk shadowing equally-named modules in other tests.
_STRESS_PATH = os.path.join(os.path.dirname(__file__), "..", "jobs", "scripts", "stress", "stress.py")
_spec = importlib.util.spec_from_file_location("stress_script", _STRESS_PATH)
stress = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(stress)


@pytest.fixture
def harness(monkeypatch):
    """Neutralize everything `install_thread_pool_fault_injection` does before
    the verify query: the config source exists, the symlink and the reload
    succeed, and retry backoff does not actually sleep."""
    monkeypatch.setattr(stress.os.path, "exists", lambda path: True)
    monkeypatch.setattr(stress.subprocess, "run", lambda *args, **kwargs: subprocess.CompletedProcess(args, 0))
    monkeypatch.setattr(stress, "call_with_retry", lambda *args, **kwargs: None)
    monkeypatch.setattr(stress.time, "sleep", lambda seconds: None)
    return monkeypatch


class FakeCheckOutput:
    """`check_output` stand-in replaying a canned script of outcomes."""

    def __init__(self, outcomes):
        self.outcomes = list(outcomes)
        self.calls = 0

    def __call__(self, *args, **kwargs):
        self.calls += 1
        assert self.outcomes, "check_output called more times than the test scripted"
        outcome = self.outcomes.pop(0)
        if isinstance(outcome, BaseException):
            raise outcome
        return outcome


def _timeout():
    return subprocess.TimeoutExpired(cmd="clickhouse client", timeout=15)


def _failed_client():
    return subprocess.CalledProcessError(returncode=210, cmd="clickhouse client")


def test_transient_timeout_is_retried(harness):
    fake = FakeCheckOutput([_timeout(), "0.001\n"])
    harness.setattr(stress, "check_output", fake)

    stress.install_thread_pool_fault_injection()

    assert fake.calls == 2


def test_transient_client_error_is_retried(harness):
    fake = FakeCheckOutput([_failed_client(), _timeout(), "0.001\n"])
    harness.setattr(stress, "check_output", fake)

    stress.install_thread_pool_fault_injection()

    assert fake.calls == 3


def test_persistent_timeout_still_raises(harness):
    fake = FakeCheckOutput([_timeout() for _ in range(5)])
    harness.setattr(stress, "check_output", fake)

    with pytest.raises(subprocess.TimeoutExpired):
        stress.install_thread_pool_fault_injection()

    # Exactly the retry budget: no fewer (would drop the retry) and no more
    # (would loop past it).
    assert fake.calls == 5


def test_persistent_client_error_still_raises(harness):
    fake = FakeCheckOutput([_failed_client() for _ in range(5)])
    harness.setattr(stress, "check_output", fake)

    with pytest.raises(subprocess.CalledProcessError):
        stress.install_thread_pool_fault_injection()

    assert fake.calls == 5


def test_no_retry_when_the_first_attempt_succeeds(harness):
    fake = FakeCheckOutput(["0.001\n"])
    harness.setattr(stress, "check_output", fake)

    stress.install_thread_pool_fault_injection()

    assert fake.calls == 1


@pytest.mark.parametrize("value", ["", "0", "0.0"])
def test_inactive_injector_after_reload_raises(harness, value):
    harness.setattr(stress, "check_output", FakeCheckOutput([value]))

    with pytest.raises(RuntimeError, match="cannot_allocate_thread_fault_injection_probability"):
        stress.install_thread_pool_fault_injection()


def test_missing_source_config_raises(harness):
    harness.setattr(stress.os.path, "exists", lambda path: False)
    harness.setattr(stress, "check_output", FakeCheckOutput(["0.001\n"]))

    with pytest.raises(RuntimeError, match="fault-injection source config not found"):
        stress.install_thread_pool_fault_injection()
