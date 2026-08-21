"""Unit tests for `HungCheckMonitor` in `tests/clickhouse-test`.

The hung check turns "the server stopped answering" into "Server died", which
aborts the whole run.  On a sanitizer build the server can stop making progress
process-wide for minutes and then recover: `Stateless tests (amd_tsan,
parallel)` on master @ `756b18a3921e` reported `Server died` with `Failed: 0,
Passed: 10925` after two ~2-minute whole-process stalls, and the server answered
again 89 s after the verdict and then shut down cleanly.

Report:
https://s3.amazonaws.com/clickhouse-test-reports/json.html?REF=master&sha=756b18a3921e9a5daec71d5c4413bfdad78a8275&name_0=MasterCI&name_1=Stateless%20tests%20%28amd_tsan%2C%20parallel%29

So the two evidence classes must stay apart: a process that is gone aborts on
the first failed probe, a process that is still there gets a bounded grace.
These tests drive the real class on a fake clock with the probe and the PID
lookup stubbed out; no server and no `ps` are involved.
"""

import importlib.machinery
import importlib.util
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parent.parent.parent
_CLICKHOUSE_TEST = str(_REPO_ROOT / "tests" / "clickhouse-test")


def _load_clickhouse_test():
    """Import `tests/clickhouse-test` as a module.

    Not `runpy.run_path`: that returns a *copy* of the globals, so replacing a
    name in it would not be seen by the functions defined there.  A real module
    object lets the tests swap `check_server_liveness` and `get_server_pid`.
    """
    loader = importlib.machinery.SourceFileLoader("clickhouse_test", _CLICKHOUSE_TEST)
    spec = importlib.util.spec_from_loader(loader.name, loader)
    module = importlib.util.module_from_spec(spec)
    loader.exec_module(module)
    return module


_ct = _load_clickhouse_test()


class FakeClock:
    """Monotonic clock the test advances by hand."""

    def __init__(self):
        self.now = 1000.0

    def __call__(self):
        return self.now

    def advance(self, seconds):
        self.now += seconds


@pytest.fixture(name="env")
def _env(monkeypatch):
    """A monitor wired to a fake clock, a scripted probe and a scripted PID.

    `probe_cost` models the wall time `check_server_liveness` burns before
    answering, so a failing probe advances the clock the way the real one does.
    """
    clock = FakeClock()
    state = {"alive": True, "pid": 588, "probe_cost": 0.0, "probes": []}

    def fake_check_server_liveness(_args, max_retries=10):
        state["probes"].append(max_retries)
        clock.advance(state["probe_cost"])
        return state["alive"]

    def fake_get_server_pid(_args):
        return state["pid"]

    monkeypatch.setattr(_ct, "time", clock)
    monkeypatch.setattr(_ct, "check_server_liveness", fake_check_server_liveness)
    monkeypatch.setattr(_ct, "get_server_pid", fake_get_server_pid)

    state["clock"] = clock
    state["monitor"] = _ct.HungCheckMonitor(args=None)
    return state


def test_healthy_server_is_never_hung_and_keeps_the_full_probe_budget(env):
    """While the server answers, nothing changes: one full-budget probe per call."""
    for _ in range(5):
        assert env["monitor"].is_hung() is False
        env["clock"].advance(0.1)
    assert env["probes"] == [10] * 5


def test_dead_process_aborts_on_the_first_failed_probe(env):
    """No live process - the pre-existing latency, no grace."""
    env["alive"] = False
    env["pid"] = None
    env["probe_cost"] = 165.0  # the full retry budget of a real failing probe
    assert env["monitor"].is_hung() is True
    assert env["probes"] == [10]


def test_unknown_pid_reads_as_dead(env):
    """`ps` unavailable or a server on another host must not buy a grace."""
    env["alive"] = False
    env["pid"] = None
    assert env["monitor"].is_hung() is True


def test_live_process_stalled_then_recovering_does_not_abort(env):
    """The reported failure: a ~285 s whole-process stall that recovers.

    Master aborts here as soon as the first probe exhausts its ~165 s budget.
    """
    env["alive"] = False
    env["probe_cost"] = 165.0
    assert env["monitor"].is_hung() is False  # t=1165, silent for 165 s

    # Keep probing while the server stays silent; each re-probe is a single
    # attempt, so it costs one socket timeout instead of the full budget.
    env["probe_cost"] = 10.0
    for _ in range(6):
        env["clock"].advance(_ct.HungCheckMonitor.RETRY_INTERVAL)
        assert env["monitor"].is_hung() is False
    assert env["probes"] == [10] + [1] * 6

    env["alive"] = True
    env["clock"].advance(_ct.HungCheckMonitor.RETRY_INTERVAL)
    assert env["monitor"].is_hung() is False
    assert env["monitor"].unresponsive_since is None

    # And the next stall starts from scratch, with the full budget again.
    env["alive"] = False
    env["clock"].advance(_ct.HungCheckMonitor.RETRY_INTERVAL)
    assert env["monitor"].is_hung() is False
    assert env["probes"][-1] == 10


def test_live_process_silent_past_the_grace_is_a_hang(env):
    """A real deadlock is still reported - later, but reported."""
    env["alive"] = False
    env["probe_cost"] = 165.0
    assert env["monitor"].is_hung() is False

    env["probe_cost"] = 10.0
    verdict = False
    for _ in range(200):
        env["clock"].advance(_ct.HungCheckMonitor.RETRY_INTERVAL)
        verdict = env["monitor"].is_hung()
        if verdict:
            break
    assert verdict is True
    silent_for = env["clock"].now - env["monitor"].unresponsive_since
    assert silent_for >= _ct.HungCheckMonitor.GRACE_WITH_LIVE_PROCESS


def test_probes_inside_the_grace_are_interval_gated(env):
    """Without the gate an instantly-refusing socket would be probed at 10 Hz."""
    env["alive"] = False
    assert env["monitor"].is_hung() is False
    probes_after_first = len(env["probes"])

    for _ in range(100):  # the caller loop's own cadence
        env["clock"].advance(0.1)
        assert env["monitor"].is_hung() is False
    # 10 s of fake time at 0.1 s per turn buys exactly one extra probe.
    assert len(env["probes"]) == probes_after_first + 1
