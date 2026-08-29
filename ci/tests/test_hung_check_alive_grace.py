"""Unit tests for `HungCheckMonitor` in `tests/clickhouse-test`.

The hung check turns "the server stopped answering" into "Server died", which
aborts the whole run.  On a sanitizer build the server can stop making progress
process-wide for minutes and then recover: `Stateless tests (amd_tsan,
parallel)` on master @ `756b18a3921e` reported `Server died` with `Failed: 0,
Passed: 10925` after two ~2-minute whole-process stalls, and the server answered
again 89 s after the verdict and then shut down cleanly.

Report:
https://s3.amazonaws.com/clickhouse-test-reports/json.html?REF=master&sha=756b18a3921e9a5daec71d5c4413bfdad78a8275&name_0=MasterCI&name_1=Stateless%20tests%20%28amd_tsan%2C%20parallel%29

So the two evidence classes must stay apart: a server that is gone aborts on
the first failed probe, a server that is still there gets a bounded grace.
"Still there" means the server behind the probed port, not any visible
`clickhouse-server`, so the fixture turns a `get_server_pid` call into a
failure.  The monitor tests drive the real class on a fake clock with the probe
and the process lookup stubbed out; the lookup itself is tested against a real
listening socket.
"""

import importlib.machinery
import importlib.util
import os
import socket
from argparse import Namespace
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
    state = {
        "alive": True,
        # (alive, pid, socket inodes) as reported for the server behind the
        # probed port.
        "process": (True, 588, frozenset({"4242"})),
        "probe_cost": 0.0,
        "probes": [],
        "process_reads": 0,
    }

    def fake_check_server_liveness(_args, max_retries=10):
        state["probes"].append(max_retries)
        clock.advance(state["probe_cost"])
        return state["alive"]

    def fake_probed_server_process(_args):
        state["process_reads"] += 1
        return state["process"]

    def unexpected_get_server_pid(_args):
        raise AssertionError(
            "the grace must be keyed off the probed port, not off any visible "
            "clickhouse-server process"
        )

    monkeypatch.setattr(_ct, "time", clock)
    monkeypatch.setattr(_ct, "check_server_liveness", fake_check_server_liveness)
    monkeypatch.setattr(_ct, "probed_server_process", fake_probed_server_process)
    monkeypatch.setattr(_ct, "get_server_pid", unexpected_get_server_pid)

    state["clock"] = clock
    state["monitor"] = _ct.HungCheckMonitor(args=Namespace(http_port=8123))
    return state


def test_healthy_server_is_never_hung_and_keeps_the_full_probe_budget(env):
    """While the server answers, each probe keeps its full retry budget, and
    the probes are interval-gated so the caller loop's own 10 Hz cadence does
    not hammer the server."""
    for _ in range(101):  # 10 s of the caller loop's own cadence
        assert env["monitor"].is_hung() is False
        env["clock"].advance(0.1)
    assert env["probes"] == [10, 10]


def test_dead_process_aborts_on_the_first_failed_probe(env):
    """No live process - the pre-existing latency, no grace."""
    env["alive"] = False
    env["process"] = (False, None, None)
    env["probe_cost"] = 165.0  # the full retry budget of a real failing probe
    assert env["monitor"].is_hung() is True
    assert env["probes"] == [10]


def test_unknown_liveness_reads_as_dead(env):
    """No `/proc`, or a server on another host, must not buy a grace."""
    env["alive"] = False
    env["process"] = (None, None, None)
    assert env["monitor"].is_hung() is True


def test_a_surviving_replica_does_not_buy_a_grace_for_the_probed_server(env):
    """The multi-replica harness keeps other servers on `18123` / `28123`.

    Nothing listens on the probed port any more, so the main server is gone and
    the run must abort at the old latency even though `clickhouse-server`
    processes are still visible - the fixture makes any use of `get_server_pid`
    an error.
    """
    env["alive"] = False
    env["process"] = (False, None, None)
    assert env["monitor"].is_hung() is True


def test_probed_server_process_follows_the_listening_socket():
    """The real helper, against a real socket - no `/proc` stubbing."""
    if not os.path.exists("/proc/net/tcp"):
        pytest.skip("needs Linux /proc")

    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        listener.bind(("127.0.0.1", 0))
        listener.listen(1)
        port = listener.getsockname()[1]
        args = Namespace(tcp_host="localhost", http_port=port)
        alive, pid, inodes = _ct.probed_server_process(args)
        assert (alive, pid) == (True, os.getpid())
        assert inodes, "a held port must come with the sockets holding it"
        # A server on another host cannot be judged from here.
        assert _ct.probed_server_process(
            Namespace(tcp_host="some-other-host", http_port=port)
        ) == (None, None, None)
    finally:
        listener.close()
    assert _ct.probed_server_process(args) == (False, None, None)


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


def test_a_server_replaced_on_the_same_port_mid_grace_still_fails_fast(env):
    """Stall, grace, watchdog restart re-binding the same port: the fresh
    listener holds the port, but it is not the server the grace was granted
    for, so the run must abort at the next probe instead of sitting out the
    rest of the grace on a server that lost all its state."""
    env["alive"] = False
    env["probe_cost"] = 165.0
    assert env["monitor"].is_hung() is False  # grace granted, pinned to "4242"

    env["probe_cost"] = 10.0
    env["process"] = (True, 999, frozenset({"7777"}))  # the replacement
    env["clock"].advance(_ct.HungCheckMonitor.RETRY_INTERVAL)
    assert env["monitor"].is_hung() is True


def test_a_server_replaced_inside_the_first_failed_probe_still_fails_fast(env):
    """The swap happens inside the first failed probe's own ~165 s retry
    window, before anything was pinned.  The port is held again by the time it
    is inspected, but not by the server the tests were talking to - a
    still-silent replacement must not inherit the grace."""
    assert env["monitor"].is_hung() is False  # the original is seen answering

    env["alive"] = False
    env["probe_cost"] = 165.0
    env["process"] = (True, 999, frozenset({"7777"}))  # re-bound mid-probe
    env["clock"].advance(_ct.HungCheckMonitor.RETRY_INTERVAL)
    assert env["monitor"].is_hung() is True
    assert env["probes"] == [10, 10]


def test_the_answering_identity_is_captured_with_every_answer(env):
    """The listener identity is read together with the probe it belongs to.

    No more often - the probes are interval-gated, so /proc/net is not read per
    0.1 s caller turn - and no less often: an identity promoted on a schedule
    of its own could lag a stale interval behind the listener that actually
    answered."""
    for _ in range(305):  # ~30 s of the caller loop's own cadence
        assert env["monitor"].is_hung() is False
        env["clock"].advance(0.1)
    # One /proc read per answered probe: 4 probes in ~30 s, not one per call.
    assert env["process_reads"] == len(env["probes"]) == 4


def test_a_restart_answering_before_the_next_refresh_still_earns_the_grace(env):
    """A restart that answered a probe must be promoted with that very answer.

    With the identity promoted on a lagging schedule instead, the recorded
    listener stays the old one for up to an interval, and a replacement that
    answers and then stalls inside that window reads as an unseen swap on the
    next failed probe - aborting the run at the old latency for a server that
    was seen answering."""
    assert env["monitor"].is_hung() is False  # the original is seen answering

    env["process"] = (True, 999, frozenset({"7777"}))  # restart 5 s later
    env["clock"].advance(5.0)

    # Drive the caller loop until the replacement has answered one probe.
    probes = len(env["probes"])
    while len(env["probes"]) == probes:
        assert env["monitor"].is_hung() is False
        env["clock"].advance(0.1)

    # ... and it stalls right after that answer.
    env["alive"] = False
    env["probe_cost"] = 165.0
    probes, verdicts = len(env["probes"]), []
    while len(env["probes"]) == probes:
        verdicts.append(env["monitor"].is_hung())
        env["clock"].advance(0.1)
    # The failed probe grants the grace instead of reporting a swap.
    assert verdicts[-1] is False
    assert env["monitor"].grace_holder_inodes == frozenset({"7777"})


def test_a_replacement_server_answering_does_not_resume_the_run(env):
    """The same swap, but the replacement is already up and answers the probe.

    Clearing the stall here would silently continue the run on a new server,
    hiding both the original's death and everything the restart lost - the
    recovery path must verify it is the *same* server that recovered."""
    env["alive"] = False
    env["probe_cost"] = 165.0
    assert env["monitor"].is_hung() is False  # grace granted, pinned to "4242"

    env["probe_cost"] = 0.0
    env["alive"] = True
    env["process"] = (True, 999, frozenset({"7777"}))  # the replacement
    env["clock"].advance(_ct.HungCheckMonitor.RETRY_INTERVAL)
    assert env["monitor"].is_hung() is True


def test_the_same_server_recovering_clears_the_pinned_identity(env):
    """After a genuine recovery the next stall pins afresh: the identity from a
    past grace must not leak into the next one."""
    env["alive"] = False
    assert env["monitor"].is_hung() is False
    assert env["monitor"].grace_holder_inodes == frozenset({"4242"})

    env["alive"] = True
    env["clock"].advance(_ct.HungCheckMonitor.RETRY_INTERVAL)
    assert env["monitor"].is_hung() is False
    assert env["monitor"].grace_holder_inodes is None

    # A restart that was then seen answering is a new server legitimately
    # starting a new grace, not a replacement caught mid-grace.
    env["process"] = (True, 999, frozenset({"7777"}))
    env["clock"].advance(_ct.HungCheckMonitor.RETRY_INTERVAL)
    assert env["monitor"].is_hung() is False  # the replacement answers
    env["alive"] = False
    env["clock"].advance(_ct.HungCheckMonitor.RETRY_INTERVAL)
    assert env["monitor"].is_hung() is False
    assert env["monitor"].grace_holder_inodes == frozenset({"7777"})


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
