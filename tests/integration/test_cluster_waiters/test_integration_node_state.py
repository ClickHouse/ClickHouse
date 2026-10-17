"""Pins the post-condition check, and the timeout diagnostic, around a ZooKeeper restart.

`docker compose start zoo1 zoo2 zoo3` can exit 0, and even print `Started`, for a node it
did not act on, so the harness compares the daemon's own view of every requested node
against the requested state before logging success. Three properties make that comparison
meaningful: it reads `State.Status` rather than `State.Running`, it reads the status after
the action, and it checks the nodes it was asked about. When a node is nevertheless left
behind, the ZooKeeper waiter is what a reporter reads, so it has to name the node that
blocked, its container state and the elapsed time instead of guessing at a cause.

None of that is observable from the suites that use these seams, because every one of them
drives only the success path: a check that silently became a no-op, or a diagnostic that
lost the node it names, would keep them all green.

The methods under test are the shipped ones, bound to a stub, so these assertions track
helpers/cluster.py rather than a copy of it. No Docker and no cluster is needed.
"""

import types

import pytest  # pylint:disable=import-error; for style check

from helpers import cluster
from helpers.cluster import (
    CONTAINER_DESCRIBE_TIMEOUT,
    CONTAINER_STATE_CHECK_TIMEOUT,
    INTEGRATION_NODE_EXPECTED_STATUSES,
    ClickHouseCluster,
)

# The timeout the shipped shared client is built with, which is sized for image pulls.
SHARED_CLIENT_TIMEOUT = 600

# Statuses whose `State.Running` is true. It covers three of them, so `Running` cannot
# distinguish a started container from a paused or restarting one.
RUNNING_STATUSES = ("running", "paused", "restarting")

ALL_STATUSES = ("running", "exited", "paused", "restarting", "dead", "created")


def _state(status):
    """Build a `State` block. `status` is a status, or a (status, exit code) pair."""
    status, exit_code = status if isinstance(status, tuple) else (status, 0)
    return {
        "Status": status,
        "Running": status in RUNNING_STATUSES,
        "ExitCode": exit_code,
        "OOMKilled": False,
    }


class _FakeDocker:
    """Records every container fetch, so which node was inspected, and when, is observable.

    A fetch returns the status block held at fetch time, as a real handle does: one taken
    before an action keeps reporting the state from before it.
    """

    def __init__(self, states, events, ips=None):
        self._states = states
        self._events = events
        self._ips = ips or {}
        # The code under test reaches a fetch as `docker_client.containers.get(...)`.
        self.containers = self
        # docker-py reads this attribute per request, so the value held at fetch time is
        # the one that request would have waited on.
        self.api = types.SimpleNamespace(timeout=SHARED_CLIENT_TIMEOUT)
        self.fetch_timeouts = []

    def set(self, container, status):
        self._states[container] = _state(status)

    def get(self, container):
        self._events.append(("inspect", container))
        # One arm removes the request layer, so this bookkeeping must not be what fails.
        api = getattr(self, "api", None)
        self.fetch_timeouts.append(getattr(api, "timeout", None))
        if container not in self._states:
            raise RuntimeError(f"stub: no such container: {container}")
        ip = self._ips.get(container, "")
        # get_instance_ip reads the first network's address, so the shape matters as much
        # as the value. `None` stands for the empty map docker leaves behind on a container
        # that is no longer attached to a network, where the lookup itself raises.
        networks = {} if ip is None else {"stubnet": {"IPAddress": ip}}
        return types.SimpleNamespace(
            attrs={
                "State": self._states[container],
                "NetworkSettings": {"Networks": networks},
            }
        )


class _Stub:
    """Carries only the attributes the bound methods touch."""

    check_integration_nodes_state = ClickHouseCluster.check_integration_nodes_state
    process_integration_nodes = ClickHouseCluster.process_integration_nodes
    bounded_docker_requests = ClickHouseCluster.bounded_docker_requests
    get_instance_docker_id = ClickHouseCluster.get_instance_docker_id

    def __init__(self, statuses, project="stubproject"):
        # Names a container on no daemon, so nothing here is shared between arms.
        self.project_name = project
        self.events = []
        self.docker_client = _FakeDocker(
            {self.container(name): _state(status) for name, status in statuses.items()},
            self.events,
        )
        self.base_zookeeper_cmd = ["docker", "compose", "--project-name", project]

    def container(self, name):
        return self.get_instance_docker_id(name)

    def inspected(self):
        return [name for kind, name in self.events if kind == "inspect"]


def _stub_compose(stub, monkeypatch, becomes):
    """Replace the compose call with one that records itself and applies `becomes`: the
    statuses the daemon holds once the action has run. A node absent from `becomes` is one
    compose reported success for without acting on it."""

    def fake_call(args, **kwargs):
        stub.events.append(("compose", list(args)))
        for name, status in becomes.items():
            stub.docker_client.set(stub.container(name), status)
        return ""

    monkeypatch.setattr(cluster, "subprocess_check_call", fake_call)


def test_start_names_the_node_compose_left_behind():
    """The failure a reporter has to act on: which node, which container, what state it is
    in instead, and what compose claimed."""
    stub = _Stub({"zoo1": "running", "zoo2": "running", "zoo3": ("exited", 7)})
    with pytest.raises(Exception) as excinfo:
        stub.check_integration_nodes_state(
            "zookeeper", ["zoo1", "zoo2", "zoo3"], "start"
        )

    message = str(excinfo.value)
    for token in ("zoo3", stub.container("zoo3"), "exited", "running", "exit code 7"):
        assert token in message, message


def test_requested_nodes_in_the_expected_state_pass_silently():
    """Negative control: the check adds no failure of its own, and inspects exactly the
    nodes it was given."""
    stub = _Stub({"zoo1": "running", "zoo2": "running", "zoo3": "running"})
    stub.check_integration_nodes_state("zookeeper", ["zoo1", "zoo2", "zoo3"], "start")
    assert stub.inspected() == [stub.container(n) for n in ("zoo1", "zoo2", "zoo3")]


@pytest.mark.parametrize("status", ("paused", "restarting"))
def test_start_rejects_a_status_that_is_running_but_not_started(status):
    """A container in either status reports `Running` true while nothing in it is
    reachable, so only the status distinguishes it from a started one."""
    assert _state(status)["Running"] is True
    assert status not in INTEGRATION_NODE_EXPECTED_STATUSES["start"]

    stub = _Stub({"zoo1": status})
    with pytest.raises(Exception) as excinfo:
        stub.check_integration_nodes_state("zookeeper", ["zoo1"], "start")
    assert status in str(excinfo.value)


def test_status_is_read_after_the_action(monkeypatch):
    """A node that was down and did start must pass, and a node compose skipped must fail.
    Both hold only if the status compared is the one the action left behind."""
    started = _Stub({"zoo1": "exited"})
    _stub_compose(started, monkeypatch, becomes={"zoo1": "running"})
    started.process_integration_nodes("zookeeper", ["zoo1"], "start")
    assert [kind for kind, _ in started.events] == [
        "compose",
        "inspect",
    ], started.events

    skipped = _Stub({"zoo1": "exited"})
    _stub_compose(skipped, monkeypatch, becomes={})
    with pytest.raises(Exception) as excinfo:
        skipped.process_integration_nodes("zookeeper", ["zoo1"], "start")
    assert "exited" in str(excinfo.value)


def test_only_the_requested_nodes_are_checked():
    """Several suites start a subset and leave the rest down on purpose, so a node outside
    the request is not this call's business - while the same node inside it is."""
    statuses = {"zoo1": "exited", "zoo2": "running", "zoo3": "running"}

    subset = _Stub(statuses)
    subset.check_integration_nodes_state("zookeeper", ["zoo2", "zoo3"], "start")
    assert subset.inspected() == [subset.container(n) for n in ("zoo2", "zoo3")]

    requested = _Stub(statuses)
    with pytest.raises(Exception) as excinfo:
        requested.check_integration_nodes_state("zookeeper", ["zoo1"], "start")
    assert "zoo1" in str(excinfo.value)


def test_every_checking_fetch_is_bounded_below_the_shared_client_timeout():
    """The check runs on the main path, so an unresponsive daemon would hold it for the
    shared client's timeout once per node, and three of those outlive the per-test timeout
    that would otherwise report the failure."""
    assert CONTAINER_STATE_CHECK_TIMEOUT < SHARED_CLIENT_TIMEOUT

    stub = _Stub({"zoo1": "running", "zoo2": "running", "zoo3": "running"})
    stub.check_integration_nodes_state("zookeeper", ["zoo1", "zoo2", "zoo3"], "start")

    # Per node, not once for the loop: a bound taken outside it would leave every node
    # after the first waiting on whatever the previous iteration restored.
    assert stub.docker_client.fetch_timeouts == [CONTAINER_STATE_CHECK_TIMEOUT] * 3


def test_the_checking_bound_is_wider_than_the_describing_one():
    """The two calls are bounded for opposite reasons: this one decides whether a test
    passes, so expiring early would fail a run a slow daemon would have completed, while
    describing only shapes the text of a failure that already happened."""
    assert CONTAINER_STATE_CHECK_TIMEOUT > CONTAINER_DESCRIBE_TIMEOUT


def test_the_shared_client_timeout_is_restored_after_checking():
    """The client is shared with every other caller, so a lowered timeout that outlived the
    check would silently shorten unrelated calls, including image pulls."""
    passing = _Stub({"zoo1": "running"})
    passing.check_integration_nodes_state("zookeeper", ["zoo1"], "start")
    assert passing.docker_client.api.timeout == SHARED_CLIENT_TIMEOUT

    # Restored on both failing paths: a node in the wrong state, and one that cannot be
    # inspected at all, which is the path that raises from inside the bound.
    wrong_state = _Stub({"zoo1": "exited"})
    with pytest.raises(Exception):
        wrong_state.check_integration_nodes_state("zookeeper", ["zoo1"], "start")
    assert wrong_state.docker_client.api.timeout == SHARED_CLIENT_TIMEOUT

    vanished = _Stub({})
    with pytest.raises(Exception):
        vanished.check_integration_nodes_state("zookeeper", ["zoo1"], "start")
    assert vanished.docker_client.api.timeout == SHARED_CLIENT_TIMEOUT


def test_a_client_without_a_request_layer_is_checked_anyway():
    """Negative control on the bound itself: it must not become the thing that breaks the
    check. A stub client carrying no `api` is still compared against its status."""
    stub = _Stub({"zoo1": "exited"})
    del stub.docker_client.api

    with pytest.raises(Exception) as excinfo:
        stub.check_integration_nodes_state("zookeeper", ["zoo1"], "start")
    assert "exited" in str(excinfo.value)


def test_each_action_accepts_only_the_statuses_declared_for_it():
    """The accepted sets are spelled out here rather than read from the table alone, so
    that widening the table is a change these arms notice."""
    accepted = {
        "start": ("running",),
        "stop": ("exited", "dead", "created"),
        "kill": ("exited", "dead", "created"),
    }
    assert INTEGRATION_NODE_EXPECTED_STATUSES == accepted

    for action, statuses in accepted.items():
        for status in statuses:
            passing = _Stub({"zoo1": status})
            passing.check_integration_nodes_state("zookeeper", ["zoo1"], action)
        for status in (s for s in ALL_STATUSES if s not in statuses):
            failing = _Stub({"zoo1": status})
            with pytest.raises(Exception):
                failing.check_integration_nodes_state("zookeeper", ["zoo1"], action)

    # An action with no declared post-condition keeps its previous behaviour: it is not
    # asserted at all, so it does not even inspect.
    other = _Stub({"zoo1": "exited"})
    other.check_integration_nodes_state("zookeeper", ["zoo1"], "restart")
    assert other.events == []


# --- the ZooKeeper waiter's timeout diagnostic --------------------------------------

# Small enough to spell out in the assertions below, and the clock is faked, so the whole
# budget elapses within one arm.
TIMEOUT = 5.0


class _FakeClock:
    """Advances only when the code under test sleeps, so arms are exact and instant.

    Reads are capped, so a loop that neither sleeps nor exits fails here rather than
    running until pytest's own timeout.
    """

    _MAX_READS = 100000

    def __init__(self):
        self.now = 0.0
        self.reads = 0

    def time(self):
        self.reads += 1
        if self.reads > self._MAX_READS:
            raise AssertionError(
                f"clock read over {self._MAX_READS} times without advancing: "
                "the waiter is spinning without sleeping or exiting"
            )
        return self.now

    def sleep(self, seconds):
        self.now += seconds


class _WaiterStub:
    """Carries only the attributes the waiter and its diagnostic touch."""

    wait_zookeeper_nodes_to_start = ClickHouseCluster.wait_zookeeper_nodes_to_start
    describe_container_state = ClickHouseCluster.describe_container_state
    bounded_docker_requests = ClickHouseCluster.bounded_docker_requests
    get_instance_ip = ClickHouseCluster.get_instance_ip
    get_instance_docker_id = ClickHouseCluster.get_instance_docker_id

    def __init__(self, statuses, ips=None, unreachable=(), project="stubproject"):
        # Names containers on no daemon, so nothing here is shared between arms.
        self.project_name = project
        self.events = []
        self._unreachable = set(unreachable)
        self.docker_client = _FakeDocker(
            {self.container(name): _state(status) for name, status in statuses.items()},
            self.events,
            {self.container(name): ip for name, ip in (ips or {}).items()},
        )

    def container(self, name):
        return self.get_instance_docker_id(name)

    def contacted(self):
        return [name for kind, name in self.events if kind == "kazoo"]

    def get_kazoo_client(self, zoo_instance_name, timeout=30.0, retries=10):
        # Matches the keywords the waiter passes; a mismatch would fail the bind instead
        # of the assertion.
        assert (timeout, retries) == (5.0, 1)
        self.events.append(("kazoo", zoo_instance_name))
        if zoo_instance_name in self._unreachable:
            # What kazoo raises for a container with no address, which is the shape the
            # reported CI failure took.
            raise ValueError("bad hostname")
        return types.SimpleNamespace(get_children=lambda path: [], stop=lambda: None)


def _fake_clock(monkeypatch):
    clock = _FakeClock()
    monkeypatch.setattr(cluster, "time", clock)
    return clock


def test_the_timeout_reports_what_a_reporter_has_to_act_on(monkeypatch):
    """A node compose left behind is unreachable for the whole budget. The message has to
    carry the node, its container, the state it is in and how long was spent, because the
    alternative is a reader guessing at host networking."""
    _fake_clock(monkeypatch)
    stub = _WaiterStub(
        {"zoo1": "running", "zoo2": "running", "zoo3": ("exited", 137)},
        ips={"zoo1": "172.16.0.2", "zoo2": "172.16.0.3", "zoo3": ""},
        unreachable=["zoo3"],
    )
    with pytest.raises(Exception) as excinfo:
        stub.wait_zookeeper_nodes_to_start(["zoo1", "zoo2", "zoo3"], timeout=TIMEOUT)

    message = str(excinfo.value)
    for token in (
        "zoo3",
        stub.container("zoo3"),
        "status exited",
        "exit code 137",
        # An address the daemon never assigned is the tell that the container is down.
        "<empty>",
        f"after {TIMEOUT:.1f}s of {TIMEOUT}s",
    ):
        assert token in message, message


def test_the_named_node_is_the_one_that_blocked(monkeypatch):
    """With a healthy node on either side of the failing one, the message must name the
    middle node: neither the first of the request nor the last one tried."""
    _fake_clock(monkeypatch)
    stub = _WaiterStub(
        {"zoo1": "running", "zoo2": ("exited", 137), "zoo3": "running"},
        ips={"zoo1": "172.16.0.2", "zoo2": "", "zoo3": "172.16.0.4"},
        unreachable=["zoo2"],
    )
    with pytest.raises(Exception) as excinfo:
        stub.wait_zookeeper_nodes_to_start(["zoo1", "zoo2", "zoo3"], timeout=TIMEOUT)

    message = str(excinfo.value)
    assert "zoo2" in message, message
    assert "zoo1" not in message and "zoo3" not in message, message


def test_only_the_requested_nodes_are_waited_for(monkeypatch):
    """Several suites restart a subset and leave the rest down on purpose, so a node
    outside the request is neither contacted nor blamed - even when it is the one that is
    actually unreachable."""
    _fake_clock(monkeypatch)
    stub = _WaiterStub(
        {"zoo1": ("exited", 137), "zoo2": "running", "zoo3": ("exited", 137)},
        ips={"zoo1": "", "zoo2": "172.16.0.3", "zoo3": ""},
        unreachable=["zoo1", "zoo3"],
    )
    with pytest.raises(Exception) as excinfo:
        stub.wait_zookeeper_nodes_to_start(["zoo2", "zoo3"], timeout=TIMEOUT)

    message = str(excinfo.value)
    assert "zoo3" in message, message
    assert "zoo1" not in message, message
    assert "zoo1" not in stub.contacted(), stub.contacted()


def test_a_vanished_container_is_described_rather_than_masked(monkeypatch):
    """The diagnostic runs while a failure is already being reported, so a lookup of its
    own that fails must not become the exception the reader sees. Both fail here."""
    _fake_clock(monkeypatch)
    stub = _WaiterStub({}, unreachable=["zoo1"])
    with pytest.raises(Exception) as excinfo:
        stub.wait_zookeeper_nodes_to_start(["zoo1"], timeout=TIMEOUT)

    message = str(excinfo.value)
    assert "Cannot connect to ZooKeeper node zoo1" in message, message
    assert "status unavailable" in message, message
    assert "<unavailable:" in message, message


def test_a_container_with_no_address_is_described_rather_than_masked(monkeypatch):
    """The second lookup fails on its own: the container is still there to inspect, but it
    is attached to no network, so reading its address raises."""
    _fake_clock(monkeypatch)
    stub = _WaiterStub(
        {"zoo1": ("exited", 137)}, ips={"zoo1": None}, unreachable=["zoo1"]
    )
    with pytest.raises(Exception) as excinfo:
        stub.wait_zookeeper_nodes_to_start(["zoo1"], timeout=TIMEOUT)

    message = str(excinfo.value)
    assert "Cannot connect to ZooKeeper node zoo1" in message, message
    assert "status exited, exit code 137" in message, message
    assert "<unavailable:" in message, message


def test_reachable_nodes_return_without_spending_the_budget(monkeypatch):
    """Negative control: without this arm every assertion above is satisfied by a waiter
    that raises unconditionally."""
    clock = _fake_clock(monkeypatch)
    stub = _WaiterStub(
        {"zoo1": "running", "zoo2": "running", "zoo3": "running"},
        ips={"zoo1": "172.16.0.2", "zoo2": "172.16.0.3", "zoo3": "172.16.0.4"},
    )
    stub.wait_zookeeper_nodes_to_start(["zoo1", "zoo2", "zoo3"], timeout=TIMEOUT)

    assert stub.contacted() == ["zoo1", "zoo2", "zoo3"]
    assert clock.now == 0.0


def test_every_describing_fetch_is_bounded_below_the_shared_client_timeout(monkeypatch):
    """The diagnostic runs after a budget is already spent, so each of its fetches has to
    carry a timeout of its own: on the shared client's, a wedged daemon would hold the
    caller for minutes past the deadline it advertises."""
    assert CONTAINER_DESCRIBE_TIMEOUT < SHARED_CLIENT_TIMEOUT

    _fake_clock(monkeypatch)
    stub = _WaiterStub(
        {"zoo1": ("exited", 137)}, ips={"zoo1": ""}, unreachable=["zoo1"]
    )
    with pytest.raises(Exception):
        stub.wait_zookeeper_nodes_to_start(["zoo1"], timeout=TIMEOUT)

    # Both fetches: the state lookup and the address lookup behind it.
    assert stub.docker_client.fetch_timeouts == [CONTAINER_DESCRIBE_TIMEOUT] * 2


def test_the_shared_client_timeout_is_restored_after_describing():
    """The client is shared with every other caller, so a lowered timeout that outlived the
    diagnostic would silently shorten unrelated calls, including image pulls."""
    stub = _WaiterStub({"zoo1": ("exited", 137)}, ips={"zoo1": ""})
    stub.describe_container_state("zoo1")
    assert stub.docker_client.api.timeout == SHARED_CLIENT_TIMEOUT

    # Restored on the failing path too, which is the only path this runs on in practice.
    vanished = _WaiterStub({})
    vanished.describe_container_state("zoo1")
    assert vanished.docker_client.api.timeout == SHARED_CLIENT_TIMEOUT


def test_a_client_without_a_request_layer_is_described_anyway():
    """Negative control on the bound itself: it must not become the thing that breaks the
    diagnostic. A stub client carrying no `api` still gets described."""
    stub = _WaiterStub({"zoo1": ("exited", 137)}, ips={"zoo1": ""})
    del stub.docker_client.api

    described = stub.describe_container_state("zoo1")
    assert "status exited, exit code 137" in described, described
