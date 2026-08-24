"""Pins the post-condition check ClickHouseCluster runs after a `docker compose` action.

`docker compose start zoo1 zoo2 zoo3` can exit 0, and even print `Started`, for a node it
did not act on, so the harness compares the daemon's own view of every requested node
against the requested state before logging success. Three properties make that comparison
meaningful: it reads `State.Status` rather than `State.Running`, it reads the status after
the action, and it checks the nodes it was asked about. None of them is observable from the
suites that use the seam, because every one of those drives only the success path, so a
check that silently became a no-op would keep them all green.

The methods under test are the shipped ones, bound to a stub, so these assertions track
helpers/cluster.py rather than a copy of it. No Docker and no cluster is needed.
"""

import types

import pytest  # pylint:disable=import-error; for style check

from helpers import cluster
from helpers.cluster import INTEGRATION_NODE_EXPECTED_STATUSES, ClickHouseCluster

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

    def __init__(self, states, events):
        self._states = states
        self._events = events
        # The code under test reaches a fetch as `docker_client.containers.get(...)`.
        self.containers = self

    def set(self, container, status):
        self._states[container] = _state(status)

    def get(self, container):
        self._events.append(("inspect", container))
        if container not in self._states:
            raise RuntimeError(f"stub: no such container: {container}")
        return types.SimpleNamespace(attrs={"State": self._states[container]})


class _Stub:
    """Carries only the attributes the bound methods touch."""

    check_integration_nodes_state = ClickHouseCluster.check_integration_nodes_state
    process_integration_nodes = ClickHouseCluster.process_integration_nodes
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
