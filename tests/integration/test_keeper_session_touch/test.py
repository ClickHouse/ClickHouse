import threading
import time
import uuid

import pytest

import helpers.keeper_utils as keeper_utils
from helpers.cluster import ClickHouseCluster


SESSION_TIMEOUT_SECONDS = 4
ELECTION_SESSION_TIMEOUT_SECONDS = 10
SERVER_MAX_SESSION_TIMEOUT_SECONDS = 12
NODES = []

cluster = ClickHouseCluster(__file__)
for server_id in range(1, 4):
    NODES.append(
        cluster.add_instance(
            f"node{server_id}",
            main_configs=["configs/enable_keeper.xml"],
            env_variables={"KEEPER_SERVER_ID": str(server_id)},
            instance_env_variables=True,
            stay_alive=True,
        )
    )


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()
        keeper_utils.wait_nodes(cluster, NODES)
        yield cluster
    finally:
        cluster.shutdown()


def create_client(node, timeout=SESSION_TIMEOUT_SECONDS):
    return keeper_utils.get_fake_zk(
        cluster,
        node.name,
        timeout=timeout,
        retries=30,
    )


def wait_for_leader(nodes, timeout=20):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        for node in nodes:
            try:
                if keeper_utils.is_leader(cluster, node):
                    return node
            except Exception:
                pass
        time.sleep(0.1)
    raise AssertionError("Keeper leader was not elected")


def wait_until_leader(node, timeout=20):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            if keeper_utils.is_leader(cluster, node):
                return
        except Exception:
            pass
        time.sleep(0.1)
    raise AssertionError(f"{node.name} did not become Keeper leader")


def start_reading(client, path):
    stop = threading.Event()
    errors = []

    def reader():
        while not stop.is_set():
            try:
                value, _ = client.get(path)
                if value != b"alive":
                    raise AssertionError(f"Unexpected value: {value!r}")
            except BaseException as ex:
                errors.append(ex)
                return
            stop.wait(0.1)

    thread = threading.Thread(target=reader, daemon=True)
    thread.start()
    return stop, thread, errors


def assert_session_is_alive(client, initial_session_id, errors):
    assert not errors, f"Read-only session failed: {errors[0]!r}"
    assert client.client_id[0] == initial_session_id


def stop_reading(stop, thread):
    stop.set()
    thread.join(timeout=10)
    assert not thread.is_alive(), "Read request did not finish"


def test_local_reads_on_follower_touch_session_on_leader():
    follower = keeper_utils.get_any_follower(cluster, NODES)
    client = create_client(follower)
    path = f"/session_touch_{uuid.uuid4().hex}"
    initial_session_id = client.client_id[0]
    stop = thread = errors = None

    try:
        client.create(path, b"alive", ephemeral=True)
        stop, thread, errors = start_reading(client, path)

        time.sleep(SERVER_MAX_SESSION_TIMEOUT_SECONDS + SESSION_TIMEOUT_SECONDS)
        stop_reading(stop, thread)
        assert_session_is_alive(client, initial_session_id, errors)
    finally:
        if stop is not None:
            stop.set()
            thread.join(timeout=5)
        client.stop()
        client.close()


def test_local_reads_keep_session_alive_during_leader_change():
    old_leader = wait_for_leader(NODES)
    followers = [node for node in NODES if node != old_leader]
    new_leader, client_node = followers
    client = create_client(client_node, ELECTION_SESSION_TIMEOUT_SECONDS)
    path = f"/session_touch_election_{uuid.uuid4().hex}"
    initial_session_id = client.client_id[0]
    stop = thread = errors = None

    try:
        client.create(path, b"alive", ephemeral=True)
        stop, thread, errors = start_reading(client, path)

        # Let the future leader's local expiry queue become stale before the election.
        # Only the current leader receives touches from client_node at this point.
        time.sleep(ELECTION_SESSION_TIMEOUT_SECONDS * 1.5)
        response = keeper_utils.send_4lw_cmd(cluster, new_leader, "rqld")
        assert response == "Sent leadership request to leader."
        wait_until_leader(new_leader)

        time.sleep(ELECTION_SESSION_TIMEOUT_SECONDS * 2.5)
        stop_reading(stop, thread)
        assert_session_is_alive(client, initial_session_id, errors)
    finally:
        if stop is not None:
            stop.set()
            thread.join(timeout=5)
        client.stop()
        client.close()
