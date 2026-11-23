import threading
import time
import uuid

import pytest

import helpers.keeper_utils as keeper_utils
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance(
    "node1", main_configs=["configs/enable_keeper1.xml"], stay_alive=True
)
node2 = cluster.add_instance(
    "node2", main_configs=["configs/enable_keeper2.xml"], stay_alive=True
)

WINDOW_MARKER = "Delaying the stop of the Raft asio service"
# Logged by NuRaft when Raft shuts down while a forwarded request still holds a connection.
IN_FLIGHT_MARKER = "srv [0-9]+, in-use [1-9]"
CRASH_MARKERS = [
    ("from thread [0-9]+\\).* Received signal", "clickhouse-server.log"),
    ("Segmentation fault", "clickhouse-server.log"),
    ("AddressSanitizer|ThreadSanitizer|MemorySanitizer", "clickhouse-server.err.log"),
]


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def count_in_log(node, regexp, filename="clickhouse-server.log"):
    # The logs accumulate across restarts and test repetitions, so callers compare counts.
    out = node.exec_in_container(
        [
            "bash",
            "-c",
            f"grep -acsE '{regexp}' /var/log/clickhouse-server/{filename} || true",
        ],
        user="root",
    )
    return int(out.strip() or 0)


def destroy_zk_client(zk):
    try:
        if zk:
            zk.stop()
            zk.close()
    except:
        pass


def test_follower_shutdown_with_forwarded_write_in_flight(started_cluster):
    keeper_utils.wait_nodes(cluster, [node1, node2])
    leader = keeper_utils.get_leader(cluster, [node1, node2])
    follower = node2 if leader == node1 else node1

    zk = None
    stopper = None
    try:
        zk = keeper_utils.get_fake_zk(cluster, follower.name)
        base = f"/test_forwarded_{uuid.uuid4().hex}"
        zk.create(base)

        follower.query(
            "SYSTEM ENABLE FAILPOINT keeper_shutdown_delay_before_asio_stop"
        )
        before_window = count_in_log(follower, WINDOW_MARKER)
        before_in_flight = count_in_log(follower, IN_FLIGHT_MARKER)
        before_crash = [count_in_log(follower, m, f) for m, f in CRASH_MARKERS]

        # The follower forwards the write to the frozen leader, which never answers it.
        leader.exec_in_container(["bash", "-c", "pkill -STOP clickhouse"], user="root")
        pending = zk.create_async(f"{base}/n", b"x")
        time.sleep(0.5)
        assert not pending.ready(), "the forwarded write was answered"

        stopper = threading.Thread(target=follower.stop_clickhouse)
        stopper.start()

        deadline = time.monotonic() + 60
        while count_in_log(follower, WINDOW_MARKER) == before_window:
            assert time.monotonic() < deadline, "the follower never reached the asio stop"
            time.sleep(0.1)

        # Killing the leader closes its sockets, so the forwarded request fails on an asio worker
        # of the follower while that one is shutting down.
        leader.exec_in_container(["bash", "-c", "pkill -KILL clickhouse"], user="root")

        stopper.join(timeout=120)
        assert not stopper.is_alive(), "the follower did not stop"

        assert count_in_log(follower, IN_FLIGHT_MARKER) > before_in_flight
        for (marker, filename), before in zip(CRASH_MARKERS, before_crash):
            assert (
                count_in_log(follower, marker, filename) == before
            ), f"the follower crashed: {marker} in {filename}"
        assert follower.clickhouse_last_exit_code in (None, 0)
    finally:
        destroy_zk_client(zk)
        if stopper is not None:
            stopper.join(timeout=120)
        leader.exec_in_container(
            ["bash", "-c", "pkill -KILL clickhouse"], user="root", nothrow=True
        )
        leader.start_clickhouse()
        follower.start_clickhouse()
        keeper_utils.wait_nodes(cluster, [node1, node2])
