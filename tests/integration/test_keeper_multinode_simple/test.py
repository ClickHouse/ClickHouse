import time

import pytest

import helpers.keeper_utils as keeper_utils
from helpers.cluster import ClickHouseCluster
from helpers.network import PartitionManager
from helpers.test_tools import assert_eq_with_retry

import uuid

cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance(
    "node1",
    main_configs=["configs/enable_keeper1.xml", "configs/use_keeper.xml"],
    stay_alive=True,
)
node2 = cluster.add_instance(
    "node2",
    main_configs=["configs/enable_keeper2.xml", "configs/use_keeper.xml"],
    stay_alive=True,
)
node3 = cluster.add_instance(
    "node3",
    main_configs=["configs/enable_keeper3.xml", "configs/use_keeper.xml"],
    stay_alive=True,
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        yield cluster

    finally:
        cluster.shutdown()


def smaller_exception(ex):
    return "\n".join(str(ex).split("\n")[0:2])


def wait_nodes():
    keeper_utils.wait_nodes(cluster, [node1, node2, node3])


def get_fake_zk(nodename, timeout=30.0):
    return keeper_utils.get_fake_zk(cluster, nodename, timeout=timeout)


def test_read_write_multinode(started_cluster):
    try:
        wait_nodes()
        node1_zk = get_fake_zk("node1")
        node2_zk = get_fake_zk("node2")
        node3_zk = get_fake_zk("node3")

        # Cleanup
        for i in range(1, 4):
            if node1_zk.exists(f"/test_read_write_multinode_node{i}") != None:
                node1_zk.delete(f"/test_read_write_multinode_node{i}")

        node1_zk.create("/test_read_write_multinode_node1", b"somedata1")
        node2_zk.create("/test_read_write_multinode_node2", b"somedata2")
        node3_zk.create("/test_read_write_multinode_node3", b"somedata3")

        # stale reads are allowed
        while node1_zk.exists("/test_read_write_multinode_node2") is None:
            time.sleep(0.1)

        while node1_zk.exists("/test_read_write_multinode_node3") is None:
            time.sleep(0.1)

        while node2_zk.exists("/test_read_write_multinode_node3") is None:
            time.sleep(0.1)

        assert node3_zk.get("/test_read_write_multinode_node1")[0] == b"somedata1"
        assert node2_zk.get("/test_read_write_multinode_node1")[0] == b"somedata1"
        assert node1_zk.get("/test_read_write_multinode_node1")[0] == b"somedata1"

        assert node3_zk.get("/test_read_write_multinode_node2")[0] == b"somedata2"
        assert node2_zk.get("/test_read_write_multinode_node2")[0] == b"somedata2"
        assert node1_zk.get("/test_read_write_multinode_node2")[0] == b"somedata2"

        assert node3_zk.get("/test_read_write_multinode_node3")[0] == b"somedata3"
        assert node2_zk.get("/test_read_write_multinode_node3")[0] == b"somedata3"
        assert node1_zk.get("/test_read_write_multinode_node3")[0] == b"somedata3"

    finally:
        try:
            for zk_conn in [node1_zk, node2_zk, node3_zk]:
                zk_conn.stop()
                zk_conn.close()
        except:
            pass


def test_watch_on_follower(started_cluster):
    try:
        wait_nodes()
        node1_zk = get_fake_zk("node1")
        node2_zk = get_fake_zk("node2")
        node3_zk = get_fake_zk("node3")

        # Cleanup
        if node1_zk.exists("/test_data_watches") != None:
            node1_zk.delete("/test_data_watches")

        node1_zk.create("/test_data_watches")
        node2_zk.set("/test_data_watches", b"hello")
        node3_zk.set("/test_data_watches", b"world")

        node1_data = None

        def node1_callback(event):
            if not keeper_utils.is_znode_watch_event(event):
                return
            print("node1 data watch called")
            nonlocal node1_data
            node1_data = event

        node1_zk.get("/test_data_watches", watch=node1_callback)

        node2_data = None

        def node2_callback(event):
            if not keeper_utils.is_znode_watch_event(event):
                return
            print("node2 data watch called")
            nonlocal node2_data
            node2_data = event

        node2_zk.get("/test_data_watches", watch=node2_callback)

        node3_data = None

        def node3_callback(event):
            if not keeper_utils.is_znode_watch_event(event):
                return
            print("node3 data watch called")
            nonlocal node3_data
            node3_data = event

        node3_zk.get("/test_data_watches", watch=node3_callback)

        node1_zk.set("/test_data_watches", b"somevalue")
        time.sleep(3)

        print(node1_data)
        print(node2_data)
        print(node3_data)

        assert node1_data == node2_data
        assert node3_data == node2_data

    finally:
        try:
            for zk_conn in [node1_zk, node2_zk, node3_zk]:
                zk_conn.stop()
                zk_conn.close()
        except:
            pass


def test_session_expiration(started_cluster):
    try:
        wait_nodes()
        node1_zk = get_fake_zk("node1")
        node2_zk = get_fake_zk("node2")
        node3_zk = get_fake_zk("node3", timeout=3.0)
        print("Node3 session id", node3_zk._session_id)

        # Cleanup
        if node3_zk.exists("/test_ephemeral_node") != None:
            node3_zk.delete("/test_ephemeral_node")

        node3_zk.create("/test_ephemeral_node", b"world", ephemeral=True)

        with PartitionManager() as pm:
            pm.partition_instances(node3, node2)
            pm.partition_instances(node3, node1)
            node3_zk.stop()
            node3_zk.close()

            # Wait for the ephemeral node to disappear after session expiration.
            # Use a wall-clock timeout instead of a fixed iteration count,
            # because under sanitizers (especially MSan) everything is much slower.
            # session_timeout_ms is 10s; give extra time for sanitizer builds.
            start = time.time()
            while time.time() - start < 120:
                time.sleep(2)
                try:
                    node1_exists = node1_zk.exists("/test_ephemeral_node")
                    node2_exists = node2_zk.exists("/test_ephemeral_node")
                except Exception as e:
                    print("Exception checking existence:", e)
                    continue

                if node1_exists is None and node2_exists is None:
                    break

                print("Node1 exists", node1_exists)
                print("Node2 exists", node2_exists)
            else:
                raise Exception(
                    "Ephemeral node was not deleted after session expiration"
                )

        assert node1_zk.exists("/test_ephemeral_node") is None
        assert node2_zk.exists("/test_ephemeral_node") is None

    finally:
        try:
            for zk_conn in [node1_zk, node2_zk, node3_zk]:
                try:
                    zk_conn.stop()
                    zk_conn.close()
                except:
                    pass
        except:
            pass


def test_follower_restart(started_cluster):
    try:
        wait_nodes()
        node1_zk = get_fake_zk("node1")
        node3_zk = get_fake_zk("node3")

        # Cleanup
        if node1_zk.exists("/test_restart_node") != None:
            node1_zk.delete("/test_restart_node")

        node1_zk.create("/test_restart_node", b"hello")
        node3.restart_clickhouse(kill=True)

        wait_nodes()

        node3_zk = get_fake_zk("node3")
        # got data from log
        assert node3_zk.get("/test_restart_node")[0] == b"hello"

    finally:
        try:
            for zk_conn in [node1_zk, node3_zk]:
                try:
                    zk_conn.stop()
                    zk_conn.close()
                except:
                    pass
        except:
            pass


def test_simple_replicated_table(started_cluster):
    wait_nodes()
    test_name = f"test_simple_replicated_table_{uuid.uuid4().hex}"

    for i, node in enumerate([node1, node2, node3]):
        node.query(f"DROP TABLE IF EXISTS {test_name} SYNC")
        node.query(
            f"CREATE TABLE {test_name} (value UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/{test_name}', '{i + 1}') ORDER BY tuple()"
        )

    node2.query(f"INSERT INTO {test_name} SELECT number FROM numbers(10)")

    node1.query(f"SYSTEM SYNC REPLICA {test_name}", timeout=10)
    node3.query(f"SYSTEM SYNC REPLICA {test_name}", timeout=10)

    assert_eq_with_retry(node1, f"SELECT COUNT() FROM {test_name}", "10")
    assert_eq_with_retry(node2, f"SELECT COUNT() FROM {test_name}", "10")
    assert_eq_with_retry(node3, f"SELECT COUNT() FROM {test_name}", "10")
