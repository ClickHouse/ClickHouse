import os
import pytest
import socket
from helpers.cluster import ClickHouseCluster
import time

cluster = ClickHouseCluster(__file__)
CONFIG_DIR = os.path.join(os.path.dirname(os.path.realpath(__file__)), "configs")

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
node4 = cluster.add_instance(
    "node4",
    main_configs=["configs/enable_keeper4.xml", "configs/use_keeper.xml"],
    stay_alive=True,
)
node5 = cluster.add_instance(
    "node5",
    main_configs=["configs/enable_keeper5.xml", "configs/use_keeper.xml"],
    stay_alive=True,
)

from kazoo.client import KazooClient, KazooState


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        yield cluster

    finally:
        cluster.shutdown()


def smaller_exception(ex):
    return "\n".join(str(ex).split("\n")[0:2])


def wait_node(node):
    for _ in range(100):
        zk = None
        try:
            node.query("SELECT * FROM system.zookeeper WHERE path = '/'")
            zk = get_fake_zk(node.name, timeout=30.0)
            zk.create("/test", sequence=True)
            print("node", node.name, "ready")
            break
        except Exception as ex:
            time.sleep(0.2)
            print("Waiting until", node.name, "will be ready, exception", ex)
        finally:
            if zk:
                zk.stop()
                zk.close()
    else:
        raise Exception("Can't wait node", node.name, "to become ready")


def wait_nodes(nodes):
    for node in nodes:
        wait_node(node)


def get_fake_zk(nodename, timeout=30.0):
    _fake_zk_instance = KazooClient(
        hosts=cluster.get_instance_ip(nodename) + ":9181", timeout=timeout
    )
    _fake_zk_instance.start()
    return _fake_zk_instance

def get_keeper_socket(node_name):
    hosts = cluster.get_instance_ip(node_name)
    client = socket.socket()
    client.settimeout(10)
    client.connect((hosts, 9181))
    return client

def send_4lw_cmd(node_name=node1.name, cmd="ruok"):
    client = None
    try:
        client = get_keeper_socket(node_name)
        client.send(cmd.encode())
        data = client.recv(100_000)
        data = data.decode()
        return data
    finally:
        if client is not None:
            client.close()
    
def wait_until_connected(node_name):
    while send_4lw_cmd(node_name, "mntr") == NOT_SERVING_REQUESTS_ERROR_MSG:
        time.sleep(0.2)

def wait_and_assert_data(zk, path, data):
    while zk.exists(path) is None:
        time.sleep(0.2)
    assert zk.get(path)[0] == data.encode()


NOT_SERVING_REQUESTS_ERROR_MSG = "This instance is not currently serving requests"

def test_three_node_recovery(started_cluster):
    try:
        # initial cluster is node1 <-> node2 <-> node3
        node4.stop_clickhouse();
        node5.stop_clickhouse();

        wait_nodes([node1, node2, node3])
        node1_zk = get_fake_zk("node1")
        node2_zk = get_fake_zk("node2")
        node3_zk = get_fake_zk("node3")
        node4_zk = None
        node5_zk = None

        for i, zk in enumerate([node1_zk, node2_zk, node3_zk]):
            zk.create(f"/test_force_recovery_node{i+1}", f"somedata{i+1}".encode())

        for zk in [node1_zk, node2_zk, node3_zk]:
            for i in range(1, 4):
                wait_and_assert_data(zk, f"/test_force_recovery_node{i}", f"somedata{i}")

        node1.stop_clickhouse()

        node2_zk.create("/test_force_recovery_extra", b"someexstradata")
        wait_and_assert_data(node3_zk, "/test_force_recovery_extra", "someexstradata")

        node1.start_clickhouse()
        wait_and_assert_data(node1_zk, "/test_force_recovery_extra", "someexstradata")

        node2.stop_clickhouse()
        node3.stop_clickhouse()

        # wait for node1 to lose quorum
        while send_4lw_cmd(node1.name, "mntr") != NOT_SERVING_REQUESTS_ERROR_MSG:
            time.sleep(0.2)

        node1.copy_file_to_container(
                os.path.join(CONFIG_DIR, "recovered_keeper1.xml"),
                "/etc/clickhouse-server/config.d/enable_keeper1.xml")

        node1.query("SYSTEM RELOAD CONFIG")

        assert send_4lw_cmd(node1.name, "mntr") == NOT_SERVING_REQUESTS_ERROR_MSG
        send_4lw_cmd(node1.name, "rcvr")
        assert send_4lw_cmd(node1.name, "mntr") == NOT_SERVING_REQUESTS_ERROR_MSG

        node4.start_clickhouse()
        wait_node(node4)
        wait_until_connected(node4.name)

        # node1 should have quorum now and accept requests
        wait_until_connected(node1.name)

        node5.start_clickhouse()
        wait_node(node5)
        wait_until_connected(node5.name)

        node4_zk = get_fake_zk("node4")
        node5_zk = get_fake_zk("node5")

        for zk in [node1_zk, node4_zk, node5_zk]:
            for i in range(1, 4):
                wait_and_assert_data(zk, f"/test_force_recovery_node{i}", f"somedata{i}")
            wait_and_assert_data(zk, "/test_force_recovery_extra", "someexstradata")

        # new nodes can achieve quorum without the recovery node (cluster should work properly from now on)
        node1.stop_clickhouse()

        node4_zk.create("/test_force_recovery_node4", b"somedata4")
        wait_and_assert_data(node5_zk, "/test_force_recovery_node4", "somedata4")

        node1.start_clickhouse()
        for i in range(1, 5):
            wait_and_assert_data(node1_zk, f"/test_force_recovery_node{i}", f"somedata{i}")
        wait_and_assert_data(node1_zk, "/test_force_recovery_extra", "someexstradata")
    finally:
        try:
            for zk_conn in [node1_zk, node2_zk, node3_zk, node4_zk, node5_zk]:
                if zk_conn:
                    zk_conn.stop()
                    zk_conn.close()
        except:
            pass
