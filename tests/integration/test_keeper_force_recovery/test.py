import os
import pytest
import socket
from helpers.cluster import ClickHouseCluster
import time


from kazoo.client import KazooClient

CLUSTER_SIZE = 5
QUORUM_SIZE = CLUSTER_SIZE // 2 + 1


cluster = ClickHouseCluster(__file__)
CONFIG_DIR = os.path.join(os.path.dirname(os.path.realpath(__file__)), "configs")


def get_nodes():
    nodes = []
    for i in range(CLUSTER_SIZE):
        nodes.append(
            cluster.add_instance(
                f"node{i+1}",
                main_configs=[
                    f"configs/enable_keeper{i+1}.xml",
                    f"configs/use_keeper.xml",
                ],
                stay_alive=True,
            )
        )

    for i in range(CLUSTER_SIZE, CLUSTER_SIZE + QUORUM_SIZE):
        nodes.append(
            cluster.add_instance(f"node{i+1}", main_configs=[], stay_alive=True)
        )

    return nodes


nodes = get_nodes()


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        yield cluster
    finally:
        cluster.shutdown()


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


def send_4lw_cmd(node_name, cmd="ruok"):
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
        time.sleep(0.1)


def wait_nodes(nodes):
    for node in nodes:
        wait_until_connected(node.name)


def wait_and_assert_data(zk, path, data):
    while zk.exists(path) is None:
        time.sleep(0.1)
    assert zk.get(path)[0] == data.encode()


def close_zk(zk):
    zk.stop()
    zk.close()


NOT_SERVING_REQUESTS_ERROR_MSG = "This instance is not currently serving requests"


def test_cluster_recovery(started_cluster):
    node_zks = []
    try:
        # initial cluster of `cluster_size` nodes
        for node in nodes[CLUSTER_SIZE:]:
            node.stop_clickhouse()

        wait_nodes(nodes[:CLUSTER_SIZE])

        node_zks = [get_fake_zk(node.name) for node in nodes[:CLUSTER_SIZE]]

        data_in_cluster = []

        def add_data(zk, path, data):
            zk.create(path, data.encode())
            data_in_cluster.append((path, data))

        def assert_all_data(zk):
            for path, data in data_in_cluster:
                wait_and_assert_data(zk, path, data)

        for i, zk in enumerate(node_zks):
            add_data(zk, f"/test_force_recovery_node{i+1}", f"somedata{i+1}")

        for zk in node_zks:
            assert_all_data(zk)

        nodes[0].stop_clickhouse()

        add_data(node_zks[1], "/test_force_recovery_extra", "somedataextra")

        for node_zk in node_zks[2:CLUSTER_SIZE]:
            wait_and_assert_data(node_zk, "/test_force_recovery_extra", "somedataextra")

        nodes[0].start_clickhouse()
        wait_until_connected(nodes[0].name)
        node_zks[0] = get_fake_zk(nodes[0].name)
        wait_and_assert_data(node_zks[0], "/test_force_recovery_extra", "somedataextra")

        # stop last quorum size nodes
        nodes_left = CLUSTER_SIZE - QUORUM_SIZE
        for node_zk in node_zks[nodes_left:CLUSTER_SIZE]:
            close_zk(node_zk)

        node_zks = node_zks[:nodes_left]

        for node in nodes[nodes_left:CLUSTER_SIZE]:
            node.stop_clickhouse()

        # wait for node1 to lose quorum
        while send_4lw_cmd(nodes[0].name, "mntr") != NOT_SERVING_REQUESTS_ERROR_MSG:
            time.sleep(0.2)

        nodes[0].copy_file_to_container(
            os.path.join(CONFIG_DIR, "recovered_keeper1.xml"),
            "/etc/clickhouse-server/config.d/enable_keeper1.xml",
        )

        nodes[0].query("SYSTEM RELOAD CONFIG")

        assert send_4lw_cmd(nodes[0].name, "mntr") == NOT_SERVING_REQUESTS_ERROR_MSG
        send_4lw_cmd(nodes[0].name, "rcvr")
        assert send_4lw_cmd(nodes[0].name, "mntr") == NOT_SERVING_REQUESTS_ERROR_MSG

        # add one node to restore the quorum
        nodes[CLUSTER_SIZE].copy_file_to_container(
            os.path.join(
                CONFIG_DIR,
                f"enable_keeper{CLUSTER_SIZE+1}.xml",
            ),
            f"/etc/clickhouse-server/config.d/enable_keeper{CLUSTER_SIZE+1}.xml",
        )

        nodes[CLUSTER_SIZE].start_clickhouse()
        wait_until_connected(nodes[CLUSTER_SIZE].name)

        # node1 should have quorum now and accept requests
        wait_until_connected(nodes[0].name)

        node_zks.append(get_fake_zk(nodes[CLUSTER_SIZE].name))

        # add rest of the nodes
        for i in range(CLUSTER_SIZE + 1, len(nodes)):
            node = nodes[i]
            node.copy_file_to_container(
                os.path.join(CONFIG_DIR, f"enable_keeper{i+1}.xml"),
                f"/etc/clickhouse-server/config.d/enable_keeper{i+1}.xml",
            )
            node.start_clickhouse()
            wait_until_connected(node.name)
            node_zks.append(get_fake_zk(node.name))

        # refresh old zk sessions
        for i, node in enumerate(nodes[:nodes_left]):
            node_zks[i] = get_fake_zk(node.name)

        for zk in node_zks:
            assert_all_data(zk)

        # new nodes can achieve quorum without the recovery node (cluster should work properly from now on)
        nodes[0].stop_clickhouse()

        add_data(node_zks[-2], "/test_force_recovery_last", "somedatalast")
        wait_and_assert_data(node_zks[-1], "/test_force_recovery_last", "somedatalast")

        nodes[0].start_clickhouse()
        wait_until_connected(nodes[0].name)
        node_zks[0] = get_fake_zk(nodes[0].name)
        for zk in node_zks[:nodes_left]:
            assert_all_data(zk)
    finally:
        try:
            for zk_conn in node_zks:
                close_zk(zk_conn)
        except:
            pass
