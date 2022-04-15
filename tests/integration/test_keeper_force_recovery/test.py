import os
import pytest
import socket
from helpers.cluster import ClickHouseCluster
import time

BASE_DIR = os.path.dirname(os.path.realpath(__file__))

from kazoo.client import KazooClient


def get_quorum_size(cluster_size):
    return cluster_size // 2 + 1


def get_config_dir(cluster_size):
    if cluster_size == 3:
        return "configs/three_node_cluster"
    elif cluster_size == 5:
        return "configs/five_node_cluster"
    else:
        raise Exception("Invalid cluster size {}", cluster_size)


def create_and_start_cluster(cluster_size):
    cluster = ClickHouseCluster(__file__)
    config_dir = get_config_dir(cluster_size)

    quorum_size = get_quorum_size(cluster_size)

    nodes = []
    for i in range(1, cluster_size + quorum_size + 1):
        nodes.append(
            cluster.add_instance(
                f"node{i}",
                main_configs=[
                    f"{config_dir}/enable_keeper{i}.xml",
                    f"{config_dir}/use_keeper.xml",
                ],
                stay_alive=True,
            )
        )

    cluster.start()
    return cluster, nodes


def smaller_exception(ex):
    return "\n".join(str(ex).split("\n")[0:2])


def wait_node(cluster, node):
    for _ in range(100):
        zk = None
        try:
            node.query("SELECT * FROM system.zookeeper WHERE path = '/'")
            zk = get_fake_zk(cluster, node.name, timeout=30.0)
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


def wait_nodes(cluster, nodes):
    for node in nodes:
        wait_node(cluster, node)


def get_fake_zk(cluster, nodename, timeout=30.0):
    _fake_zk_instance = KazooClient(
        hosts=cluster.get_instance_ip(nodename) + ":9181", timeout=timeout
    )
    _fake_zk_instance.start()
    return _fake_zk_instance


def get_keeper_socket(cluster, node_name):
    hosts = cluster.get_instance_ip(node_name)
    client = socket.socket()
    client.settimeout(10)
    client.connect((hosts, 9181))
    return client


def send_4lw_cmd(cluster, node_name, cmd="ruok"):
    client = None
    try:
        client = get_keeper_socket(cluster, node_name)
        client.send(cmd.encode())
        data = client.recv(100_000)
        data = data.decode()
        return data
    finally:
        if client is not None:
            client.close()


def wait_until_connected(cluster, node_name):
    while send_4lw_cmd(cluster, node_name, "mntr") == NOT_SERVING_REQUESTS_ERROR_MSG:
        time.sleep(0.1)


def wait_and_assert_data(zk, path, data):
    while zk.exists(path) is None:
        time.sleep(0.1)
    assert zk.get(path)[0] == data.encode()


def close_zk(zk):
    zk.stop()
    zk.close()


NOT_SERVING_REQUESTS_ERROR_MSG = "This instance is not currently serving requests"


@pytest.mark.parametrize("cluster_size", [3, 5])
def test_three_node_recovery(cluster_size):
    cluster, nodes = create_and_start_cluster(3)
    quorum_size = get_quorum_size(cluster_size)
    node_zks = []
    try:
        # initial cluster of `cluster_size` nodes
        for node in nodes[cluster_size:]:
            node.stop_clickhouse()

        wait_nodes(cluster, nodes[:cluster_size])

        node_zks = [get_fake_zk(cluster, node.name) for node in nodes[:cluster_size]]

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

        for node_zk in node_zks[2:cluster_size]:
            wait_and_assert_data(node_zk, "/test_force_recovery_extra", "somedataextra")

        nodes[0].start_clickhouse()
        wait_and_assert_data(node_zks[0], "/test_force_recovery_extra", "somedataextra")

        # stop last quorum size nodes
        nodes_left = cluster_size - quorum_size
        for node_zk in node_zks[nodes_left:cluster_size]:
            close_zk(node_zk)

        node_zks = node_zks[:nodes_left]

        for node in nodes[nodes_left:cluster_size]:
            node.stop_clickhouse()

        # wait for node1 to lose quorum
        while (
            send_4lw_cmd(cluster, nodes[0].name, "mntr")
            != NOT_SERVING_REQUESTS_ERROR_MSG
        ):
            time.sleep(0.2)

        nodes[0].copy_file_to_container(
            os.path.join(
                BASE_DIR, get_config_dir(cluster_size), "recovered_keeper1.xml"
            ),
            "/etc/clickhouse-server/config.d/enable_keeper1.xml",
        )

        nodes[0].query("SYSTEM RELOAD CONFIG")

        assert (
            send_4lw_cmd(cluster, nodes[0].name, "mntr")
            == NOT_SERVING_REQUESTS_ERROR_MSG
        )
        send_4lw_cmd(cluster, nodes[0].name, "rcvr")
        assert (
            send_4lw_cmd(cluster, nodes[0].name, "mntr")
            == NOT_SERVING_REQUESTS_ERROR_MSG
        )

        # add one node to restore the quorum
        nodes[cluster_size].start_clickhouse()
        wait_node(cluster, nodes[cluster_size])
        wait_until_connected(cluster, nodes[cluster_size].name)

        # node1 should have quorum now and accept requests
        wait_until_connected(cluster, nodes[0].name)

        node_zks.append(get_fake_zk(cluster, nodes[cluster_size].name))

        # add rest of the nodes
        for node in nodes[cluster_size + 1 :]:
            node.start_clickhouse()
            wait_node(cluster, node)
            wait_until_connected(cluster, node.name)
            node_zks.append(get_fake_zk(cluster, node.name))

        for zk in node_zks:
            assert_all_data(zk)

        # new nodes can achieve quorum without the recovery node (cluster should work properly from now on)
        nodes[0].stop_clickhouse()

        add_data(node_zks[-2], "/test_force_recovery_last", "somedatalast")
        wait_and_assert_data(node_zks[-1], "/test_force_recovery_last", "somedatalast")

        nodes[0].start_clickhouse()
        for zk in node_zks[:nodes_left]:
            assert_all_data(zk)
    finally:
        try:
            for zk_conn in node_zks:
                close_zk(zk_conn)
        except:
            pass

        cluster.shutdown()
