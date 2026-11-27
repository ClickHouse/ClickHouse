import os
import time

import pytest

import helpers.keeper_utils as keeper_utils
from helpers.cluster import ClickHouseCluster

CLUSTER_SIZE = 3

cluster = ClickHouseCluster(__file__)
CONFIG_DIR = os.path.join(os.path.dirname(os.path.realpath(__file__)), "configs")


def get_nodes():
    nodes = []
    for i in range(CLUSTER_SIZE):
        nodes.append(
            cluster.add_instance(
                f"node{i+1}",
                main_configs=[f"configs/enable_keeper{i+1}.xml"],
                stay_alive=True,
            )
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
    return keeper_utils.get_fake_zk(cluster, nodename, timeout=timeout)


def wait_and_assert_data(zk, path, data):
    while zk.retry(zk.exists, path) is None:
        time.sleep(0.1)
    assert zk.retry(zk.get, path)[0] == data.encode()


def close_zk(zk):
    zk.stop()
    zk.close()


def replace_zk(node_zks, i, nodename):
    try:
        close_zk(node_zks[i])
    except Exception:
        pass
    node_zks[i] = get_fake_zk(nodename)


def test_cluster_recovery(started_cluster):
    node_zks = []
    try:
        keeper_utils.wait_nodes(cluster, nodes)

        for node in nodes:
            node_zks.append(get_fake_zk(node.name))

        data_in_cluster = []

        def add_data(zk, path, data):
            zk.retry(zk.create, path, data.encode())
            data_in_cluster.append((path, data))

        def assert_all_data(zk):
            for path, data in data_in_cluster:
                wait_and_assert_data(zk, path, data)

        for i, zk in enumerate(node_zks):
            add_data(zk, f"/test_force_recovery_node{i+1}", f"somedata{i+1}")

        for zk in node_zks:
            assert_all_data(zk)

        nodes[0].stop_clickhouse()

        # we potentially killed the leader node so we give time for election
        for _ in range(100):
            try:
                replace_zk(node_zks, 1, nodes[1].name)
                add_data(node_zks[1], "/test_force_recovery_extra", "somedataextra")
                break
            except Exception as ex:
                time.sleep(0.5)
                print(f"Retrying create on {nodes[1].name}, exception {ex}")
        else:
            raise Exception(f"Failed creating a node on {nodes[1].name}")

        for node_zk in node_zks[2:CLUSTER_SIZE]:
            wait_and_assert_data(node_zk, "/test_force_recovery_extra", "somedataextra")

        nodes[0].start_clickhouse()
        keeper_utils.wait_until_connected(cluster, nodes[0])

        replace_zk(node_zks, 0, nodes[0].name)
        wait_and_assert_data(node_zks[0], "/test_force_recovery_extra", "somedataextra")

        # stop all nodes
        for node_zk in node_zks:
            close_zk(node_zk)
        node_zks = []

        for node in nodes:
            node.stop_clickhouse()

        nodes[0].copy_file_to_container(
            os.path.join(CONFIG_DIR, "enable_keeper1_solo.xml"),
            "/etc/clickhouse-server/config.d/enable_keeper1.xml",
        )

        nodes[0].start_clickhouse()
        keeper_utils.wait_until_connected(cluster, nodes[0])

        node_zks.append(get_fake_zk(nodes[0].name))
        assert_all_data(node_zks[-1])
    finally:
        for zk_conn in node_zks:
            try:
                close_zk(zk_conn)
            except Exception:
                pass
