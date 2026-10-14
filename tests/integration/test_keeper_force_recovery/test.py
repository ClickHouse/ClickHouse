import os
import socket
import time

import pytest
from kazoo.client import KazooClient, KazooRetry

import helpers.keeper_utils as keeper_utils
from helpers.cluster import ClickHouseCluster

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
                main_configs=[f"configs/enable_keeper{i+1}.xml"],
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
        hosts=cluster.get_instance_ip(nodename) + ":9181",
        timeout=timeout,
        connection_retry=KazooRetry(max_tries=10),
        command_retry=KazooRetry(max_tries=10),
    )

    _fake_zk_instance.start()
    return _fake_zk_instance


def wait_and_assert_data(zk, path, data):
    while zk.retry(zk.exists, path) is None:
        time.sleep(0.1)
    assert zk.retry(zk.get, path)[0] == data.encode()


def close_zk(zk):
    zk.stop()
    zk.close()


def wait_nodes_with_retries(cluster, nodes, timeout=120):
    """
    Wait for Keeper nodes to be ready, handling ConnectionRefusedError.
    This is needed because wait_until_connected doesn't handle the case
    where the server isn't listening yet.
    """
    start_time = time.time()
    while time.time() - start_time < timeout:
        try:
            keeper_utils.wait_nodes(cluster, nodes)
            return
        except (ConnectionRefusedError, OSError) as e:
            # Server not listening yet, retry
            time.sleep(1)
    raise Exception(f"Timeout waiting for Keeper nodes to be ready after {timeout}s")


def reset_cluster_for_next_iteration():
    """
    Reset cluster to initial state for repeated test runs.
    This is needed because the test modifies Keeper cluster membership via force recovery.
    """
    # Check if node 6 has a Keeper config (it only has one after the test runs)
    result = nodes[CLUSTER_SIZE].exec_in_container(
        ["bash", "-c", f"test -f /etc/clickhouse-server/config.d/enable_keeper{CLUSTER_SIZE+1}.xml && echo yes || echo no"],
        nothrow=True,
    )
    if result.strip() != "yes":
        return  # Cluster is in initial state, no reset needed

    # Force kill all ClickHouse processes (use kill=True for faster shutdown with TSAN)
    for node in nodes:
        node.stop_clickhouse(kill=True)

    # Clear coordination data on all nodes to reset Raft state
    for node in nodes:
        node.exec_in_container(["rm", "-rf", "/var/lib/clickhouse/coordination"])

    # Remove Keeper configs from nodes 6-8
    for i in range(CLUSTER_SIZE, len(nodes)):
        nodes[i].exec_in_container(
            ["rm", "-f", f"/etc/clickhouse-server/config.d/enable_keeper{i+1}.xml"],
            nothrow=True,
        )

    # Restore node 1's original Keeper config
    nodes[0].copy_file_to_container(
        os.path.join(CONFIG_DIR, "enable_keeper1.xml"),
        "/etc/clickhouse-server/config.d/enable_keeper1.xml",
    )

    # Start nodes 1-5 without waiting (they need to start together to form quorum)
    for node in nodes[:CLUSTER_SIZE]:
        node.exec_in_container(
            ["bash", "-c", node.clickhouse_start_command],
            detach=True,
        )

    # Wait for ClickHouse processes to actually be running before checking Keeper
    for node in nodes[:CLUSTER_SIZE]:
        for _ in range(120):  # 120 seconds timeout
            if node.get_process_pid("clickhouse") is not None:
                break
            time.sleep(1)
        else:
            raise Exception(f"ClickHouse process did not start on {node.name}")

    # Wait for the Keeper cluster to form and become ready (with retry for connection refused)
    wait_nodes_with_retries(cluster, nodes[:CLUSTER_SIZE])


def test_cluster_recovery(started_cluster):
    node_zks = []
    try:
        # Reset cluster state if this is a repeated test run (flaky test checker runs this 10 times)
        reset_cluster_for_next_iteration()

        # initial cluster of `cluster_size` nodes
        for node in nodes[CLUSTER_SIZE:]:
            node.stop_clickhouse()

        # Use retry wrapper in case Keeper is still starting after reset
        wait_nodes_with_retries(cluster, nodes[:CLUSTER_SIZE])

        node_zks = [get_fake_zk(node.name) for node in nodes[:CLUSTER_SIZE]]

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
                node_zks[1] = get_fake_zk(nodes[1].name, timeout=30.0)
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
        keeper_utils.wait_until_quorum_lost(cluster, nodes[0])

        nodes[0].copy_file_to_container(
            os.path.join(CONFIG_DIR, "recovered_keeper1.xml"),
            "/etc/clickhouse-server/config.d/enable_keeper1.xml",
        )

        nodes[0].query("SYSTEM RELOAD CONFIG")

        assert (
            keeper_utils.send_4lw_cmd(cluster, nodes[0], "mntr")
            == keeper_utils.NOT_SERVING_REQUESTS_ERROR_MSG
        )
        keeper_utils.send_4lw_cmd(cluster, nodes[0], "rcvr")
        assert (
            keeper_utils.send_4lw_cmd(cluster, nodes[0], "mntr")
            == keeper_utils.NOT_SERVING_REQUESTS_ERROR_MSG
        )

        # add one node to restore the quorum
        nodes[CLUSTER_SIZE].copy_file_to_container(
            os.path.join(
                CONFIG_DIR,
                f"enable_keeper{CLUSTER_SIZE+1}.xml",
            ),
            f"/etc/clickhouse-server/config.d/enable_keeper{CLUSTER_SIZE+1}.xml",
        )

        nodes[CLUSTER_SIZE].start_clickhouse()
        keeper_utils.wait_until_connected(cluster, nodes[CLUSTER_SIZE])

        # node1 should have quorum now and accept requests
        keeper_utils.wait_until_connected(cluster, nodes[0])

        node_zks.append(get_fake_zk(nodes[CLUSTER_SIZE].name))

        # add rest of the nodes
        for i in range(CLUSTER_SIZE + 1, len(nodes)):
            node = nodes[i]
            node.copy_file_to_container(
                os.path.join(CONFIG_DIR, f"enable_keeper{i+1}.xml"),
                f"/etc/clickhouse-server/config.d/enable_keeper{i+1}.xml",
            )
            node.start_clickhouse()
            keeper_utils.wait_until_connected(cluster, node)
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
        keeper_utils.wait_until_connected(cluster, nodes[0])
        node_zks[0] = get_fake_zk(nodes[0].name)
        for zk in node_zks[:nodes_left]:
            assert_all_data(zk)
    finally:
        try:
            for zk_conn in node_zks:
                close_zk(zk_conn)
        except:
            pass
