#!/usr/bin/env python3
"""
Test for TLS certificate hot-reload on Keeper Raft connections.

This test verifies that when TLS certificates are replaced on disk,
new Raft connections between Keeper nodes pick up the new certificates
without requiring a restart.
"""

import os
import time

import pytest

import helpers.keeper_utils as ku
from helpers.cluster import ClickHouseCluster

CURRENT_TEST_DIR = os.path.dirname(os.path.abspath(__file__))
cluster = ClickHouseCluster(__file__)

# Common config files for all nodes
COMMON_CONFIGS = [
    "configs/ssl_conf.yml",
    "configs/first.crt",
    "configs/first.key",
    "configs/second.crt",
    "configs/second.key",
    "configs/rootCA.pem",
]

node1 = cluster.add_instance(
    "node1",
    main_configs=["configs/enable_secure_keeper1.xml"] + COMMON_CONFIGS,
    stay_alive=True,
)
node2 = cluster.add_instance(
    "node2",
    main_configs=["configs/enable_secure_keeper2.xml"] + COMMON_CONFIGS,
    stay_alive=True,
)
node3 = cluster.add_instance(
    "node3",
    main_configs=["configs/enable_secure_keeper3.xml"] + COMMON_CONFIGS,
    stay_alive=True,
)

all_nodes = [node1, node2, node3]


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def get_fake_zk(nodename, timeout=30.0):
    return ku.get_fake_zk(cluster, nodename, timeout=timeout)


def wait_nodes_ready(nodes):
    """Wait for specified nodes to be connected and form a quorum."""
    for node in nodes:
        ku.wait_until_connected(cluster, node)


def verify_cluster_works(test_path, nodes_to_check):
    """Verify the cluster can perform basic operations."""
    node_zks = []
    try:
        for node in nodes_to_check:
            node_zks.append(get_fake_zk(node.name))

        # Create a node from first node
        node_zks[0].create(test_path, b"test_data")

        # Verify from all nodes
        for i, node_zk in enumerate(node_zks):
            node_zk.sync(test_path)
            assert node_zk.exists(test_path) is not None, f"Node {i+1} cannot see {test_path}"
            data, _ = node_zk.get(test_path)
            assert data == b"test_data", f"Node {i+1} has wrong data"

        return True
    finally:
        for zk_conn in node_zks:
            if zk_conn:
                try:
                    zk_conn.stop()
                    zk_conn.close()
                except Exception:
                    pass


def replace_certificates(node):
    """
    Replace certificate files in-place.
    Copies second.crt/key over first.crt/key (the configured paths).
    """
    node.exec_in_container(
        [
            "bash",
            "-c",
            "cp /etc/clickhouse-server/config.d/second.crt /etc/clickhouse-server/config.d/first.crt && "
            "cp /etc/clickhouse-server/config.d/second.key /etc/clickhouse-server/config.d/first.key && "
            "touch /etc/clickhouse-server/config.d/first.crt /etc/clickhouse-server/config.d/first.key",
        ]
    )


def get_cert_serial(node):
    """Get the serial number of the currently configured certificate."""
    result = node.exec_in_container(
        [
            "openssl",
            "x509",
            "-in",
            "/etc/clickhouse-server/config.d/first.crt",
            "-serial",
            "-noout",
        ]
    )
    return result.strip()


def test_cert_reload_on_reconnect(started_cluster):
    """
    Test that restarted node uses updated certificates for new Raft connections.

    Steps:
    1. Start 3-node cluster with 'first' certificate
    2. Verify cluster works
    3. Replace certificates with 'second' on all nodes
    4. Trigger config reload
    5. Restart node3 - creates NEW Raft connections
    6. Verify cluster works (proves new certs work)
    """
    # Wait for cluster to be ready
    wait_nodes_ready(all_nodes)

    # Get initial certificate serial
    initial_serial = get_cert_serial(node1)
    print(f"Initial certificate serial: {initial_serial}")

    # Verify initial cluster works
    verify_cluster_works("/test_initial", all_nodes)
    print("Initial cluster working with first certificates")

    # Replace certificate files on ALL nodes
    for node in all_nodes:
        replace_certificates(node)
    print("Replaced certificate files on all nodes")

    # Trigger config reload on all nodes
    for node in all_nodes:
        node.query("SYSTEM RELOAD CONFIG")
    print("Config reload triggered")

    # Verify the certificate file changed
    new_serial = get_cert_serial(node1)
    print(f"New certificate serial: {new_serial}")
    assert initial_serial != new_serial, "Certificate serial should have changed"

    # Give time for reload to process
    time.sleep(2)

    # Restart node3 - this creates NEW Raft connections using new certs
    print("Restarting node3 to create new Raft connections...")
    node3.restart_clickhouse()

    # Wait for node3 to rejoin
    wait_nodes_ready([node3])
    print("Node3 restarted and reconnected")

    # Verify cluster works - proves new connections use updated certs
    verify_cluster_works("/test_after_restart", all_nodes)
    print("Cluster working after restart - new Raft connections use updated certs!")
