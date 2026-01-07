#!/usr/bin/env python3
"""
Test for TLS certificate hot-reload on Keeper Raft connections.

This test verifies that when TLS certificates are replaced on disk,
new Raft connections between Keeper nodes pick up the new certificates
without requiring a restart. CertificateReloader detects file changes
via modification time.
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

# Start with 3 nodes, node4 will be added dynamically to test new connections
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
# node4 starts later - used to verify new Raft connections use updated certs
node4 = cluster.add_instance(
    "node4",
    main_configs=["configs/enable_secure_keeper4.xml"] + COMMON_CONFIGS,
    stay_alive=True,
)

initial_nodes = [node1, node2, node3]
all_nodes = [node1, node2, node3, node4]


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        # Initialize: copy first.crt/key to current.crt/key on all nodes
        for node in all_nodes:
            node.exec_in_container(
                [
                    "bash",
                    "-c",
                    "cp /etc/clickhouse-server/config.d/first.crt /etc/clickhouse-server/config.d/current.crt && "
                    "cp /etc/clickhouse-server/config.d/first.key /etc/clickhouse-server/config.d/current.key",
                ]
            )

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

        # Sync and verify from all nodes
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
                except:
                    pass


def replace_certificates_in_place(node, cert_name):
    """
    Replace certificate files in-place (same path, new content).
    This simulates how cert-manager or certbot renews certificates.
    The file modification time changes, triggering CertificateReloader.
    """
    node.exec_in_container(
        [
            "bash",
            "-c",
            f"cp /etc/clickhouse-server/config.d/{cert_name}.crt /etc/clickhouse-server/config.d/current.crt && "
            f"cp /etc/clickhouse-server/config.d/{cert_name}.key /etc/clickhouse-server/config.d/current.key && "
            # Touch to ensure modification time is updated
            "touch /etc/clickhouse-server/config.d/current.crt /etc/clickhouse-server/config.d/current.key",
        ]
    )


def get_cert_serial(node):
    """Get the serial number of the currently configured certificate."""
    result = node.exec_in_container(
        [
            "openssl",
            "x509",
            "-in",
            "/etc/clickhouse-server/config.d/current.crt",
            "-serial",
            "-noout",
        ]
    )
    return result.strip()


def trigger_config_reload(node):
    """Trigger config reload to pick up new certificates."""
    node.query("SYSTEM RELOAD CONFIG")


def test_new_node_joins_with_updated_certs(started_cluster):
    """
    Test that a new node can join the cluster after certificates are rotated.

    This verifies that new Raft connections use the updated certificates.

    Steps:
    1. Start 3-node cluster with 'first' certificate
    2. Verify cluster works
    3. Replace certificates with 'second' on all nodes (including node4)
    4. Trigger config reload on existing nodes
    5. Start node4 - it will establish NEW Raft connections
    6. Verify node4 successfully joins (proves new certs work for new connections)
    """
    # Wait for initial 3-node cluster to be ready
    wait_nodes_ready(initial_nodes)

    # Get initial certificate serial
    initial_serial = get_cert_serial(node1)
    print(f"Initial certificate serial: {initial_serial}")

    # Verify initial cluster works
    verify_cluster_works("/test_initial", initial_nodes)
    print("Initial 3-node cluster working with first certificates")

    # Replace certificate files on ALL nodes (including node4 which hasn't started yet)
    for node in all_nodes:
        replace_certificates_in_place(node, "second")
    print("Replaced certificate files with second cert on all nodes")

    # Trigger config reload on running nodes
    for node in initial_nodes:
        trigger_config_reload(node)
    print("Config reload triggered on initial nodes")

    # Verify the certificate file changed
    new_serial = get_cert_serial(node1)
    print(f"New certificate serial: {new_serial}")
    assert initial_serial != new_serial, "Certificate serial should have changed"

    # Give some time for the reload to process
    time.sleep(2)

    # Existing cluster should still work
    verify_cluster_works("/test_after_reload", initial_nodes)
    print("Cluster working after cert reload (existing connections)")

    # Now start node4 - this creates NEW Raft connections
    # These new connections must use the updated certificates
    print("Starting node4 to test new Raft connections with updated certs...")
    node4.start_clickhouse()

    # Wait for node4 to join the cluster
    wait_nodes_ready([node4])
    print("Node4 started and connected")

    # Verify all 4 nodes can work together
    # This proves new Raft connections successfully use the new certificates
    verify_cluster_works("/test_with_new_node", all_nodes)
    print("All 4 nodes working together - new Raft connections use updated certs!")
