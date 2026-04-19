#!/usr/bin/env python3
"""
Test for TLS certificate hot-reload on Keeper Raft connections.

This test verifies that when TLS certificates are replaced on disk,
new Raft connections between Keeper nodes pick up the new certificates
without requiring a restart.
"""

import os
import time
import uuid

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


@pytest.fixture(autouse=True)
def restore_certificates(started_cluster):
    """Backup and restore certificates for each test to support flaky check repeated runs."""
    # Before test: backup original first.crt/key
    for node in all_nodes:
        node.exec_in_container(
            [
                "bash",
                "-c",
                "cp /etc/clickhouse-server/config.d/first.crt /etc/clickhouse-server/config.d/first.crt.bak && "
                "cp /etc/clickhouse-server/config.d/first.key /etc/clickhouse-server/config.d/first.key.bak",
            ]
        )
    yield
    # After test: restore original first.crt/key from backup and reload config
    for node in all_nodes:
        try:
            node.exec_in_container(
                [
                    "bash",
                    "-c",
                    "cp /etc/clickhouse-server/config.d/first.crt.bak /etc/clickhouse-server/config.d/first.crt && "
                    "cp /etc/clickhouse-server/config.d/first.key.bak /etc/clickhouse-server/config.d/first.key",
                ]
            )
        except Exception:
            pass
    # Reload config so ClickHouse picks up the restored certificates for next iteration
    for node in all_nodes:
        try:
            node.query("SYSTEM RELOAD CONFIG")
        except Exception:
            pass


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


def get_cert_serial_from_file(node):
    """Get the serial number of the certificate file on disk."""
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


def get_cert_serial_from_raft_port(node, target_host):
    """
    Connect to the Raft SSL port and get the certificate serial being served.
    This verifies the actual certificate loaded in the SSL context.
    """
    result = node.exec_in_container(
        [
            "bash",
            "-c",
            f"echo | openssl s_client -connect {target_host}:9234 2>/dev/null | "
            "openssl x509 -serial -noout 2>/dev/null || echo 'CONNECT_FAILED'",
        ]
    )
    return result.strip()


def wait_for_cert_reload(node, target_host, expected_serial, timeout=30):
    """Poll the Raft SSL port until it serves the expected certificate."""
    start = time.time()
    while time.time() - start < timeout:
        served = get_cert_serial_from_raft_port(node, target_host)
        if served == expected_serial:
            return True
        time.sleep(0.5)
    return False


def kill_raft_connections(node):
    """Kill all established connections on the Raft port to force reconnection."""
    node.exec_in_container(
        ["ss", "--kill", "-tn", "state", "established", "( dport = :9234 or sport = :9234 )"]
    )


def test_cert_reload_on_reconnect(started_cluster):
    """
    Test that new Raft connections use updated certificates after reload.

    Steps:
    1. Start 3-node cluster with 'first' certificate
    2. Verify cluster works
    3. Replace certificates with 'second' on all nodes
    4. Trigger config reload
    5. Kill Raft connections to force reconnection with new certs
    6. Verify all nodes serve the new certificate
    7. Verify cluster works (proves new certs work)
    """
    # Wait for cluster to be ready
    wait_nodes_ready(all_nodes)

    # Generate unique test ID for this run to avoid conflicts on flaky retries
    test_id = uuid.uuid4().hex[:8]

    # Get initial certificate serial from file
    initial_serial = get_cert_serial_from_file(node1)
    print(f"Initial certificate serial (from file): {initial_serial}")

    # Wait for the Raft port to serve the initial certificate
    # This handles flaky retries where previous run may have left different cert loaded
    assert wait_for_cert_reload(node2, "node1", initial_serial, timeout=30), (
        f"Raft port should serve initial cert {initial_serial}"
    )
    print(f"Verified: Raft port is serving initial certificate")

    # Verify initial cluster works
    verify_cluster_works(f"/test_initial_{test_id}", all_nodes)
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
    new_serial = get_cert_serial_from_file(node1)
    print(f"New certificate serial (from file): {new_serial}")
    assert initial_serial != new_serial, "Certificate serial should have changed"

    # Kill existing Raft connections on all nodes to force reconnection with new certs
    for node in all_nodes:
        kill_raft_connections(node)
    print("Killed Raft connections on all nodes")

    # Wait for all nodes to reconnect and serve the new certificate
    for node in all_nodes:
        for target in all_nodes:
            if target != node:
                assert wait_for_cert_reload(node, target.name, new_serial, timeout=30), (
                    f"Node {target.name} should serve new cert {new_serial} "
                    f"(as seen from {node.name})"
                )
    print("All nodes serve the new certificate on Raft port")

    # Wait for cluster to re-establish quorum after connection reset
    wait_nodes_ready(all_nodes)

    # Verify cluster works with the new certificates
    verify_cluster_works(f"/test_after_reload_{test_id}", all_nodes)
    print("Cluster working after cert reload - new Raft connections use updated certs!")
