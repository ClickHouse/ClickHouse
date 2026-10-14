#!/usr/bin/env python3

import logging
import os
import time

import pytest
from azure.storage.blob import BlobServiceClient

import helpers.keeper_utils as keeper_utils
from helpers.cluster import ClickHouseCluster

CURRENT_TEST_DIR = os.path.dirname(os.path.abspath(__file__))


def generate_config(azurite_port):
    path = os.path.join(CURRENT_TEST_DIR, "_gen/keeper_config.xml")
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        f.write(
            f"""<clickhouse>
    <logger>
        <level>trace</level>
    </logger>
    <storage_configuration>
        <disks>
            <log_local>
                <type>local</type>
                <path>/var/lib/clickhouse/coordination/logs/</path>
            </log_local>
            <log_azure_plain>
                <type>s3_plain</type>
                <object_storage_type>azure_blob_storage</object_storage_type>
                <endpoint>http://azurite1:{azurite_port}/devstoreaccount1/cont</endpoint>
                <endpoint_subpath>logs</endpoint_subpath>
                <account_name>devstoreaccount1</account_name>
                <account_key>Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==</account_key>
            </log_azure_plain>
            <snapshot_local>
                <type>local</type>
                <path>/var/lib/clickhouse/coordination/snapshots/</path>
            </snapshot_local>
        </disks>
    </storage_configuration>
    <keeper_server>
        <use_cluster>false</use_cluster>
        <tcp_port>9181</tcp_port>
        <server_id>1</server_id>
        <create_snapshot_on_exit>false</create_snapshot_on_exit>
        <coordination_settings>
            <operation_timeout_ms>5000</operation_timeout_ms>
            <session_timeout_ms>10000</session_timeout_ms>
            <raft_logs_level>trace</raft_logs_level>
            <!-- High snapshot_distance so no snapshot is created;
                 Keeper must rely on log files for state reconstruction. -->
            <snapshot_distance>100000</snapshot_distance>
            <stale_log_gap>10</stale_log_gap>
            <reserved_log_items>1</reserved_log_items>
            <rotate_log_storage_interval>3</rotate_log_storage_interval>
        </coordination_settings>
        <log_storage_disk>log_azure_plain</log_storage_disk>
        <latest_log_storage_disk>log_local</latest_log_storage_disk>
        <snapshot_storage_disk>snapshot_local</snapshot_storage_disk>
        <raft_configuration>
            <server>
                <id>1</id>
                <hostname>node</hostname>
                <port>9234</port>
            </server>
        </raft_configuration>
    </keeper_server>
</clickhouse>"""
        )
    return path


@pytest.fixture(scope="module")
def started_cluster():
    cluster = ClickHouseCluster(__file__)
    try:
        config_path = generate_config(cluster.azurite_port)
        cluster.add_instance(
            "node",
            main_configs=[config_path],
            stay_alive=True,
            with_azurite=True,
        )
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def get_azure_container_client(started_cluster):
    """Get an Azure container client for the Azurite instance."""
    port = started_cluster.env_variables["AZURITE_PORT"]
    connection_string = (
        f"DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;"
        f"AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;"
        f"BlobEndpoint=http://127.0.0.1:{port}/devstoreaccount1;"
    )
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    return blob_service_client.get_container_client("cont")


def list_azure_blobs(started_cluster, prefix=""):
    """List blobs in the Azure container via the Python SDK (from the host)."""
    container_client = get_azure_container_client(started_cluster)
    return [
        blob.name
        for blob in container_client.list_blobs(
            name_starts_with=prefix or None
        )
    ]


def cleanup(started_cluster, node):
    """Remove all Azure blobs and local coordination data for a clean run."""
    node.stop_clickhouse()

    container_client = get_azure_container_client(started_cluster)
    for blob in container_client.list_blobs():
        container_client.delete_blob(blob.name)

    # Remove local coordination directories.
    node.exec_in_container(
        ["rm", "-rf", "/var/lib/clickhouse/coordination/logs"]
    )
    node.exec_in_container(
        ["rm", "-rf", "/var/lib/clickhouse/coordination/snapshots"]
    )

    node.start_clickhouse()
    keeper_utils.wait_until_connected(started_cluster, node)


def test_keeper_logs_azure_s3_plain(started_cluster):
    """Keeper writes logs to Azure s3_plain disk; after restart it must be able
    to list and read them back."""

    node = started_cluster.instances["node"]
    cleanup(started_cluster, node)

    # Create enough Keeper entries to trigger multiple log file rotations.
    # With stale_log_gap=10 and rotate_log_storage_interval=3, creating 30+
    # entries should produce several log files that get rotated to Azure.
    zk = keeper_utils.get_fake_zk(started_cluster, "node")
    try:
        zk.create("/test_azure")
        for i in range(30):
            zk.create(f"/test_azure/node_{i:04d}", b"data")
    finally:
        zk.stop()
        zk.close()

    # Wait for log rotation to move old log files to Azure.
    deadline = time.time() + 60
    azure_blobs = []
    while time.time() < deadline:
        azure_blobs = list_azure_blobs(started_cluster, prefix="logs/")
        if azure_blobs:
            break
        time.sleep(1)

    logging.info(f"Azure blobs after log rotation: {azure_blobs}")
    assert azure_blobs, "Expected Keeper log files in Azure after rotation"

    # Restart Keeper. On startup, it must list the Azure disk to discover
    # old log files and replay them. With the fs::path bug,
    # ContainerClientWrapper::ListBlobs receives prefix "/" which replaces
    # the blob_prefix "logs" entirely, so Azure returns zero blobs.
    node.restart_clickhouse()
    keeper_utils.wait_until_connected(started_cluster, node)

    # All data must be accessible. If Azure listing is broken, the old logs
    # are invisible and znodes written before the last log rotation are lost.
    zk = keeper_utils.get_fake_zk(started_cluster, "node")
    try:
        children = sorted(zk.get_children("/test_azure"))
        assert len(children) == 30, (
            f"Expected 30 znodes after restart, got {len(children)}. "
            "Azure s3_plain listing likely returned empty results."
        )
        for child in children:
            data, _ = zk.get(f"/test_azure/{child}")
            assert data == b"data"
    finally:
        zk.stop()
        zk.close()
