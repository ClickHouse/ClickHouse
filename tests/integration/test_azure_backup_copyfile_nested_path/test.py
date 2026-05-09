#!/usr/bin/env python3

"""
Test for fix of BackupWriterAzureBlobStorage::copyFile bug.

The bug: in copyFile(), the destination path was missing the blob_path prefix.
Source: fs::path(blob_path) / source  -- correct
Dest:   destination                   -- was missing blob_path prefix!

Fixed in: src/Backups/BackupIO_AzureBlobStorage.cpp:207

This caused failures when backing up KeeperMap tables that share the same
ZK path to a nested blob_path in Azure. The copied file was written to the
container root instead of the nested path, causing restore to fail.

Related to PR #63636 refactoring near azure blob storage.
"""

import os
import time

import pytest
from helpers.cluster import ClickHouseCluster


def generate_cluster_def(port):
    path = os.path.join(
        os.path.dirname(os.path.realpath(__file__)),
        "./_gen/storage_conf.xml",
    )
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        f.write(
            f"""<clickhouse>
    <keeper_map_path_prefix>/keeper_map</keeper_map_path_prefix>
    <storage_configuration>
        <disks>
            <blob_storage_disk>
                <type>azure_blob_storage</type>
                <storage_account_url>http://azurite1:{port}/devstoreaccount1</storage_account_url>
                <container_name>cont</container_name>
                <skip_access_check>false</skip_access_check>
                <account_name>devstoreaccount1</account_name>
                <account_key>Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==</account_key>
            </blob_storage_disk>
        </disks>
        <policies>
            <blob_storage_policy>
                <volumes>
                    <main>
                        <disk>blob_storage_disk</disk>
                    </main>
                </volumes>
            </blob_storage_policy>
        </policies>
    </storage_configuration>
</clickhouse>
"""
        )
    return path


@pytest.fixture(scope="module")
def started_cluster():
    cluster = ClickHouseCluster(__file__)
    try:
        port = cluster.azurite_port
        path = generate_cluster_def(port)
        cluster.add_instance(
            "node",
            main_configs=[path],
            with_azurite=True,
            with_zookeeper=True,
        )
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def azure_query(node, query, try_num=10):
    """Query wrapper with retry logic for transient Azure errors."""
    for i in range(try_num):
        try:
            return node.query(query)
        except Exception as ex:
            retriable_errors = [
                "Azure::Core::Http::TransportException: Connection was closed",
                "Azure::Core::Http::TransportException: Connection closed",
                "Azure::Core::Http::TransportException: Error while polling",
            ]
            retry = any(error in str(ex) for error in retriable_errors)
            if not retry or i == try_num - 1:
                raise
            time.sleep(i + 1)


def test_azure_backup_copyfile_keepermap_nested_path(started_cluster):
    """
    Test backup with KeeperMap file references to a nested blob_path.

    The bug occurred when:
    1. Multiple KeeperMap tables share the same ZK path (creating references)
    2. Backup uses deduplicate_files=0 (plain backup mode)
    3. Backup path is nested (e.g., 'backups/nested/test')

    Without the fix, the copied (referenced) files were written to container
    root instead of the nested path, causing restore to fail.
    """
    node = started_cluster.instances["node"]
    port = started_cluster.azurite_port

    # Build connection string from cluster port
    connection_string = (
        f"DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;"
        f"AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;"
        f"BlobEndpoint=http://azurite1:{port}/devstoreaccount1;"
    )

    # Create a unique test identifier
    test_id = int(time.time_ns())
    db_name = f"keeper_db_{test_id}"
    zk_path = f"/keeper_map/test_{test_id}"

    # Clean up any previous test state
    azure_query(node, f"DROP DATABASE IF EXISTS {db_name} SYNC")

    # Create database and KeeperMap tables that share the same ZK path
    # This creates BackupEntryReference for the second table
    azure_query(node, f"CREATE DATABASE {db_name}")
    azure_query(
        node,
        f"""
        CREATE TABLE {db_name}.km1 (key UInt64, value String)
        ENGINE = KeeperMap('{zk_path}')
        PRIMARY KEY key
        """,
    )
    azure_query(
        node,
        f"""
        CREATE TABLE {db_name}.km2 (key UInt64, value String)
        ENGINE = KeeperMap('{zk_path}')
        PRIMARY KEY key
        """,
    )

    # Insert data (goes to shared ZK path)
    azure_query(
        node,
        f"INSERT INTO {db_name}.km1 VALUES (1, 'hello'), (2, 'world'), (3, 'test')",
    )

    # Verify both tables see the same data
    result1 = azure_query(node, f"SELECT * FROM {db_name}.km1 ORDER BY key")
    result2 = azure_query(node, f"SELECT * FROM {db_name}.km2 ORDER BY key")
    expected = "1\thello\n2\tworld\n3\ttest\n"
    assert result1 == expected, f"km1 data mismatch: {result1!r}"
    assert result2 == expected, f"km2 data mismatch: {result2!r}"

    # Create a nested backup path - this is the key trigger for the bug
    nested_backup_path = f"backups/nested/keeper_test_{test_id}"
    backup_destination = (
        f"AzureBlobStorage('{connection_string}', 'cont', '{nested_backup_path}')"
    )

    # Backup with deduplicate_files=0 to trigger plain backup mode
    # This causes copyFile to be used for the reference files
    azure_query(
        node,
        f"BACKUP DATABASE {db_name} TO {backup_destination} "
        f"SETTINGS deduplicate_files=0",
    )

    # Drop database to test restore
    azure_query(node, f"DROP DATABASE {db_name} SYNC")

    # Restore - before the fix, this would fail because copyFile
    # wrote files to container root instead of the nested path
    azure_query(node, f"RESTORE DATABASE {db_name} FROM {backup_destination}")

    # Verify restored data is correct
    result1_restored = azure_query(node, f"SELECT * FROM {db_name}.km1 ORDER BY key")
    result2_restored = azure_query(node, f"SELECT * FROM {db_name}.km2 ORDER BY key")

    assert (
        result1_restored == expected
    ), f"km1 restored data mismatch: {result1_restored!r}"
    assert (
        result2_restored == expected
    ), f"km2 restored data mismatch: {result2_restored!r}"

    # Cleanup
    azure_query(node, f"DROP DATABASE IF EXISTS {db_name} SYNC")
