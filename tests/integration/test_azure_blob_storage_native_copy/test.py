#!/usr/bin/env python3

import gzip
import io
import json
import logging
import os
import random
import threading
import time

import pytest
from azure.storage.blob import BlobServiceClient

import helpers.client
from helpers.cluster import ClickHouseCluster, ClickHouseInstance
from helpers.mock_servers import start_mock_servers
from helpers.network import PartitionManager
from helpers.test_tools import exec_query_with_retry


def generate_config(port):
    path = os.path.join(
        os.path.dirname(os.path.realpath(__file__)),
        "./_gen/storage_conf.xml",
    )
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        TEMPLATE = """
        <clickhouse>
            <storage_configuration>
                <disks>
                    <disk_azure>
                        <metadata_type>local</metadata_type>
                        <type>object_storage</type>
                        <object_storage_type>azure_blob_storage</object_storage_type>
                        <storage_account_url>http://azurite1:{port}/devstoreaccount1/</storage_account_url>
                        <container_name>cont</container_name>
                        <skip_access_check>false</skip_access_check>
                        <account_name>devstoreaccount1</account_name>
                        <account_key>Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==</account_key>
                        <use_native_copy>true</use_native_copy>
                    </disk_azure>
                    <disk_azure_other_bucket>
                        <metadata_type>local</metadata_type>
                        <type>object_storage</type>
                        <object_storage_type>azure_blob_storage</object_storage_type>
                        <use_native_copy>true</use_native_copy>
                        <storage_account_url>http://azurite1:{port}/devstoreaccount1/</storage_account_url>
                        <container_name>othercontainer</container_name>
                        <skip_access_check>false</skip_access_check>
                        <account_name>devstoreaccount1</account_name>
                        <account_key>Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==</account_key>
                    </disk_azure_other_bucket>
                    <disk_azure_cache>
                        <type>cache</type>
                        <disk>disk_azure</disk>
                        <path>/tmp/azure_cache/</path>
                        <max_size>1000000000</max_size>
                        <cache_on_write_operations>1</cache_on_write_operations>
                    </disk_azure_cache>
                </disks>
                <policies>
                    <policy_azure>
                        <volumes>
                            <main>
                                <disk>disk_azure</disk>
                            </main>
                        </volumes>
                    </policy_azure>
                    <policy_azure_other_bucket>
                        <volumes>
                            <main>
                                <disk>disk_azure_other_bucket</disk>
                            </main>
                        </volumes>
                    </policy_azure_other_bucket>
                    <policy_azure_cache>
                        <volumes>
                            <main>
                                <disk>disk_azure_cache</disk>
                            </main>
                        </volumes>
                    </policy_azure_cache>
                </policies>
            </storage_configuration>
            <backups>
                <allowed_disk>disk_azure</allowed_disk>
                <allowed_disk>disk_azure_cache</allowed_disk>
                <allowed_disk>disk_azure_other_bucket</allowed_disk>
            </backups>
        </clickhouse>
        """
        f.write(TEMPLATE.format(port=port))
    return path


@pytest.fixture(scope="module")
def cluster():
    try:
        cluster = ClickHouseCluster(__file__)
        port = cluster.azurite_port
        path = generate_config(port)
        cluster.add_instance(
            "node1",
            main_configs=[path],
            with_azurite=True,
        )
        cluster.add_instance(
            "node2",
            main_configs=[path],
            with_azurite=True,
        )
        cluster.add_instance(
            "node3",
            main_configs=[path],
            with_azurite=True,
        )
        cluster.start()

        yield cluster
    finally:
        cluster.shutdown()


def azure_query(
    node, query, expect_error=False, try_num=10, settings={}, query_on_retry=None
):
    for i in range(try_num):
        try:
            if expect_error:
                return node.query_and_get_error(query, settings=settings)
            else:
                return node.query(query, settings=settings)
        except Exception as ex:
            retriable_errors = [
                "DB::Exception: Azure::Core::Http::TransportException: Connection was closed by the server while trying to read a response",
                "DB::Exception: Azure::Core::Http::TransportException: Connection closed before getting full response or response is less than expected",
                "DB::Exception: Azure::Core::Http::TransportException: Connection was closed by the server while trying to read a response",
                "DB::Exception: Azure::Core::Http::TransportException: Error while polling for socket ready read",
                "Azure::Core::Http::TransportException, e.what() = Connection was closed by the server while trying to read a response",
                "Azure::Core::Http::TransportException, e.what() = Connection closed before getting full response or response is less than expected",
                "Azure::Core::Http::TransportException, e.what() = Connection was closed by the server while trying to read a response",
                "Azure::Core::Http::TransportException, e.what() = Error while polling for socket ready read",
            ]
            retry = False
            for error in retriable_errors:
                if error in str(ex):
                    retry = True
                    print(f"Try num: {i}. Having retriable error: {ex}")
                    time.sleep(i)
                    break
            if not retry or i == try_num - 1:
                raise Exception(ex)
            if query_on_retry is not None:
                node.query(query_on_retry)
            continue


def test_backup_restore_on_merge_tree_same_container(cluster):
    node1 = cluster.instances["node1"]
    azure_query(
        node1,
        f"CREATE TABLE test_simple_merge_tree(key UInt64, data String) Engine = MergeTree() ORDER BY tuple() SETTINGS storage_policy='policy_azure_cache'",
    )
    azure_query(node1, f"INSERT INTO test_simple_merge_tree VALUES (1, 'a')")

    backup_destination = f"AzureBlobStorage('{cluster.env_variables['AZURITE_CONNECTION_STRING']}', 'cont', 'test_simple_merge_tree_backup')"
    print("BACKUP DEST", backup_destination)
    azure_query(
        node1,
        f"BACKUP TABLE test_simple_merge_tree TO {backup_destination}",
    )

    assert node1.contains_in_log("using native copy")

    azure_query(
        node1,
        f"RESTORE TABLE test_simple_merge_tree AS test_simple_merge_tree_restored FROM {backup_destination};",
    )
    assert (
        azure_query(node1, f"SELECT * from test_simple_merge_tree_restored") == "1\ta\n"
    )

    assert node1.contains_in_log("using native copy")

    azure_query(node1, f"DROP TABLE test_simple_merge_tree")
    azure_query(node1, f"DROP TABLE test_simple_merge_tree_restored")


def test_backup_restore_on_merge_tree_different_container(cluster):
    node2 = cluster.instances["node2"]
    azure_query(
        node2,
        f"CREATE TABLE test_simple_merge_tree_different_bucket(key UInt64, data String) Engine = MergeTree() ORDER BY tuple() SETTINGS storage_policy='policy_azure_other_bucket'",
    )
    azure_query(
        node2, f"INSERT INTO test_simple_merge_tree_different_bucket VALUES (1, 'a')"
    )

    backup_destination = f"AzureBlobStorage('{cluster.env_variables['AZURITE_CONNECTION_STRING']}', 'cont', 'test_simple_merge_tree_different_bucket_backup_different_bucket')"
    print("BACKUP DEST", backup_destination)
    azure_query(
        node2,
        f"BACKUP TABLE test_simple_merge_tree_different_bucket TO {backup_destination}",
    )

    assert node2.contains_in_log("using native copy")

    azure_query(
        node2,
        f"RESTORE TABLE test_simple_merge_tree_different_bucket AS test_simple_merge_tree_different_bucket_restored FROM {backup_destination};",
    )
    assert (
        azure_query(
            node2, f"SELECT * from test_simple_merge_tree_different_bucket_restored"
        )
        == "1\ta\n"
    )

    assert node2.contains_in_log("using native copy")

    azure_query(node2, f"DROP TABLE test_simple_merge_tree_different_bucket")
    azure_query(node2, f"DROP TABLE test_simple_merge_tree_different_bucket_restored")


def test_backup_restore_on_merge_tree_native_copy_async(cluster):
    node3 = cluster.instances["node3"]
    azure_query(
        node3,
        f"CREATE TABLE test_simple_merge_tree_async(key UInt64, data String) Engine = MergeTree() ORDER BY tuple() SETTINGS storage_policy='policy_azure_cache'",
    )
    azure_query(node3, f"INSERT INTO test_simple_merge_tree_async VALUES (1, 'a')")

    backup_destination = f"AzureBlobStorage('{cluster.env_variables['AZURITE_CONNECTION_STRING']}', 'cont', 'test_simple_merge_tree_async_backup')"
    print("BACKUP DEST", backup_destination)
    azure_query(
        node3,
        f"BACKUP TABLE test_simple_merge_tree_async TO {backup_destination}",
        settings={"azure_max_single_part_copy_size": 0},
    )

    assert node3.contains_in_log("using native copy")

    azure_query(
        node3,
        f"RESTORE TABLE test_simple_merge_tree_async AS test_simple_merge_tree_async_restored FROM {backup_destination};",
        settings={"azure_max_single_part_copy_size": 0},
    )
    assert (
        azure_query(node3, f"SELECT * from test_simple_merge_tree_async_restored")
        == "1\ta\n"
    )

    assert node3.contains_in_log("using native copy")

    azure_query(node3, f"DROP TABLE test_simple_merge_tree_async")
    azure_query(node3, f"DROP TABLE test_simple_merge_tree_async_restored")
