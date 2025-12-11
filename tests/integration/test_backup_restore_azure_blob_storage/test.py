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


def generate_cluster_def(port):
    path = os.path.join(
        os.path.dirname(os.path.realpath(__file__)),
        "./_gen/named_collections.xml",
    )
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        f.write(
            f"""<clickhouse>
    <named_collections>
        <azure_conf1>
            <connection_string>DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://azurite1:{port}/devstoreaccount1;</connection_string>
            <container>cont</container>
            <format>CSV</format>
        </azure_conf1>
        <azure_conf2>
            <storage_account_url>http://azurite1:{port}/devstoreaccount1</storage_account_url>
            <container>cont</container>
            <format>CSV</format>
            <account_name>devstoreaccount1</account_name>
            <account_key>Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==</account_key>
        </azure_conf2>
    </named_collections>
    <storage_configuration>
        <disks>
            <blob_storage_disk>
                <type>azure_blob_storage</type>
                <storage_account_url>http://azurite1:{port}/devstoreaccount1</storage_account_url>
                <container_name>cont</container_name>
                <skip_access_check>false</skip_access_check>
                <account_name>devstoreaccount1</account_name>
                <account_key>Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==</account_key>
                <max_single_part_upload_size>100000</max_single_part_upload_size>
                <min_upload_part_size>100000</min_upload_part_size>
                <max_single_download_retries>10</max_single_download_retries>
                <max_single_read_retries>10</max_single_read_retries>
            </blob_storage_disk>
            <hdd>
                <type>local</type>
                <path>/</path>
            </hdd>
        </disks>
        <policies>
            <blob_storage_policy>
                <volumes>
                    <main>
                        <disk>blob_storage_disk</disk>
                    </main>
                    <external>
                        <disk>hdd</disk>
                    </external>
                </volumes>
            </blob_storage_policy>
        </policies>
    </storage_configuration>
</clickhouse>
"""
        )
    return path


@pytest.fixture(scope="module")
def cluster():
    try:
        cluster = ClickHouseCluster(__file__)
        port = cluster.azurite_port
        path = generate_cluster_def(port)
        cluster.add_instance(
            "node",
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


def get_azure_file_content(filename, port):
    container_name = "cont"
    connection_string = (
        f"DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;"
        f"AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;"
        f"BlobEndpoint=http://127.0.0.1:{port}/devstoreaccount1;"
    )
    blob_service_client = BlobServiceClient.from_connection_string(
        str(connection_string)
    )
    container_client = blob_service_client.get_container_client(container_name)
    blob_client = container_client.get_blob_client(filename)
    download_stream = blob_client.download_blob()
    return download_stream.readall().decode("utf-8")


def put_azure_file_content(filename, port, data):
    container_name = "cont"
    connection_string = (
        f"DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;"
        f"AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;"
        f"BlobEndpoint=http://127.0.0.1:{port}/devstoreaccount1;"
    )
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    try:
        container_client = blob_service_client.create_container(container_name)
    except:
        container_client = blob_service_client.get_container_client(container_name)

    blob_client = container_client.get_blob_client(filename)
    buf = io.BytesIO(data)
    blob_client.upload_blob(buf)


def test_backup_restore(cluster):
    node = cluster.instances["node"]
    port = cluster.env_variables["AZURITE_PORT"]
    azure_query(
        node,
        f"CREATE TABLE test_simple_write_connection_string (key UInt64, data String) Engine = AzureBlobStorage('{cluster.env_variables['AZURITE_CONNECTION_STRING']}', 'cont', 'test_simple_write_c.csv', 'CSV')",
    )
    azure_query(
        node, f"INSERT INTO test_simple_write_connection_string VALUES (1, 'a')"
    )
    print(get_azure_file_content("test_simple_write_c.csv", port))
    assert get_azure_file_content("test_simple_write_c.csv", port) == '1,"a"\n'

    backup_destination = f"AzureBlobStorage('{cluster.env_variables['AZURITE_CONNECTION_STRING']}', 'cont', 'test_simple_write_c_backup')"
    azure_query(
        node,
        f"BACKUP TABLE test_simple_write_connection_string TO {backup_destination}",
    )
    print(get_azure_file_content("test_simple_write_c_backup/.backup", port))
    azure_query(
        node,
        f"RESTORE TABLE test_simple_write_connection_string AS test_simple_write_connection_string_restored FROM {backup_destination};",
    )
    assert (
        azure_query(node, f"SELECT * from test_simple_write_connection_string_restored")
        == "1\ta\n"
    )


def test_backup_restore_diff_container(cluster):
    node = cluster.instances["node"]
    port = cluster.env_variables["AZURITE_PORT"]
    azure_query(
        node,
        f"CREATE TABLE test_simple_write_connection_string_cont1 (key UInt64, data String) Engine = AzureBlobStorage('{cluster.env_variables['AZURITE_CONNECTION_STRING']}', 'cont', 'test_simple_write_c_cont1.csv', 'CSV')",
    )
    azure_query(
        node, f"INSERT INTO test_simple_write_connection_string_cont1 VALUES (1, 'a')"
    )
    backup_destination = f"AzureBlobStorage('{cluster.env_variables['AZURITE_CONNECTION_STRING']}', 'cont1', 'test_simple_write_c_backup_cont1')"
    azure_query(
        node,
        f"BACKUP TABLE test_simple_write_connection_string_cont1 TO {backup_destination}",
    )
    azure_query(
        node,
        f"RESTORE TABLE test_simple_write_connection_string_cont1 AS test_simple_write_connection_string_restored_cont1 FROM {backup_destination};",
    )
    assert (
        azure_query(
            node, f"SELECT * from test_simple_write_connection_string_restored_cont1"
        )
        == "1\ta\n"
    )


def test_backup_restore_with_named_collection_azure_conf1(cluster):
    node = cluster.instances["node"]
    port = cluster.env_variables["AZURITE_PORT"]
    azure_query(
        node,
        f"CREATE TABLE test_write_connection_string (key UInt64, data String) Engine = AzureBlobStorage('{cluster.env_variables['AZURITE_CONNECTION_STRING']}', 'cont', 'test_simple_write.csv', 'CSV')",
    )
    azure_query(node, f"INSERT INTO test_write_connection_string VALUES (1, 'a')")
    print(get_azure_file_content("test_simple_write.csv", port))
    assert get_azure_file_content("test_simple_write.csv", port) == '1,"a"\n'

    backup_destination = f"AzureBlobStorage(azure_conf1, 'test_simple_write_nc_backup')"
    azure_query(
        node,
        f"BACKUP TABLE test_write_connection_string TO {backup_destination}",
    )
    print(get_azure_file_content("test_simple_write_nc_backup/.backup", port))
    azure_query(
        node,
        f"RESTORE TABLE test_write_connection_string AS test_write_connection_string_restored FROM {backup_destination};",
    )
    assert (
        azure_query(node, f"SELECT * from test_write_connection_string_restored")
        == "1\ta\n"
    )


def test_backup_restore_with_named_collection_azure_conf2(cluster):
    node = cluster.instances["node"]
    port = cluster.env_variables["AZURITE_PORT"]
    azure_query(
        node,
        f"CREATE TABLE test_write_connection_string_2 (key UInt64, data String) Engine = AzureBlobStorage('{cluster.env_variables['AZURITE_CONNECTION_STRING']}', 'cont', 'test_simple_write_2.csv', 'CSV')",
    )
    azure_query(node, f"INSERT INTO test_write_connection_string_2 VALUES (1, 'a')")
    print(get_azure_file_content("test_simple_write_2.csv", port))
    assert get_azure_file_content("test_simple_write_2.csv", port) == '1,"a"\n'

    backup_destination = (
        f"AzureBlobStorage(azure_conf2, 'test_simple_write_nc_backup_2')"
    )
    azure_query(
        node,
        f"BACKUP TABLE test_write_connection_string_2 TO {backup_destination}",
    )
    print(get_azure_file_content("test_simple_write_nc_backup_2/.backup", port))
    azure_query(
        node,
        f"RESTORE TABLE test_write_connection_string_2 AS test_write_connection_string_restored_2 FROM {backup_destination};",
    )
    assert (
        azure_query(node, f"SELECT * from test_write_connection_string_restored_2")
        == "1\ta\n"
    )


def test_backup_restore_on_merge_tree(cluster):
    node = cluster.instances["node"]
    azure_query(
        node,
        f"""
        DROP TABLE IF EXISTS test_simple_merge_tree;
        CREATE TABLE test_simple_merge_tree(key UInt64, data String) Engine = MergeTree() ORDER BY tuple() SETTINGS storage_policy='blob_storage_policy'
        """,
    )
    azure_query(node, f"INSERT INTO test_simple_merge_tree VALUES (1, 'a')")

    backup_destination = f"AzureBlobStorage('{cluster.env_variables['AZURITE_CONNECTION_STRING']}', 'cont', 'test_simple_merge_tree_backup')"
    azure_query(
        node,
        f"BACKUP TABLE test_simple_merge_tree TO {backup_destination}",
    )
    azure_query(
        node,
        f"RESTORE TABLE test_simple_merge_tree AS test_simple_merge_tree_restored FROM {backup_destination};",
    )
    assert (
        azure_query(node, f"SELECT * from test_simple_merge_tree_restored") == "1\ta\n"
    )
    azure_query(node, f"DROP TABLE test_simple_merge_tree")
    azure_query(node, f"DROP TABLE test_simple_merge_tree_restored")


def test_backup_restore_correct_block_ids(cluster):
    node = cluster.instances["node"]
    azure_query(
        node,
        f"""
        DROP TABLE IF EXISTS test_simple_merge_tree;
        CREATE TABLE test_simple_merge_tree(key UInt64, data String)
        Engine = MergeTree()
        ORDER BY tuple()
        SETTINGS storage_policy='blob_storage_policy'""",
    )
    data_query = "SELECT number, repeat('a', 100) FROM numbers(1000)"
    azure_query(
        node,
        f"INSERT INTO test_simple_merge_tree {data_query}",
    )

    for min_upload_size, max_upload_size, max_blocks, expected_block_size in [
        (42, 100, 1000, 42),
        (42, 52, 86, 52),
    ]:
        data_path = f"test_backup_correct_block_ids_{max_blocks}"

        backup_destination = f"AzureBlobStorage('{cluster.env_variables['AZURITE_CONNECTION_STRING']}', 'cont', '{data_path}')"
        azure_query(
            node,
            f"""
            SET azure_min_upload_part_size = {min_upload_size};
            SET azure_max_upload_part_size = {max_upload_size};
            SET azure_max_blocks_in_multipart_upload = {max_blocks};
            BACKUP TABLE test_simple_merge_tree TO {backup_destination} SETTINGS allow_azure_native_copy = 0;
            """,
        )

        port = cluster.azurite_port
        connection_string = (
            f"DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;"
            f"AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;"
            f"BlobEndpoint=http://127.0.0.1:{port}/devstoreaccount1;"
        )
        container_name = "cont"
        blob_service_client = BlobServiceClient.from_connection_string(
            connection_string
        )
        container_client = blob_service_client.get_container_client(container_name)
        blobs = container_client.list_blobs()

        data_blob = (
            f"{data_path}/data/default/test_simple_merge_tree/all_1_1_0/data.bin"
        )
        found = False
        for blob in blobs:
            if data_blob == blob.get("name"):
                found = True
                break
        assert found

        blob_client = blob_service_client.get_blob_client(
            blob=data_blob, container=container_name
        )

        blocks_num = len(blob_client.get_block_list()[0])
        assert blocks_num > 50

        count = 0
        for block in blob_client.get_block_list()[0]:
            count += 1
            if count < blocks_num:
                assert block.get("size") == expected_block_size
            else:
                assert block.get("size") < expected_block_size

        azure_query(
            node,
            f"RESTORE TABLE test_simple_merge_tree AS test_simple_merge_tree_restored_{max_blocks} FROM {backup_destination};",
        )
        assert azure_query(
            node,
            f"SELECT * from test_simple_merge_tree_restored_{max_blocks} ORDER BY key",
        ) == node.query(data_query)
