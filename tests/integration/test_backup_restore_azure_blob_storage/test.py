#!/usr/bin/env python3

import gzip
import json
import logging
import os
import io
import random
import threading
import time

from azure.storage.blob import BlobServiceClient
import helpers.client
import pytest
from helpers.cluster import ClickHouseCluster, ClickHouseInstance
from helpers.network import PartitionManager
from helpers.mock_servers import start_mock_servers
from helpers.test_tools import exec_query_with_retry


@pytest.fixture(scope="module")
def cluster():
    try:
        cluster = ClickHouseCluster(__file__)
        cluster.add_instance(
            "node",
            with_azurite=True,
        )
        cluster.start()

        yield cluster
    finally:
        cluster.shutdown()


def azure_query(
    node, query, expect_error="false", try_num=10, settings={}, query_on_retry=None
):
    for i in range(try_num):
        try:
            if expect_error == "true":
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


@pytest.fixture(autouse=True, scope="function")
def delete_all_files(cluster):
    port = cluster.env_variables["AZURITE_PORT"]
    connection_string = (
        f"DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;"
        f"AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;"
        f"BlobEndpoint=http://127.0.0.1:{port}/devstoreaccount1;"
    )
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    containers = blob_service_client.list_containers()
    for container in containers:
        container_client = blob_service_client.get_container_client(container)
        blob_list = container_client.list_blobs()
        for blob in blob_list:
            print(blob)
            blob_client = container_client.get_blob_client(blob)
            blob_client.delete_blob()

        assert len(list(container_client.list_blobs())) == 0

    yield


def test_create_table_connection_string(cluster):
    node = cluster.instances["node"]
    azure_query(
        node,
        f"CREATE TABLE test_create_table_conn_string (key UInt64, data String) Engine = AzureBlobStorage('{cluster.env_variables['AZURITE_CONNECTION_STRING']}', 'cont', 'test_create_connection_string', 'CSV')",
    )


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

    backup_destination = f"AzureBlobStorage('{cluster.env_variables['AZURITE_CONNECTION_STRING']}', 'cont', 'test_simple_write_c_backup.csv')"
    azure_query(
        node,
        f"BACKUP TABLE test_simple_write_connection_string TO {backup_destination}",
    )
    print(get_azure_file_content("test_simple_write_c_backup.csv.backup", port))
    azure_query(
        node,
        f"RESTORE TABLE test_simple_write_connection_string AS test_simple_write_connection_string_restored FROM {backup_destination};",
    )
    assert (
        azure_query(node, f"SELECT * from test_simple_write_connection_string_restored")
        == "1\ta\n"
    )
