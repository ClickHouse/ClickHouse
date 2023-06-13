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
from helpers.test_tools import TSV
from helpers.network import PartitionManager
from helpers.mock_servers import start_mock_servers
from helpers.test_tools import exec_query_with_retry


@pytest.fixture(scope="module")
def cluster():
    try:
        cluster = ClickHouseCluster(__file__)
        cluster.add_instance(
            "node_0",
            main_configs=["configs/named_collections.xml", "configs/cluster.xml"],
            with_azurite=True,
        )
        cluster.add_instance(
            "node_1",
            main_configs=["configs/named_collections.xml", "configs/cluster.xml"],
            with_azurite=True,
        )
        cluster.add_instance(
            "node_2",
            main_configs=["configs/named_collections.xml", "configs/cluster.xml"],
            with_azurite=True,
        )
        cluster.start()

        yield cluster
    finally:
        cluster.shutdown()


def azure_query(node, query, try_num=3, settings={}):
    for i in range(try_num):
        try:
            return node.query(query, settings=settings)
        except Exception as ex:
            retriable_errors = [
                "DB::Exception: Azure::Core::Http::TransportException: Connection was closed by the server while trying to read a response"
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
            continue


def get_azure_file_content(filename):
    container_name = "cont"
    connection_string = "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;"
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    container_client = blob_service_client.get_container_client(container_name)
    blob_client = container_client.get_blob_client(filename)
    download_stream = blob_client.download_blob()
    return download_stream.readall().decode("utf-8")


def test_select_all(cluster):
    node = cluster.instances["node_0"]
    azure_query(
        node,
        "INSERT INTO TABLE FUNCTION azureBlobStorage("
        "'http://azurite1:10000/devstoreaccount1', 'cont', 'test_cluster_select_all.csv', 'devstoreaccount1', "
        "'Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==', 'CSV', "
        "'auto', 'key UInt64, data String') VALUES (1, 'a'), (2, 'b')",
    )
    print(get_azure_file_content("test_cluster_select_all.csv"))

    pure_azure = node.query(
        """
    SELECT * from azureBlobStorage(
        'http://azurite1:10000/devstoreaccount1', 'cont', 'test_cluster_select_all.csv', 'devstoreaccount1',
        'Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==', 'CSV',
        'auto')"""
    )
    print(pure_azure)
    distributed_azure = node.query(
        """
    SELECT * from azureBlobStorageCluster(
        'simple_cluster', 'http://azurite1:10000/devstoreaccount1', 'cont', 'test_cluster_select_all.csv', 'devstoreaccount1',
        'Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==', 'CSV',
        'auto')"""
    )
    print(distributed_azure)
    assert TSV(pure_azure) == TSV(distributed_azure)


def test_count(cluster):
    node = cluster.instances["node_0"]
    azure_query(
        node,
        "INSERT INTO TABLE FUNCTION azureBlobStorage("
        "'http://azurite1:10000/devstoreaccount1', 'cont', 'test_cluster_count.csv', 'devstoreaccount1', "
        "'Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==', 'CSV', "
        "'auto', 'key UInt64') VALUES (1), (2)",
    )
    print(get_azure_file_content("test_cluster_count.csv"))

    pure_azure = node.query(
        """
    SELECT count(*) from azureBlobStorage(
        'http://azurite1:10000/devstoreaccount1', 'cont', 'test_cluster_count.csv', 'devstoreaccount1',
        'Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==', 'CSV',
        'auto', 'key UInt64')"""
    )
    print(pure_azure)
    distributed_azure = node.query(
        """
    SELECT count(*) from azureBlobStorageCluster(
        'simple_cluster', 'http://azurite1:10000/devstoreaccount1', 'cont', 'test_cluster_count.csv', 'devstoreaccount1',
        'Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==', 'CSV',
        'auto', 'key UInt64')"""
    )
    print(distributed_azure)
    assert TSV(pure_azure) == TSV(distributed_azure)


def test_union_all(cluster):
    node = cluster.instances["node_0"]
    azure_query(
        node,
        "INSERT INTO TABLE FUNCTION azureBlobStorage("
        "'http://azurite1:10000/devstoreaccount1', 'cont', 'test_parquet_union_all', 'devstoreaccount1', "
        "'Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==', 'Parquet', "
        "'auto', 'a Int32, b String') VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd')",
    )

    pure_azure = node.query(
        """
    SELECT * FROM
    (
        SELECT * from azureBlobStorage(
            'http://azurite1:10000/devstoreaccount1', 'cont', 'test_parquet_union_all', 'devstoreaccount1', 
            'Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==', 'Parquet', 
            'auto', 'a Int32, b String')
        UNION ALL
        SELECT * from azureBlobStorage(
            'http://azurite1:10000/devstoreaccount1', 'cont', 'test_parquet_union_all', 'devstoreaccount1', 
            'Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==', 'Parquet', 
            'auto', 'a Int32, b String')
    )
    ORDER BY (a)
    """
    )
    azure_distributed = node.query(
        """
    SELECT * FROM
    (
        SELECT * from azureBlobStorageCluster(
            'simple_cluster',
            'http://azurite1:10000/devstoreaccount1', 'cont', 'test_parquet_union_all', 'devstoreaccount1', 
            'Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==', 'Parquet', 
            'auto', 'a Int32, b String')
        UNION ALL
        SELECT * from azureBlobStorageCluster(
            'simple_cluster',
            'http://azurite1:10000/devstoreaccount1', 'cont', 'test_parquet_union_all', 'devstoreaccount1', 
            'Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==', 'Parquet', 
            'auto', 'a Int32, b String')
    )
    ORDER BY (a)
    """
    )

    assert TSV(pure_azure) == TSV(azure_distributed)

def test_skip_unavailable_shards(cluster):
    node = cluster.instances["node_0"]
    azure_query(
        node,
        "INSERT INTO TABLE FUNCTION azureBlobStorage("
        "'http://azurite1:10000/devstoreaccount1', 'cont', 'test_skip_unavailable.csv', 'devstoreaccount1', "
        "'Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==', 'auto', "
        "'auto', 'a UInt64') VALUES (1), (2)",
    )
    result = node.query(
        """
    SELECT count(*) from azureBlobStorageCluster(
        'cluster_non_existent_port',
        'http://azurite1:10000/devstoreaccount1', 'cont', 'test_skip_unavailable.csv', 'devstoreaccount1',
        'Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==')
    SETTINGS skip_unavailable_shards = 1
    """
    )

    assert result == "2\n"


def test_unset_skip_unavailable_shards(cluster):
    # Although skip_unavailable_shards is not set, cluster table functions should always skip unavailable shards.
    node = cluster.instances["node_0"]
    azure_query(
        node,
        "INSERT INTO TABLE FUNCTION azureBlobStorage("
        "'http://azurite1:10000/devstoreaccount1', 'cont', 'test_unset_skip_unavailable.csv', 'devstoreaccount1', "
        "'Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==', 'auto', "
        "'auto', 'a UInt64') VALUES (1), (2)",
    )
    result = node.query(
        """
    SELECT count(*) from azureBlobStorageCluster(
        'cluster_non_existent_port',
        'http://azurite1:10000/devstoreaccount1', 'cont', 'test_skip_unavailable.csv', 'devstoreaccount1',
        'Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==')
    """
    )

    assert result == "2\n"

def test_cluster_with_named_collection(cluster):
    node = cluster.instances["node_0"]

    azure_query(
        node,
        "INSERT INTO TABLE FUNCTION azureBlobStorage("
        "'http://azurite1:10000/devstoreaccount1', 'cont', 'test_cluster_with_named_collection.csv', 'devstoreaccount1', "
        "'Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==', 'auto', "
        "'auto', 'a UInt64') VALUES (1), (2)",
    )

    pure_azure = node.query(
        """
    SELECT * from azureBlobStorage(
        'http://azurite1:10000/devstoreaccount1', 'cont', 'test_cluster_with_named_collection.csv', 'devstoreaccount1',
        'Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==')
    """
    )

    azure_cluster = node.query(
        """
    SELECT * from azureBlobStorageCluster(
        'simple_cluster', azure_conf2, container='cont', blob_path='test_cluster_with_named_collection.csv')
    """
    )

    assert TSV(pure_azure) == TSV(azure_cluster)