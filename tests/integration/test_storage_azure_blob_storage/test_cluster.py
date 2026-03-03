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
from helpers.test_tools import TSV, exec_query_with_retry
from test_storage_azure_blob_storage.test import azure_query


@pytest.fixture(scope="module")
def cluster():
    try:
        cluster = ClickHouseCluster(__file__)
        cluster.add_instance(
            "node_0",
            main_configs=["configs/named_collections.xml", "configs/cluster.xml"],
            user_configs=["configs/disable_profilers.xml", "configs/users.xml"],
            with_azurite=True,
        )
        cluster.add_instance(
            "node_1",
            main_configs=["configs/named_collections.xml", "configs/cluster.xml"],
            user_configs=["configs/disable_profilers.xml", "configs/users.xml"],
            with_azurite=True,
        )
        cluster.add_instance(
            "node_2",
            main_configs=["configs/named_collections.xml", "configs/cluster.xml"],
            user_configs=["configs/disable_profilers.xml", "configs/users.xml"],
            with_azurite=True,
        )
        cluster.start()

        yield cluster
    finally:
        cluster.shutdown()


def get_azure_file_content(filename, port):
    container_name = "cont"
    connection_string = (
        f"DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;"
        f"AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;"
        f"BlobEndpoint=http://127.0.0.1:{port}/devstoreaccount1;"
    )
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    container_client = blob_service_client.get_container_client(container_name)
    blob_client = container_client.get_blob_client(filename)
    download_stream = blob_client.download_blob()
    return download_stream.readall().decode("utf-8")


def test_select_all(cluster):
    node = cluster.instances["node_0"]
    port = cluster.env_variables["AZURITE_PORT"]
    storage_account_url = cluster.env_variables["AZURITE_STORAGE_ACCOUNT_URL"]
    azure_query(
        node,
        f"INSERT INTO TABLE FUNCTION azureBlobStorage('{storage_account_url}', 'cont', 'test_cluster_select_all.csv', 'devstoreaccount1',"
        f"'Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==', 'CSV', 'auto', 'key UInt64, data String') "
        f"VALUES (1, 'a'), (2, 'b')",
        settings={"azure_truncate_on_insert": 1},
    )
    print(get_azure_file_content("test_cluster_select_all.csv", port))

    pure_azure = azure_query(
        node,
        f"SELECT * from azureBlobStorage('{storage_account_url}', 'cont', 'test_cluster_select_all.csv', 'devstoreaccount1',"
        f"'Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==', 'CSV','auto')",
    )
    print(pure_azure)
    distributed_azure = azure_query(
        node,
        f"SELECT * from azureBlobStorageCluster('simple_cluster', '{storage_account_url}', 'cont', 'test_cluster_select_all.csv', 'devstoreaccount1',"
        f"'Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==', 'CSV',"
        f"'auto')"
        "",
    )
    print(distributed_azure)
    assert TSV(pure_azure) == TSV(distributed_azure)


def test_count(cluster):
    node = cluster.instances["node_0"]
    port = cluster.env_variables["AZURITE_PORT"]
    storage_account_url = cluster.env_variables["AZURITE_STORAGE_ACCOUNT_URL"]
    azure_query(
        node,
        f"INSERT INTO TABLE FUNCTION azureBlobStorage('{storage_account_url}', 'cont', 'test_cluster_count.csv', 'devstoreaccount1', "
        f"'Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==', 'CSV', "
        f"'auto', 'key UInt64') VALUES (1), (2)",
        settings={"azure_truncate_on_insert": 1},
    )
    print(get_azure_file_content("test_cluster_count.csv", port))

    pure_azure = azure_query(
        node,
        f"SELECT count(*) from azureBlobStorage('{storage_account_url}', 'cont', 'test_cluster_count.csv', 'devstoreaccount1',"
        f"'Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==', 'CSV',"
        f"'auto', 'key UInt64')",
    )
    print(pure_azure)
    distributed_azure = azure_query(
        node,
        f"SELECT count(*) from azureBlobStorageCluster('simple_cluster', '{storage_account_url}', 'cont', 'test_cluster_count.csv', "
        f"'devstoreaccount1','Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==', 'CSV',"
        f"'auto', 'key UInt64')",
    )
    print(distributed_azure)
    assert TSV(pure_azure) == TSV(distributed_azure)


def test_union_all(cluster):
    node = cluster.instances["node_0"]
    storage_account_url = cluster.env_variables["AZURITE_STORAGE_ACCOUNT_URL"]
    azure_query(
        node,
        f"INSERT INTO TABLE FUNCTION azureBlobStorage('{storage_account_url}', 'cont', 'test_parquet_union_all', 'devstoreaccount1', "
        f"'Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==', 'Parquet', "
        f"'auto', 'a Int32, b String') VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd')",
        settings={"azure_truncate_on_insert": 1},
    )

    pure_azure = azure_query(
        node,
        f"""
    SELECT * FROM
    (
        SELECT * from azureBlobStorage('{storage_account_url}', 'cont', 'test_parquet_union_all', 'devstoreaccount1',
            'Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==', 'Parquet',
            'auto', 'a Int32, b String')
        UNION ALL
        SELECT * from azureBlobStorage(
            '{storage_account_url}', 'cont', 'test_parquet_union_all', 'devstoreaccount1',
            'Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==', 'Parquet',
            'auto', 'a Int32, b String')
    )
    ORDER BY (a)
    """,
    )
    azure_distributed = azure_query(
        node,
        f"""
    SELECT * FROM
    (
        SELECT * from azureBlobStorageCluster(
            'simple_cluster',
            '{storage_account_url}', 'cont', 'test_parquet_union_all', 'devstoreaccount1',
            'Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==', 'Parquet',
            'auto', 'a Int32, b String')
        UNION ALL
        SELECT * from azureBlobStorageCluster(
            'simple_cluster',
            '{storage_account_url}', 'cont', 'test_parquet_union_all', 'devstoreaccount1',
            'Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==', 'Parquet',
            'auto', 'a Int32, b String')
    )
    ORDER BY (a)
    """,
    )

    assert TSV(pure_azure) == TSV(azure_distributed)


def test_skip_unavailable_shards(cluster):
    node = cluster.instances["node_0"]
    storage_account_url = cluster.env_variables["AZURITE_STORAGE_ACCOUNT_URL"]
    azure_query(
        node,
        f"INSERT INTO TABLE FUNCTION azureBlobStorage('{storage_account_url}', 'cont', 'test_skip_unavailable.csv', 'devstoreaccount1', "
        f"'Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==', 'auto', "
        f"'auto', 'a UInt64') VALUES (1), (2)",
        settings={"azure_truncate_on_insert": 1},
    )
    result = azure_query(
        node,
        f"SELECT count(*) from azureBlobStorageCluster('cluster_non_existent_port','{storage_account_url}', 'cont', 'test_skip_unavailable.csv', "
        f"'devstoreaccount1','Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==') "
        f"SETTINGS skip_unavailable_shards = 1",
    )

    assert result == "2\n"


def test_unset_skip_unavailable_shards(cluster):
    # Although skip_unavailable_shards is not set, cluster table functions should always skip unavailable shards.
    node = cluster.instances["node_0"]
    storage_account_url = cluster.env_variables["AZURITE_STORAGE_ACCOUNT_URL"]
    azure_query(
        node,
        f"INSERT INTO TABLE FUNCTION azureBlobStorage('{storage_account_url}', 'cont', 'test_unset_skip_unavailable.csv', 'devstoreaccount1', "
        f"'Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==', 'auto', "
        f"'auto', 'a UInt64') VALUES (1), (2)",
        settings={"azure_truncate_on_insert": 1},
    )
    result = azure_query(
        node,
        f"SELECT count(*) from azureBlobStorageCluster('cluster_non_existent_port','{storage_account_url}', 'cont', 'test_unset_skip_unavailable.csv', "
        f"'devstoreaccount1','Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==')",
    )

    assert result == "2\n"


def test_cluster_with_named_collection(cluster):
    node = cluster.instances["node_0"]
    storage_account_url = cluster.env_variables["AZURITE_STORAGE_ACCOUNT_URL"]
    azure_query(
        node,
        f"INSERT INTO TABLE FUNCTION azureBlobStorage('{storage_account_url}', 'cont', 'test_cluster_with_named_collection.csv', 'devstoreaccount1', "
        f"'Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==', 'auto', "
        f"'auto', 'a UInt64') VALUES (1), (2)",
        settings={"azure_truncate_on_insert": 1},
    )

    pure_azure = azure_query(
        node,
        f"SELECT * from azureBlobStorage('{storage_account_url}', 'cont', 'test_cluster_with_named_collection.csv', 'devstoreaccount1',"
        f"'Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==')",
    )

    azure_cluster = azure_query(
        node,
        f"SELECT * from azureBlobStorageCluster('simple_cluster', azure_conf2, storage_account_url = '{storage_account_url}', container='cont', "
        f"blob_path='test_cluster_with_named_collection.csv')",
    )

    assert TSV(pure_azure) == TSV(azure_cluster)


def test_partition_parallel_reading_with_cluster(cluster):
    node = cluster.instances["node_0"]
    table_format = "column1 UInt32, column2 UInt32, column3 UInt32"
    partition_by = "column3"
    values = "(1, 2, 3), (3, 2, 1), (78, 43, 45)"
    filename = "test_tf_{_partition_id}.csv"
    port = cluster.env_variables["AZURITE_PORT"]
    storage_account_url = cluster.env_variables["AZURITE_STORAGE_ACCOUNT_URL"]

    azure_query(
        node,
        f"INSERT INTO TABLE FUNCTION azureBlobStorage('{storage_account_url}', 'cont', '{filename}', 'devstoreaccount1', "
        f"'Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==', 'CSV', 'auto', '{table_format}') "
        f"PARTITION BY {partition_by} VALUES {values}",
        settings={"azure_truncate_on_insert": 1},
    )

    assert "1,2,3\n" == get_azure_file_content("test_tf_3.csv", port)
    assert "3,2,1\n" == get_azure_file_content("test_tf_1.csv", port)
    assert "78,43,45\n" == get_azure_file_content("test_tf_45.csv", port)

    azure_cluster = azure_query(
        node,
        f"SELECT count(*) from azureBlobStorageCluster('simple_cluster', azure_conf2, storage_account_url = '{storage_account_url}', "
        f"container='cont', blob_path='test_tf_*.csv', format='CSV', compression='auto', structure='column1 UInt32, column2 UInt32, column3 UInt32')",
    )

    assert azure_cluster == "3\n"


def test_format_detection(cluster):
    node = cluster.instances["node_0"]
    storage_account_url = cluster.env_variables["AZURITE_STORAGE_ACCOUNT_URL"]
    account_name = "devstoreaccount1"
    account_key = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="

    azure_query(
        node,
        f"INSERT INTO TABLE FUNCTION azureBlobStorage('{storage_account_url}', 'cont', 'test_format_detection0', '{account_name}', '{account_key}', 'JSONEachRow', 'auto', 'x UInt32, y String') select number as x, 'str_' || toString(number) from numbers(10) SETTINGS azure_truncate_on_insert=1",
    )

    azure_query(
        node,
        f"INSERT INTO TABLE FUNCTION azureBlobStorage('{storage_account_url}', 'cont', 'test_format_detection1', '{account_name}', '{account_key}', 'JSONEachRow', 'auto', 'x UInt32, y String') select number as x, 'str_' || toString(number) from numbers(10, 10) SETTINGS azure_truncate_on_insert=1",
    )

    expected_desc_result = azure_query(
        node,
        f"desc azureBlobStorage('{storage_account_url}', 'cont', 'test_format_detection*', '{account_name}', '{account_key}', 'JSONEachRow', 'auto', 'auto')",
    )

    desc_result = azure_query(
        node,
        f"desc azureBlobStorageCluster('simple_cluster', '{storage_account_url}', 'cont', 'test_format_detection*', '{account_name}', '{account_key}')",
    )

    assert expected_desc_result == desc_result

    expected_result = azure_query(
        node,
        f"select * from azureBlobStorage('{storage_account_url}', 'cont', 'test_format_detection*', '{account_name}', '{account_key}', 'JSONEachRow', 'auto', 'x UInt32, y String') order by x",
    )

    result = azure_query(
        node,
        f"select * from azureBlobStorageCluster('simple_cluster', '{storage_account_url}', 'cont', 'test_format_detection*', '{account_name}', '{account_key}') order by x",
    )

    assert result == expected_result

    result = azure_query(
        node,
        f"select * from azureBlobStorageCluster('simple_cluster', '{storage_account_url}', 'cont', 'test_format_detection*', '{account_name}', '{account_key}', auto) order by x",
    )

    assert result == expected_result

    result = azure_query(
        node,
        f"select * from azureBlobStorageCluster('simple_cluster', '{storage_account_url}', 'cont', 'test_format_detection*', '{account_name}', '{account_key}', auto, auto) order by x",
    )

    assert result == expected_result

    result = azure_query(
        node,
        f"select * from azureBlobStorageCluster('simple_cluster', '{storage_account_url}', 'cont', 'test_format_detection*', '{account_name}', '{account_key}', 'x UInt32, y String') order by x",
    )

    assert result == expected_result

    result = azure_query(
        node,
        f"select * from azureBlobStorageCluster('simple_cluster', '{storage_account_url}', 'cont', 'test_format_detection*', '{account_name}', '{account_key}', auto, auto, 'x UInt32, y String') order by x",
    )

    assert result == expected_result
