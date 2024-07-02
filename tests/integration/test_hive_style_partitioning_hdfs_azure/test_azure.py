#!/usr/bin/env python3

import pytest
import time

from helpers.cluster import ClickHouseCluster, is_arm
import re

from azure.storage.blob import BlobServiceClient
from helpers.cluster import ClickHouseCluster, ClickHouseInstance

if is_arm():
    pytestmark = pytest.mark.skip

@pytest.fixture(scope="module")
def cluster():
    try:
        cluster = ClickHouseCluster(__file__)
        cluster.add_instance(
            "node",
            main_configs=["configs/named_collections_azure.xml", "configs/schema_cache_azure.xml"],
            user_configs=["configs/disable_profilers_azure.xml", "configs/users_azure.xml"],
            with_azurite=True,
        )
        cluster.start()
        container_client = cluster.blob_service_client.get_container_client("cont")
        container_client.create_container()
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


def test_azure_partitioning_with_one_parameter(cluster):
    # type: (ClickHouseCluster) -> None
    node = cluster.instances["node"]  # type: ClickHouseInstance
    table_format = "column1 String, column2 String"
    values = f"('Elizabeth', 'Gordon')"
    path = "a/column1=Elizabeth/sample.csv"

    azure_query(
        node,
        f"INSERT INTO TABLE FUNCTION azureBlobStorage(azure_conf2, storage_account_url = '{cluster.env_variables['AZURITE_STORAGE_ACCOUNT_URL']}',"
        f" container='cont', blob_path='{path}', format='CSV', compression='auto', structure='{table_format}') VALUES {values}",
    )

    query = (
        f"SELECT column1, column2, _file, _path, _column1 FROM azureBlobStorage(azure_conf2, "
        f"storage_account_url = '{cluster.env_variables['AZURITE_STORAGE_ACCOUNT_URL']}', container='cont', "
        f"blob_path='{path}', format='CSV', structure='{table_format}')"
    )
    assert azure_query(node, query, settings={"azure_blob_storage_hive_partitioning": 1}).splitlines() == [
        "Elizabeth\tGordon\tsample.csv\t{bucket}/{max_path}\tElizabeth".format(
            bucket="cont", max_path=path
        )
    ]

    query = (
        f"SELECT column2 FROM azureBlobStorage(azure_conf2, "
        f"storage_account_url = '{cluster.env_variables['AZURITE_STORAGE_ACCOUNT_URL']}', container='cont', "
        f"blob_path='{path}', format='CSV', structure='{table_format}') WHERE column1=_column1;"
    )
    assert azure_query(node, query, settings={"azure_blob_storage_hive_partitioning": 1}).splitlines() == [
        "Gordon"
    ]

def test_azure_partitioning_with_two_parameters(cluster):
    # type: (ClickHouseCluster) -> None
    node = cluster.instances["node"]  # type: ClickHouseInstance
    table_format = "column1 String, column2 String"
    values_1 = f"('Elizabeth', 'Gordon')"
    values_2 = f"('Emilia', 'Gregor')"
    path = "a/column1=Elizabeth/column2=Gordon/sample.csv"

    azure_query(
        node,
        f"INSERT INTO TABLE FUNCTION azureBlobStorage(azure_conf2, storage_account_url = '{cluster.env_variables['AZURITE_STORAGE_ACCOUNT_URL']}',"
        f" container='cont', blob_path='{path}', format='CSV', compression='auto', structure='{table_format}') VALUES {values_1}, {values_2}",
    )

    query = (
        f"SELECT column1, column2, _file, _path, _column1, _column2 FROM azureBlobStorage(azure_conf2, "
        f"storage_account_url = '{cluster.env_variables['AZURITE_STORAGE_ACCOUNT_URL']}', container='cont', "
        f"blob_path='{path}', format='CSV', structure='{table_format}') WHERE column1=_column1;"
    )
    assert azure_query(node, query, settings={"azure_blob_storage_hive_partitioning": 1}).splitlines() == [
        "Elizabeth\tGordon\tsample.csv\t{bucket}/{max_path}\tElizabeth\tGordon".format(
            bucket="cont", max_path=path
        )
    ]

    query = (
        f"SELECT column1 FROM azureBlobStorage(azure_conf2, "
        f"storage_account_url = '{cluster.env_variables['AZURITE_STORAGE_ACCOUNT_URL']}', container='cont', "
        f"blob_path='{path}', format='CSV', structure='{table_format}') WHERE column2=_column2;"
    )
    assert azure_query(node, query, settings={"azure_blob_storage_hive_partitioning": 1}).splitlines() == [
        "Elizabeth"
    ]

    query = (
        f"SELECT column1 FROM azureBlobStorage(azure_conf2, "
        f"storage_account_url = '{cluster.env_variables['AZURITE_STORAGE_ACCOUNT_URL']}', container='cont', "
        f"blob_path='{path}', format='CSV', structure='{table_format}') WHERE column2=_column2 AND column1=_column1;"
    )
    assert azure_query(node, query, settings={"azure_blob_storage_hive_partitioning": 1}).splitlines() == [
        "Elizabeth"
    ]

def test_azure_partitioning_without_setting(cluster):
    # type: (ClickHouseCluster) -> None
    node = cluster.instances["node"]  # type: ClickHouseInstance
    table_format = "column1 String, column2 String"
    values_1 = f"('Elizabeth', 'Gordon')"
    values_2 = f"('Emilia', 'Gregor')"
    path = "a/column1=Elizabeth/column2=Gordon/sample.csv"

    azure_query(
        node,
        f"INSERT INTO TABLE FUNCTION azureBlobStorage(azure_conf2, storage_account_url = '{cluster.env_variables['AZURITE_STORAGE_ACCOUNT_URL']}',"
        f" container='cont', blob_path='{path}', format='CSV', compression='auto', structure='{table_format}') VALUES {values_1}, {values_2}",
    )

    query = (
        f"SELECT column1, column2, _file, _path, _column1, _column2 FROM azureBlobStorage(azure_conf2, "
        f"storage_account_url = '{cluster.env_variables['AZURITE_STORAGE_ACCOUNT_URL']}', container='cont', "
        f"blob_path='{path}', format='CSV', structure='{table_format}') WHERE column1=_column1;"
    )
    pattern = re.compile(r"DB::Exception: Unknown expression identifier '.*' in scope.*", re.DOTALL)

    with pytest.raises(Exception, match=pattern):
        azure_query(node, query, settings={"azure_blob_storage_hive_partitioning": 0})
