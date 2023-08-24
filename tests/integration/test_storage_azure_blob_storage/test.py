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
            main_configs=["configs/named_collections.xml", "configs/schema_cache.xml"],
            user_configs=["configs/disable_profilers.xml", "configs/users.xml"],
            with_azurite=True,
        )
        cluster.start()

        yield cluster
    finally:
        cluster.shutdown()


def azure_query(node, query, expect_error="false", try_num=10, settings={}):
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
            continue


def get_azure_file_content(filename):
    container_name = "cont"
    connection_string = "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;"
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    container_client = blob_service_client.get_container_client(container_name)
    blob_client = container_client.get_blob_client(filename)
    download_stream = blob_client.download_blob()
    return download_stream.readall().decode("utf-8")


def put_azure_file_content(filename, data):
    container_name = "cont"
    connection_string = "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;"
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    try:
        container_client = blob_service_client.create_container(container_name)
    except:
        container_client = blob_service_client.get_container_client(container_name)

    blob_client = container_client.get_blob_client(filename)
    buf = io.BytesIO(data)
    blob_client.upload_blob(buf)


def test_create_table_connection_string(cluster):
    node = cluster.instances["node"]
    azure_query(
        node,
        "CREATE TABLE test_create_table_conn_string (key UInt64, data String) Engine = AzureBlobStorage('DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://azurite1:10000/devstoreaccount1/;', 'cont', 'test_create_connection_string', 'CSV')",
    )


def test_create_table_account_string(cluster):
    node = cluster.instances["node"]
    azure_query(
        node,
        "CREATE TABLE test_create_table_account_url (key UInt64, data String) Engine = AzureBlobStorage('http://azurite1:10000/devstoreaccount1',  'cont', 'test_create_connection_string', 'devstoreaccount1', 'Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==', 'CSV')",
    )


def test_simple_write_account_string(cluster):
    node = cluster.instances["node"]
    azure_query(
        node,
        "CREATE TABLE test_simple_write (key UInt64, data String) Engine = AzureBlobStorage('http://azurite1:10000/devstoreaccount1', 'cont', 'test_simple_write.csv', 'devstoreaccount1', 'Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==', 'CSV')",
    )
    azure_query(node, "INSERT INTO test_simple_write VALUES (1, 'a')")
    print(get_azure_file_content("test_simple_write.csv"))
    assert get_azure_file_content("test_simple_write.csv") == '1,"a"\n'


def test_simple_write_connection_string(cluster):
    node = cluster.instances["node"]
    azure_query(
        node,
        "CREATE TABLE test_simple_write_connection_string (key UInt64, data String) Engine = AzureBlobStorage('DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://azurite1:10000/devstoreaccount1;', 'cont', 'test_simple_write_c.csv', 'CSV')",
    )
    azure_query(node, "INSERT INTO test_simple_write_connection_string VALUES (1, 'a')")
    print(get_azure_file_content("test_simple_write_c.csv"))
    assert get_azure_file_content("test_simple_write_c.csv") == '1,"a"\n'


def test_simple_write_named_collection_1(cluster):
    node = cluster.instances["node"]
    azure_query(
        node,
        "CREATE TABLE test_simple_write_named_collection_1 (key UInt64, data String) Engine = AzureBlobStorage(azure_conf1)",
    )
    azure_query(
        node, "INSERT INTO test_simple_write_named_collection_1 VALUES (1, 'a')"
    )
    print(get_azure_file_content("test_simple_write_named.csv"))
    assert get_azure_file_content("test_simple_write_named.csv") == '1,"a"\n'
    azure_query(node, "TRUNCATE TABLE test_simple_write_named_collection_1")


def test_simple_write_named_collection_2(cluster):
    node = cluster.instances["node"]
    azure_query(
        node,
        "CREATE TABLE test_simple_write_named_collection_2 (key UInt64, data String) Engine = AzureBlobStorage(azure_conf2, container='cont', blob_path='test_simple_write_named_2.csv', format='CSV')",
    )
    azure_query(
        node, "INSERT INTO test_simple_write_named_collection_2 VALUES (1, 'a')"
    )
    print(get_azure_file_content("test_simple_write_named_2.csv"))
    assert get_azure_file_content("test_simple_write_named_2.csv") == '1,"a"\n'


def test_partition_by(cluster):
    node = cluster.instances["node"]
    table_format = "column1 UInt32, column2 UInt32, column3 UInt32"
    partition_by = "column3"
    values = "(1, 2, 3), (3, 2, 1), (78, 43, 45)"
    filename = "test_{_partition_id}.csv"

    azure_query(
        node,
        f"CREATE TABLE test_partitioned_write ({table_format}) Engine = AzureBlobStorage('http://azurite1:10000/devstoreaccount1', 'cont', '{filename}', 'devstoreaccount1', 'Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==', 'CSV') PARTITION BY {partition_by}",
    )
    azure_query(node, f"INSERT INTO test_partitioned_write VALUES {values}")

    assert "1,2,3\n" == get_azure_file_content("test_3.csv")
    assert "3,2,1\n" == get_azure_file_content("test_1.csv")
    assert "78,43,45\n" == get_azure_file_content("test_45.csv")


def test_partition_by_string_column(cluster):
    node = cluster.instances["node"]
    table_format = "col_num UInt32, col_str String"
    partition_by = "col_str"
    values = "(1, 'foo/bar'), (3, 'йцук'), (78, '你好')"
    filename = "test_{_partition_id}.csv"
    azure_query(
        node,
        f"CREATE TABLE test_partitioned_string_write ({table_format}) Engine = AzureBlobStorage('http://azurite1:10000/devstoreaccount1', 'cont', '{filename}', 'devstoreaccount1', 'Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==', 'CSV') PARTITION BY {partition_by}",
    )
    azure_query(node, f"INSERT INTO test_partitioned_string_write VALUES {values}")

    assert '1,"foo/bar"\n' == get_azure_file_content("test_foo/bar.csv")
    assert '3,"йцук"\n' == get_azure_file_content("test_йцук.csv")
    assert '78,"你好"\n' == get_azure_file_content("test_你好.csv")


def test_partition_by_const_column(cluster):
    node = cluster.instances["node"]
    table_format = "column1 UInt32, column2 UInt32, column3 UInt32"
    values = "(1, 2, 3), (3, 2, 1), (78, 43, 45)"
    partition_by = "'88'"
    values_csv = "1,2,3\n3,2,1\n78,43,45\n"
    filename = "test_{_partition_id}.csv"
    azure_query(
        node,
        f"CREATE TABLE test_partitioned_const_write ({table_format}) Engine = AzureBlobStorage('http://azurite1:10000/devstoreaccount1', 'cont', '{filename}', 'devstoreaccount1', 'Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==', 'CSV') PARTITION BY {partition_by}",
    )
    azure_query(node, f"INSERT INTO test_partitioned_const_write VALUES {values}")
    assert values_csv == get_azure_file_content("test_88.csv")


def test_truncate(cluster):
    node = cluster.instances["node"]
    azure_query(
        node,
        "CREATE TABLE test_truncate (key UInt64, data String) Engine = AzureBlobStorage(azure_conf2, container='cont', blob_path='test_truncate.csv', format='CSV')",
    )
    azure_query(node, "INSERT INTO test_truncate VALUES (1, 'a')")
    assert get_azure_file_content("test_truncate.csv") == '1,"a"\n'
    azure_query(node, "TRUNCATE TABLE test_truncate")
    with pytest.raises(Exception):
        print(get_azure_file_content("test_truncate.csv"))


def test_simple_read_write(cluster):
    node = cluster.instances["node"]
    azure_query(
        node,
        "CREATE TABLE test_simple_read_write (key UInt64, data String) Engine = AzureBlobStorage(azure_conf2, container='cont', blob_path='test_simple_read_write.csv', format='CSV')",
    )

    azure_query(node, "INSERT INTO test_simple_read_write VALUES (1, 'a')")
    assert get_azure_file_content("test_simple_read_write.csv") == '1,"a"\n'
    print(azure_query(node, "SELECT * FROM test_simple_read_write"))
    assert azure_query(node, "SELECT * FROM test_simple_read_write") == "1\ta\n"


def test_create_new_files_on_insert(cluster):
    node = cluster.instances["node"]

    azure_query(
        node,
        f"create table test_multiple_inserts(a Int32, b String) ENGINE = AzureBlobStorage(azure_conf2, container='cont', blob_path='test_parquet', format='Parquet')",
    )
    azure_query(node, "truncate table test_multiple_inserts")
    azure_query(
        node,
        f"insert into test_multiple_inserts select number, randomString(100) from numbers(10) settings azure_truncate_on_insert=1",
    )
    azure_query(
        node,
        f"insert into test_multiple_inserts select number, randomString(100) from numbers(20) settings azure_create_new_file_on_insert=1",
    )
    azure_query(
        node,
        f"insert into test_multiple_inserts select number, randomString(100) from numbers(30) settings azure_create_new_file_on_insert=1",
    )

    result = azure_query(node, f"select count() from test_multiple_inserts")
    assert int(result) == 60

    azure_query(node, f"drop table test_multiple_inserts")


def test_overwrite(cluster):
    node = cluster.instances["node"]

    azure_query(
        node,
        f"create table test_overwrite(a Int32, b String) ENGINE = AzureBlobStorage(azure_conf2, container='cont', blob_path='test_parquet_overwrite', format='Parquet')",
    )
    azure_query(node, "truncate table test_overwrite")

    azure_query(
        node,
        f"insert into test_overwrite select number, randomString(100) from numbers(50) settings azure_truncate_on_insert=1",
    )
    node.query_and_get_error(
        f"insert into test_overwrite select number, randomString(100) from numbers(100)"
    )
    azure_query(
        node,
        f"insert into test_overwrite select number, randomString(100) from numbers(200) settings azure_truncate_on_insert=1",
    )

    result = azure_query(node, f"select count() from test_overwrite")
    assert int(result) == 200


def test_insert_with_path_with_globs(cluster):
    node = cluster.instances["node"]
    azure_query(
        node,
        f"create table test_insert_globs(a Int32, b String) ENGINE = AzureBlobStorage(azure_conf2, container='cont', blob_path='test_insert_with_globs*', format='Parquet')",
    )
    node.query_and_get_error(
        f"insert into table function test_insert_globs SELECT number, randomString(100) FROM numbers(500)"
    )


def test_put_get_with_globs(cluster):
    # type: (ClickHouseCluster) -> None
    unique_prefix = random.randint(1, 10000)
    node = cluster.instances["node"]  # type: ClickHouseInstance
    table_format = "column1 UInt32, column2 UInt32, column3 UInt32"
    max_path = ""
    for i in range(10):
        for j in range(10):
            path = "{}/{}_{}/{}.csv".format(
                unique_prefix, i, random.choice(["a", "b", "c", "d"]), j
            )
            max_path = max(path, max_path)
            values = f"({i},{j},{i + j})"

            azure_query(
                node,
                f"CREATE TABLE test_put_{i}_{j} ({table_format}) Engine = AzureBlobStorage(azure_conf2, container='cont', blob_path='{path}', format='CSV')",
            )

            query = f"insert into test_put_{i}_{j} VALUES {values}"
            azure_query(node, query)

    azure_query(
        node,
        f"CREATE TABLE test_glob_select ({table_format}) Engine = AzureBlobStorage(azure_conf2, container='cont', blob_path='{unique_prefix}/*_{{a,b,c,d}}/?.csv', format='CSV')",
    )
    query = "select sum(column1), sum(column2), sum(column3), min(_file), max(_path) from test_glob_select"
    assert azure_query(node, query).splitlines() == [
        "450\t450\t900\t0.csv\t{bucket}/{max_path}".format(
            bucket="cont", max_path=max_path
        )
    ]


def test_azure_glob_scheherazade(cluster):
    node = cluster.instances["node"]  # type: ClickHouseInstance
    table_format = "column1 UInt32, column2 UInt32, column3 UInt32"
    values = "(1, 1, 1)"
    nights_per_job = 1001 // 30
    jobs = []
    for night in range(0, 1001, nights_per_job):

        def add_tales(start, end):
            for i in range(start, end):
                path = "night_{}/tale.csv".format(i)
                unique_num = random.randint(1, 10000)
                azure_query(
                    node,
                    f"CREATE TABLE test_scheherazade_{i}_{unique_num} ({table_format}) Engine = AzureBlobStorage(azure_conf2, container='cont', blob_path='{path}', format='CSV')",
                )
                query = (
                    f"insert into test_scheherazade_{i}_{unique_num} VALUES {values}"
                )
                azure_query(node, query)

        jobs.append(
            threading.Thread(
                target=add_tales, args=(night, min(night + nights_per_job, 1001))
            )
        )
        jobs[-1].start()

    for job in jobs:
        job.join()

    azure_query(
        node,
        f"CREATE TABLE test_glob_select_scheherazade ({table_format}) Engine = AzureBlobStorage(azure_conf2, container='cont', blob_path='night_*/tale.csv', format='CSV')",
    )
    query = "select count(), sum(column1), sum(column2), sum(column3) from test_glob_select_scheherazade"
    assert azure_query(node, query).splitlines() == ["1001\t1001\t1001\t1001"]


@pytest.mark.parametrize(
    "extension,method",
    [pytest.param("bin", "gzip", id="bin"), pytest.param("gz", "auto", id="gz")],
)
def test_storage_azure_get_gzip(cluster, extension, method):
    node = cluster.instances["node"]
    filename = f"test_get_gzip.{extension}"
    name = f"test_get_gzip_{extension}"
    data = [
        "Sophia Intrieri,55",
        "Jack Taylor,71",
        "Christopher Silva,66",
        "Clifton Purser,35",
        "Richard Aceuedo,43",
        "Lisa Hensley,31",
        "Alice Wehrley,1",
        "Mary Farmer,47",
        "Samara Ramirez,19",
        "Shirley Lloyd,51",
        "Santos Cowger,0",
        "Richard Mundt,88",
        "Jerry Gonzalez,15",
        "Angela James,10",
        "Norman Ortega,33",
        "",
    ]
    azure_query(node, f"DROP TABLE IF EXISTS {name}")

    buf = io.BytesIO()
    compressed = gzip.GzipFile(fileobj=buf, mode="wb")
    compressed.write(("\n".join(data)).encode())
    compressed.close()
    put_azure_file_content(filename, buf.getvalue())

    azure_query(
        node,
        f"""CREATE TABLE {name} (name String, id UInt32) ENGINE = AzureBlobStorage(
                                azure_conf2, container='cont', blob_path ='{filename}',
                                format='CSV',
                                compression='{method}')""",
    )

    assert azure_query(node, f"SELECT sum(id) FROM {name}").splitlines() == ["565"]
    azure_query(node, f"DROP TABLE {name}")


def test_schema_inference_no_globs(cluster):
    node = cluster.instances["node"]  # type: ClickHouseInstance
    table_format = "column1 UInt32, column2 String, column3 UInt32"
    azure_query(
        node,
        f"CREATE TABLE test_schema_inference_src ({table_format}) Engine = AzureBlobStorage(azure_conf2, container='cont', blob_path='test_schema_inference_no_globs.csv', format='CSVWithNames')",
    )

    query = f"insert into test_schema_inference_src SELECT number, toString(number), number * number FROM numbers(1000)"
    azure_query(node, query)

    azure_query(
        node,
        f"CREATE TABLE test_select_inference Engine = AzureBlobStorage(azure_conf2, container='cont', blob_path='test_schema_inference_no_globs.csv')",
    )

    print(node.query("SHOW CREATE TABLE test_select_inference"))

    query = "select sum(column1), sum(length(column2)), sum(column3), min(_file), max(_path) from test_select_inference"
    assert azure_query(node, query).splitlines() == [
        "499500\t2890\t332833500\ttest_schema_inference_no_globs.csv\tcont/test_schema_inference_no_globs.csv"
    ]


def test_schema_inference_from_globs(cluster):
    node = cluster.instances["node"]
    unique_prefix = random.randint(1, 10000)
    node = cluster.instances["node"]  # type: ClickHouseInstance
    table_format = "column1 UInt32, column2 UInt32, column3 UInt32"
    max_path = ""
    for i in range(10):
        for j in range(10):
            path = "{}/{}_{}/{}.csv".format(
                unique_prefix, i, random.choice(["a", "b", "c", "d"]), j
            )
            max_path = max(path, max_path)
            values = f"({i},{j},{i + j})"

            azure_query(
                node,
                f"CREATE TABLE test_schema_{i}_{j} ({table_format}) Engine = AzureBlobStorage(azure_conf2, container='cont', blob_path='{path}', format='CSVWithNames')",
            )

            query = f"insert into test_schema_{i}_{j} VALUES {values}"
            azure_query(node, query)

    azure_query(
        node,
        f"CREATE TABLE test_glob_select_inference Engine = AzureBlobStorage(azure_conf2, container='cont', blob_path='{unique_prefix}/*_{{a,b,c,d}}/?.csv')",
    )

    print(node.query("SHOW CREATE TABLE test_glob_select_inference"))

    query = "select sum(column1), sum(column2), sum(column3), min(_file), max(_path) from test_glob_select_inference"
    assert azure_query(node, query).splitlines() == [
        "450\t450\t900\t0.csv\t{bucket}/{max_path}".format(
            bucket="cont", max_path=max_path
        )
    ]


def test_simple_write_account_string_table_function(cluster):
    node = cluster.instances["node"]
    azure_query(
        node,
        "INSERT INTO TABLE FUNCTION azureBlobStorage('http://azurite1:10000/devstoreaccount1', 'cont', 'test_simple_write_tf.csv', 'devstoreaccount1', 'Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==', 'CSV', 'auto', 'key UInt64, data String') VALUES (1, 'a')",
    )
    print(get_azure_file_content("test_simple_write_tf.csv"))
    assert get_azure_file_content("test_simple_write_tf.csv") == '1,"a"\n'


def test_simple_write_connection_string_table_function(cluster):
    node = cluster.instances["node"]
    azure_query(
        node,
        "INSERT INTO TABLE FUNCTION azureBlobStorage('DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://azurite1:10000/devstoreaccount1;', 'cont', 'test_simple_write_connection_tf.csv', 'CSV', 'auto', 'key UInt64, data String') VALUES (1, 'a')",
    )
    print(get_azure_file_content("test_simple_write_connection_tf.csv"))
    assert get_azure_file_content("test_simple_write_connection_tf.csv") == '1,"a"\n'


def test_simple_write_named_collection_1_table_function(cluster):
    node = cluster.instances["node"]
    azure_query(
        node,
        "INSERT INTO TABLE FUNCTION azureBlobStorage(azure_conf1) VALUES (1, 'a')",
    )
    print(get_azure_file_content("test_simple_write_named.csv"))
    assert get_azure_file_content("test_simple_write_named.csv") == '1,"a"\n'

    azure_query(
        node,
        "CREATE TABLE drop_table (key UInt64, data String) Engine = AzureBlobStorage(azure_conf1)",
    )

    azure_query(
        node,
        "TRUNCATE TABLE drop_table",
    )


def test_simple_write_named_collection_2_table_function(cluster):
    node = cluster.instances["node"]

    azure_query(
        node,
        "INSERT INTO TABLE FUNCTION azureBlobStorage(azure_conf2, container='cont', blob_path='test_simple_write_named_2_tf.csv', format='CSV', structure='key UInt64, data String') VALUES (1, 'a')",
    )
    print(get_azure_file_content("test_simple_write_named_2_tf.csv"))
    assert get_azure_file_content("test_simple_write_named_2_tf.csv") == '1,"a"\n'


def test_put_get_with_globs_tf(cluster):
    # type: (ClickHouseCluster) -> None
    unique_prefix = random.randint(1, 10000)
    node = cluster.instances["node"]  # type: ClickHouseInstance
    table_format = "column1 UInt32, column2 UInt32, column3 UInt32"
    max_path = ""
    for i in range(10):
        for j in range(10):
            path = "{}/{}_{}/{}.csv".format(
                unique_prefix, i, random.choice(["a", "b", "c", "d"]), j
            )
            max_path = max(path, max_path)
            values = f"({i},{j},{i + j})"

            azure_query(
                node,
                f"INSERT INTO TABLE FUNCTION azureBlobStorage(azure_conf2, container='cont', blob_path='{path}', format='CSV', compression='auto', structure='{table_format}') VALUES {values}",
            )
    query = f"select sum(column1), sum(column2), sum(column3), min(_file), max(_path) from azureBlobStorage(azure_conf2, container='cont', blob_path='{unique_prefix}/*_{{a,b,c,d}}/?.csv', format='CSV', structure='{table_format}')"
    assert azure_query(node, query).splitlines() == [
        "450\t450\t900\t0.csv\t{bucket}/{max_path}".format(
            bucket="cont", max_path=max_path
        )
    ]


def test_schema_inference_no_globs_tf(cluster):
    node = cluster.instances["node"]  # type: ClickHouseInstance
    table_format = "column1 UInt32, column2 String, column3 UInt32"

    query = f"insert into table function azureBlobStorage(azure_conf2, container='cont', blob_path='test_schema_inference_no_globs_tf.csv', format='CSVWithNames', structure='{table_format}') SELECT number, toString(number), number * number FROM numbers(1000)"
    azure_query(node, query)

    query = "select sum(column1), sum(length(column2)), sum(column3), min(_file), max(_path) from azureBlobStorage(azure_conf2, container='cont', blob_path='test_schema_inference_no_globs_tf.csv')"
    assert azure_query(node, query).splitlines() == [
        "499500\t2890\t332833500\ttest_schema_inference_no_globs_tf.csv\tcont/test_schema_inference_no_globs_tf.csv"
    ]


def test_schema_inference_from_globs_tf(cluster):
    node = cluster.instances["node"]
    unique_prefix = random.randint(1, 10000)
    node = cluster.instances["node"]  # type: ClickHouseInstance
    table_format = "column1 UInt32, column2 UInt32, column3 UInt32"
    max_path = ""

    for i in range(10):
        for j in range(10):
            path = "{}/{}_{}/{}.csv".format(
                unique_prefix, i, random.choice(["a", "b", "c", "d"]), j
            )
            max_path = max(path, max_path)
            values = f"({i},{j},{i + j})"

            query = f"insert into table function azureBlobStorage(azure_conf2, container='cont', blob_path='{path}', format='CSVWithNames', structure='{table_format}') VALUES {values}"
            azure_query(node, query)

    query = f"select sum(column1), sum(column2), sum(column3), min(_file), max(_path) from azureBlobStorage(azure_conf2, container='cont', blob_path='{unique_prefix}/*_{{a,b,c,d}}/?.csv')"
    assert azure_query(node, query).splitlines() == [
        "450\t450\t900\t0.csv\t{bucket}/{max_path}".format(
            bucket="cont", max_path=max_path
        )
    ]


def test_partition_by_tf(cluster):
    node = cluster.instances["node"]
    table_format = "column1 UInt32, column2 UInt32, column3 UInt32"
    partition_by = "column3"
    values = "(1, 2, 3), (3, 2, 1), (78, 43, 45)"
    filename = "test_partition_tf_{_partition_id}.csv"

    azure_query(
        node,
        f"INSERT INTO TABLE FUNCTION azureBlobStorage('http://azurite1:10000/devstoreaccount1', 'cont', '{filename}', 'devstoreaccount1', 'Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==', 'CSV', 'auto', '{table_format}') PARTITION BY {partition_by} VALUES {values}",
    )

    assert "1,2,3\n" == get_azure_file_content("test_partition_tf_3.csv")
    assert "3,2,1\n" == get_azure_file_content("test_partition_tf_1.csv")
    assert "78,43,45\n" == get_azure_file_content("test_partition_tf_45.csv")


def test_filter_using_file(cluster):
    node = cluster.instances["node"]
    table_format = "column1 UInt32, column2 UInt32, column3 UInt32"
    partition_by = "column3"
    values = "(1, 2, 3), (3, 2, 1), (78, 43, 45)"
    filename = "test_partition_tf_{_partition_id}.csv"

    azure_query(
        node,
        f"INSERT INTO TABLE FUNCTION azureBlobStorage('http://azurite1:10000/devstoreaccount1', 'cont', '{filename}', 'devstoreaccount1', 'Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==', 'CSV', 'auto', '{table_format}') PARTITION BY {partition_by} VALUES {values}",
    )

    query = f"select count(*) from azureBlobStorage('http://azurite1:10000/devstoreaccount1',  'cont', 'test_partition_tf_*.csv', 'devstoreaccount1', 'Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==', 'CSV', 'auto', '{table_format}') WHERE _file='test_partition_tf_3.csv'"
    assert azure_query(node, query) == "1\n"


def test_read_subcolumns(cluster):
    node = cluster.instances["node"]
    azure_query(
        node,
        "INSERT INTO TABLE FUNCTION azureBlobStorage('http://azurite1:10000/devstoreaccount1', 'cont', 'test_subcolumns.tsv', 'devstoreaccount1', 'Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==', 'auto', 'auto', 'a Tuple(b Tuple(c UInt32, d UInt32), e UInt32)') select ((1, 2), 3)",
    )

    azure_query(
        node,
        "INSERT INTO TABLE FUNCTION azureBlobStorage('http://azurite1:10000/devstoreaccount1', 'cont', 'test_subcolumns.jsonl', 'devstoreaccount1', 'Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==', 'auto', 'auto', 'a Tuple(b Tuple(c UInt32, d UInt32), e UInt32)') select ((1, 2), 3)",
    )

    res = node.query(
        f"select a.b.d, _path, a.b, _file, a.e from azureBlobStorage('http://azurite1:10000/devstoreaccount1', 'cont', 'test_subcolumns.tsv', 'devstoreaccount1', 'Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==', 'auto', 'auto', 'a Tuple(b Tuple(c UInt32, d UInt32), e UInt32)')"
    )

    assert res == "2\tcont/test_subcolumns.tsv\t(1,2)\ttest_subcolumns.tsv\t3\n"

    res = node.query(
        f"select a.b.d, _path, a.b, _file, a.e from azureBlobStorage('http://azurite1:10000/devstoreaccount1', 'cont', 'test_subcolumns.jsonl', 'devstoreaccount1', 'Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==', 'auto', 'auto', 'a Tuple(b Tuple(c UInt32, d UInt32), e UInt32)')"
    )

    assert res == "2\tcont/test_subcolumns.jsonl\t(1,2)\ttest_subcolumns.jsonl\t3\n"

    res = node.query(
        f"select x.b.d, _path, x.b, _file, x.e from azureBlobStorage('http://azurite1:10000/devstoreaccount1', 'cont', 'test_subcolumns.jsonl', 'devstoreaccount1', 'Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==', 'auto', 'auto', 'x Tuple(b Tuple(c UInt32, d UInt32), e UInt32)')"
    )

    assert res == "0\tcont/test_subcolumns.jsonl\t(0,0)\ttest_subcolumns.jsonl\t0\n"

    res = node.query(
        f"select x.b.d, _path, x.b, _file, x.e from azureBlobStorage('http://azurite1:10000/devstoreaccount1', 'cont', 'test_subcolumns.jsonl', 'devstoreaccount1', 'Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==', 'auto', 'auto', 'x Tuple(b Tuple(c UInt32, d UInt32), e UInt32) default ((42, 42), 42)')"
    )

    assert res == "42\tcont/test_subcolumns.jsonl\t(42,42)\ttest_subcolumns.jsonl\t42\n"


def test_read_from_not_existing_container(cluster):
    node = cluster.instances["node"]
    query = f"select * from azureBlobStorage('http://azurite1:10000/devstoreaccount1',  'cont_not_exists', 'test_table.csv', 'devstoreaccount1', 'Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==', 'CSV', 'auto')"
    expected_err_msg = "container does not exist"
    assert expected_err_msg in azure_query(node, query, expect_error="true")


def test_function_signatures(cluster):
    node = cluster.instances["node"]
    connection_string = "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://azurite1:10000/devstoreaccount1;"
    storage_account_url = "http://azurite1:10000/devstoreaccount1"
    account_name = "devstoreaccount1"
    account_key = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="
    azure_query(
        node,
        f"INSERT INTO TABLE FUNCTION azureBlobStorage('{storage_account_url}', 'cont', 'test_signature.csv', '{account_name}', '{account_key}', 'CSV', 'auto', 'column1 UInt32') VALUES (1),(2),(3)",
    )

    # " - connection_string, container_name, blobpath\n"
    query_1 = f"select * from azureBlobStorage('{connection_string}',  'cont', 'test_signature.csv')"
    assert azure_query(node, query_1) == "1\n2\n3\n"

    # " - connection_string, container_name, blobpath, structure \n"
    query_2 = f"select * from azureBlobStorage('{connection_string}',  'cont', 'test_signature.csv', 'column1 UInt32')"
    assert azure_query(node, query_2) == "1\n2\n3\n"

    # " - connection_string, container_name, blobpath, format \n"
    query_3 = f"select * from azureBlobStorage('{connection_string}',  'cont', 'test_signature.csv', 'CSV')"
    assert azure_query(node, query_3) == "1\n2\n3\n"

    # " - connection_string, container_name, blobpath, format, compression \n"
    query_4 = f"select * from azureBlobStorage('{connection_string}',  'cont', 'test_signature.csv', 'CSV', 'auto')"
    assert azure_query(node, query_4) == "1\n2\n3\n"

    # " - connection_string, container_name, blobpath, format, compression, structure \n"
    query_5 = f"select * from azureBlobStorage('{connection_string}',  'cont', 'test_signature.csv', 'CSV', 'auto', 'column1 UInt32')"
    assert azure_query(node, query_5) == "1\n2\n3\n"

    # " - storage_account_url, container_name, blobpath, account_name, account_key\n"
    query_6 = f"select * from azureBlobStorage('{storage_account_url}',  'cont', 'test_signature.csv', '{account_name}', '{account_key}')"
    assert azure_query(node, query_6) == "1\n2\n3\n"

    # " - storage_account_url, container_name, blobpath, account_name, account_key, structure\n"
    query_7 = f"select * from azureBlobStorage('{storage_account_url}',  'cont', 'test_signature.csv', '{account_name}', '{account_key}', 'column1 UInt32')"
    assert azure_query(node, query_7) == "1\n2\n3\n"

    # " - storage_account_url, container_name, blobpath, account_name, account_key, format\n"
    query_8 = f"select * from azureBlobStorage('{storage_account_url}',  'cont', 'test_signature.csv', '{account_name}', '{account_key}', 'CSV')"
    assert azure_query(node, query_8) == "1\n2\n3\n"

    # " - storage_account_url, container_name, blobpath, account_name, account_key, format, compression\n"
    query_9 = f"select * from azureBlobStorage('{storage_account_url}',  'cont', 'test_signature.csv', '{account_name}', '{account_key}', 'CSV', 'auto')"
    assert azure_query(node, query_9) == "1\n2\n3\n"

    # " - storage_account_url, container_name, blobpath, account_name, account_key, format, compression, structure\n"
    query_10 = f"select * from azureBlobStorage('{storage_account_url}',  'cont', 'test_signature.csv', '{account_name}', '{account_key}', 'CSV', 'auto', 'column1 UInt32')"
    assert azure_query(node, query_10) == "1\n2\n3\n"


def check_profile_event_for_query(instance, file, profile_event, amount):
    instance.query("system flush logs")
    query_pattern = f"azureBlobStorage%{file}".replace("'", "\\'")
    res = int(
        instance.query(
            f"select ProfileEvents['{profile_event}'] from system.query_log where query like '%{query_pattern}%' and query not like '%ProfileEvents%' and type = 'QueryFinish' order by query_start_time_microseconds desc limit 1"
        )
    )

    assert res == amount


def check_cache_misses(instance, file, amount=1):
    check_profile_event_for_query(instance, file, "SchemaInferenceCacheMisses", amount)


def check_cache_hits(instance, file, amount=1):
    check_profile_event_for_query(instance, file, "SchemaInferenceCacheHits", amount)


def check_cache_invalidations(instance, file, amount=1):
    check_profile_event_for_query(
        instance, file, "SchemaInferenceCacheInvalidations", amount
    )


def check_cache_evictions(instance, file, amount=1):
    check_profile_event_for_query(
        instance, file, "SchemaInferenceCacheEvictions", amount
    )


def check_cache_num_rows_hots(instance, file, amount=1):
    check_profile_event_for_query(
        instance, file, "SchemaInferenceCacheNumRowsHits", amount
    )


def run_describe_query(instance, file, connection_string):
    query = f"desc azureBlobStorage('{connection_string}', 'cont', '{file}')"
    azure_query(instance, query)


def run_count_query(instance, file, connection_string):
    query = f"select count() from azureBlobStorage('{connection_string}', 'cont', '{file}', auto, auto, 'x UInt64')"
    return azure_query(instance, query)


def check_cache(instance, expected_files):
    sources = instance.query("select source from system.schema_inference_cache")
    assert sorted(map(lambda x: x.strip().split("/")[-1], sources.split())) == sorted(
        expected_files
    )


def test_schema_inference_cache(cluster):
    node = cluster.instances["node"]
    connection_string = "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://azurite1:10000/devstoreaccount1;"
    storage_account_url = "http://azurite1:10000/devstoreaccount1"
    account_name = "devstoreaccount1"
    account_key = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="

    node.query("system drop schema cache")
    azure_query(
        node,
        f"INSERT INTO TABLE FUNCTION azureBlobStorage('{storage_account_url}', 'cont', 'test_cache0.jsonl', '{account_name}', '{account_key}') select * from numbers(100)",
    )

    time.sleep(1)

    run_describe_query(node, "test_cache0.jsonl", connection_string)
    check_cache(node, ["test_cache0.jsonl"])
    check_cache_misses(node, "test_cache0.jsonl")

    run_describe_query(node, "test_cache0.jsonl", connection_string)
    check_cache_hits(node, "test_cache0.jsonl")

    azure_query(
        node,
        f"INSERT INTO TABLE FUNCTION azureBlobStorage('{storage_account_url}', 'cont', 'test_cache0.jsonl', '{account_name}', '{account_key}') select * from numbers(100) settings azure_truncate_on_insert=1",
    )

    time.sleep(1)

    run_describe_query(node, "test_cache0.jsonl", connection_string)
    check_cache_invalidations(node, "test_cache0.jsonl")

    azure_query(
        node,
        f"INSERT INTO TABLE FUNCTION azureBlobStorage('{storage_account_url}', 'cont', 'test_cache1.jsonl', '{account_name}', '{account_key}') select * from numbers(100) settings azure_truncate_on_insert=1",
    )
    time.sleep(1)

    run_describe_query(node, "test_cache1.jsonl", connection_string)
    check_cache(node, ["test_cache0.jsonl", "test_cache1.jsonl"])
    check_cache_misses(node, "test_cache1.jsonl")

    run_describe_query(node, "test_cache1.jsonl", connection_string)
    check_cache_hits(node, "test_cache1.jsonl")

    azure_query(
        node,
        f"INSERT INTO TABLE FUNCTION azureBlobStorage('{storage_account_url}', 'cont', 'test_cache2.jsonl', '{account_name}', '{account_key}') select * from numbers(100) settings azure_truncate_on_insert=1",
    )
    time.sleep(1)

    run_describe_query(node, "test_cache2.jsonl", connection_string)
    check_cache(node, ["test_cache1.jsonl", "test_cache2.jsonl"])
    check_cache_misses(node, "test_cache2.jsonl")
    check_cache_evictions(node, "test_cache2.jsonl")

    run_describe_query(node, "test_cache2.jsonl", connection_string)
    check_cache_hits(node, "test_cache2.jsonl")

    run_describe_query(node, "test_cache1.jsonl", connection_string)
    check_cache_hits(node, "test_cache1.jsonl")

    run_describe_query(node, "test_cache0.jsonl", connection_string)
    check_cache(node, ["test_cache0.jsonl", "test_cache1.jsonl"])
    check_cache_misses(node, "test_cache0.jsonl")
    check_cache_evictions(node, "test_cache0.jsonl")

    run_describe_query(node, "test_cache2.jsonl", connection_string)
    check_cache(node, ["test_cache0.jsonl", "test_cache2.jsonl"])
    check_cache_misses(
        node,
        "test_cache2.jsonl",
    )
    check_cache_evictions(
        node,
        "test_cache2.jsonl",
    )

    run_describe_query(node, "test_cache2.jsonl", connection_string)

    check_cache_hits(
        node,
        "test_cache2.jsonl",
    )

    run_describe_query(node, "test_cache0.jsonl", connection_string)
    check_cache_hits(
        node,
        "test_cache0.jsonl",
    )

    azure_query(
        node,
        f"INSERT INTO TABLE FUNCTION azureBlobStorage('{storage_account_url}', 'cont', 'test_cache3.jsonl', '{account_name}', '{account_key}') select * from numbers(100) settings azure_truncate_on_insert=1",
    )
    time.sleep(1)

    files = "test_cache{0,1,2,3}.jsonl"
    run_describe_query(node, files, connection_string)
    print(node.query("select * from system.schema_inference_cache format Pretty"))
    check_cache_hits(node, files)

    node.query(f"system drop schema cache for azure")
    check_cache(node, [])

    run_describe_query(node, files, connection_string)
    check_cache_misses(node, files, 4)

    node.query("system drop schema cache")
    check_cache(node, [])

    run_describe_query(node, files, connection_string)
    check_cache_misses(node, files, 4)

    node.query("system drop schema cache")

    azure_query(
        node,
        f"INSERT INTO TABLE FUNCTION azureBlobStorage('{storage_account_url}', 'cont', 'test_cache0.csv', '{account_name}', '{account_key}') select * from numbers(100) settings azure_truncate_on_insert=1",
    )
    time.sleep(1)

    res = run_count_query(node, "test_cache0.csv", connection_string)

    assert int(res) == 100

    check_cache(node, ["test_cache0.csv"])
    check_cache_misses(
        node,
        "test_cache0.csv",
    )

    res = run_count_query(node, "test_cache0.csv", connection_string)
    assert int(res) == 100

    check_cache_hits(
        node,
        "test_cache0.csv",
    )

    azure_query(
        node,
        f"INSERT INTO TABLE FUNCTION azureBlobStorage('{storage_account_url}', 'cont', 'test_cache0.csv', '{account_name}', '{account_key}') select * from numbers(200) settings azure_truncate_on_insert=1",
    )
    time.sleep(1)

    res = run_count_query(node, "test_cache0.csv", connection_string)

    assert int(res) == 200

    check_cache_invalidations(
        node,
        "test_cache0.csv",
    )

    azure_query(
        node,
        f"INSERT INTO TABLE FUNCTION azureBlobStorage('{storage_account_url}', 'cont', 'test_cache1.csv', '{account_name}', '{account_key}') select * from numbers(100) settings azure_truncate_on_insert=1",
    )
    time.sleep(1)

    res = run_count_query(node, "test_cache1.csv", connection_string)

    assert int(res) == 100
    check_cache(node, ["test_cache0.csv", "test_cache1.csv"])
    check_cache_misses(
        node,
        "test_cache1.csv",
    )

    res = run_count_query(node, "test_cache1.csv", connection_string)
    assert int(res) == 100
    check_cache_hits(
        node,
        "test_cache1.csv",
    )

    res = run_count_query(node, "test_cache{0,1}.csv", connection_string)
    assert int(res) == 300
    check_cache_hits(node, "test_cache{0,1}.csv", 2)

    node.query(f"system drop schema cache for azure")
    check_cache(node, [])

    res = run_count_query(node, "test_cache{0,1}.csv", connection_string)
    assert int(res) == 300
    check_cache_misses(node, "test_cache{0,1}.csv", 2)

    azure_query(
        node,
        f"INSERT INTO TABLE FUNCTION azureBlobStorage('{storage_account_url}', 'cont', 'test_cache.parquet', '{account_name}', '{account_key}') select * from numbers(100) settings azure_truncate_on_insert=1",
    )
    time.sleep(1)

    res = azure_query(
        node,
        f"select count() from azureBlobStorage('{connection_string}', 'cont', 'test_cache.parquet')",
    )
    assert int(res) == 100
    check_cache_misses(node, "test_cache.parquet")
    check_cache_hits(node, "test_cache.parquet")
    check_cache_num_rows_hots(node, "test_cache.parquet")


def test_filtering_by_file_or_path(cluster):
    node = cluster.instances["node"]
    azure_query(
        node,
        "INSERT INTO TABLE FUNCTION azureBlobStorage('http://azurite1:10000/devstoreaccount1', 'cont', 'test_filter1.tsv', 'devstoreaccount1', 'Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==', 'auto', 'auto', 'x UInt64') select 1",
    )

    azure_query(
        node,
        "INSERT INTO TABLE FUNCTION azureBlobStorage('http://azurite1:10000/devstoreaccount1', 'cont', 'test_filter2.tsv', 'devstoreaccount1', 'Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==', 'auto', 'auto', 'x UInt64') select 2",
    )

    azure_query(
        node,
        "INSERT INTO TABLE FUNCTION azureBlobStorage('http://azurite1:10000/devstoreaccount1', 'cont', 'test_filter3.tsv', 'devstoreaccount1', 'Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==', 'auto', 'auto', 'x UInt64') select 3",
    )

    node.query(
        f"select count() from azureBlobStorage('http://azurite1:10000/devstoreaccount1', 'cont', 'test_filter*.tsv', 'devstoreaccount1', 'Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==', 'auto', 'auto', 'x UInt64') where _file = 'test_filter1.tsv'"
    )

    node.query("SYSTEM FLUSH LOGS")

    result = node.query(
        f"SELECT ProfileEvents['EngineFileLikeReadFiles'] FROM system.query_log WHERE query like '%select%azure%test_filter%' AND type='QueryFinish'"
    )

    assert int(result) == 1
