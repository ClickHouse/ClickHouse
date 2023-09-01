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
            main_configs=["configs/named_collections.xml"],
            user_configs=["configs/disable_profilers.xml", "configs/users.xml"],
            with_azurite=True,
        )
        cluster.start()

        yield cluster
    finally:
        cluster.shutdown()


def azure_query(node, query, try_num=10, settings={}):
    for i in range(try_num):
        try:
            return node.query(query, settings=settings)
        except Exception as ex:
            retriable_errors = [
                "DB::Exception: Azure::Core::Http::TransportException: Connection was closed by the server while trying to read a response",
                "DB::Exception: Azure::Core::Http::TransportException: Connection closed before getting full response or response is less than expected",
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
