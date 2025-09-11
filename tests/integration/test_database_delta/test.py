#!/usr/bin/env python3

import glob
import json
import logging
import os
import random
import time
import uuid
from datetime import datetime, timedelta
from helpers.cluster import ClickHouseCluster
import pytest
import requests
import urllib3

from helpers.test_tools import TSV


def start_unity_catalog(node):
    node.exec_in_container(
        [
            "bash",
            "-c",
            f"""cd /unitycatalog && nohup bin/start-uc-server &""",
        ]
    )


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster = ClickHouseCluster(__file__)
        cluster.add_instance(
            "node1",
            main_configs=[],
            user_configs=[],
            image="clickhouse/integration-test-with-unity-catalog",
            with_installed_binary=False,
            tag=os.environ.get("DOCKER_BASE_WITH_UNITY_CATALOG_TAG", "latest"),
        )

        logging.info("Starting cluster...")
        cluster.start()

        start_unity_catalog(cluster.instances["node1"])

        yield cluster

    finally:
        cluster.shutdown()


def execute_spark_query(node, query_text, ignore_exit_code=False):
    return node.exec_in_container(
        [
            "bash",
            "-c",
            f"""
cd /spark-3.5.4-bin-hadoop3 && bin/spark-sql --name "s3-uc-test" \\
    --master "local[*]" \\
    --packages "org.apache.hadoop:hadoop-aws:3.3.4,io.delta:delta-spark_2.12:3.2.1,io.unitycatalog:unitycatalog-spark_2.12:0.2.0" \\
    --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \\
    --conf "spark.sql.catalog.spark_catalog=io.unitycatalog.spark.UCSingleCatalog" \\
    --conf "spark.hadoop.fs.s3.impl=org.apache.hadoop.fs.s3a.S3AFileSystem" \\
    --conf "spark.sql.catalog.unity=io.unitycatalog.spark.UCSingleCatalog" \\
    --conf "spark.sql.catalog.unity.uri=http://localhost:8080" \\
    --conf "spark.sql.catalog.unity.token=" \\
    --conf "spark.sql.defaultCatalog=unity" \\
    -S -e "{query_text}" | grep -v 'loading settings'
""",
        ],
        nothrow=ignore_exit_code,
    )


def execute_multiple_spark_queries(node, queries_list, ignore_exit_code=False):
    return execute_spark_query(node, ";".join(queries_list), ignore_exit_code)


@pytest.mark.parametrize("use_delta_kernel", ["1", "0"])
def test_embedded_database_and_tables(started_cluster, use_delta_kernel):
    node1 = started_cluster.instances["node1"]
    node1.query("drop database if exists unity_test")
    node1.query(
        f"create database unity_test engine DataLakeCatalog('http://localhost:8080/api/2.1/unity-catalog') settings warehouse = 'unity', catalog_type='unity', vended_credentials=false, allow_experimental_delta_kernel_rs={use_delta_kernel}",
        settings={"allow_experimental_database_unity_catalog": "1"},
    )
    default_tables = list(
        sorted(
            node1.query(
                "SHOW TABLES FROM unity_test LIKE 'default%'",
                settings={"use_hive_partitioning": "0"},
            )
            .strip()
            .split("\n")
        )
    )
    print("Default tables", default_tables)
    assert default_tables == [
        "default.marksheet",
        "default.marksheet_uniform",
        "default.numbers",
        "default.user_countries",
    ]

    for table in default_tables:
        if table == "default.marksheet_uniform":
            continue
        assert "DeltaLake" in node1.query(f"show create table unity_test.`{table}`")
        if table in ("default.marksheet", "default.user_countries"):
            data_clickhouse = TSV(
                node1.query(f"SELECT * FROM unity_test.`{table}` ORDER BY 1,2,3")
            )
            data_spark = TSV(
                execute_spark_query(
                    node1, f"SELECT * FROM unity.{table} ORDER BY 1,2,3"
                )
            )
            print("Data ClickHouse\n", data_clickhouse)
            print("Data Spark\n", data_spark)
            assert data_clickhouse == data_spark


def test_multiple_schemes_tables(started_cluster):
    node1 = started_cluster.instances["node1"]
    execute_multiple_spark_queries(
        node1, [f"CREATE SCHEMA test_schema{i}" for i in range(10)], True
    )
    execute_multiple_spark_queries(
        node1,
        [
            f"CREATE TABLE test_schema{i}.test_table{i} (col1 int, col2 double) using Delta location '/tmp/test_schema{i}/test_table{i}'"
            for i in range(10)
        ],
        True,
    )
    execute_multiple_spark_queries(
        node1,
        [
            f"INSERT INTO test_schema{i}.test_table{i} VALUES ({i}, {i}.0)"
            for i in range(10)
        ],
        True,
    )

    node1.query(
        "create database multi_schema_test engine DataLakeCatalog('http://localhost:8080/api/2.1/unity-catalog') settings warehouse = 'unity', catalog_type='unity', vended_credentials=false",
        settings={"allow_experimental_database_unity_catalog": "1"},
    )
    multi_schema_tables = list(
        sorted(
            node1.query(
                "SHOW TABLES FROM multi_schema_test LIKE 'test_schema%'",
                settings={"use_hive_partitioning": "0"},
            )
            .strip()
            .split("\n")
        )
    )
    print(multi_schema_tables)

    for i, table in enumerate(multi_schema_tables):
        assert node1.query(
            f"SELECT col1 FROM multi_schema_test.`{table}`"
        ).strip() == str(i)
        assert (
            int(node1.query(f"SELECT col2 FROM multi_schema_test.`{table}`").strip())
            == i
        )


@pytest.mark.parametrize("use_delta_kernel", ["1", "0"])
def test_complex_table_schema(started_cluster, use_delta_kernel):
    node1 = started_cluster.instances["node1"]
    schema_name = f"schema_with_complex_tables_{use_delta_kernel}"
    execute_spark_query(node1, f"CREATE SCHEMA {schema_name}", ignore_exit_code=True)
    table_name = f"complex_table_{use_delta_kernel}"
    schema = "event_date DATE, event_time TIMESTAMP, hits ARRAY<integer>, ids MAP<int, string>, really_complex STRUCT<f1:int,f2:string>"
    create_query = f"CREATE TABLE {schema_name}.{table_name} ({schema}) using Delta location '/tmp/complex_schema/{table_name}'"
    execute_spark_query(node1, create_query, ignore_exit_code=True)
    execute_spark_query(
        node1,
        f"insert into {schema_name}.{table_name} SELECT to_date('2024-10-01', 'yyyy-MM-dd'), to_timestamp('2024-10-01 00:12:00'), array(42, 123, 77), map(7, 'v7', 5, 'v5'), named_struct(\\\"f1\\\", 34, \\\"f2\\\", 'hello')",
        ignore_exit_code=True,
    )

    node1.query(
        f"""
drop database if exists complex_schema;
create database complex_schema
engine DataLakeCatalog('http://localhost:8080/api/2.1/unity-catalog')
settings warehouse = 'unity', catalog_type='unity', vended_credentials=false, allow_experimental_delta_kernel_rs={use_delta_kernel}
        """,
        settings={"allow_experimental_database_unity_catalog": "1"},
    )

    complex_schema_tables = list(
        sorted(
            node1.query(
                f"SHOW TABLES FROM complex_schema LIKE '{schema_name}%'",
                settings={"use_hive_partitioning": "0"},
            )
            .strip()
            .split("\n")
        )
    )

    assert len(complex_schema_tables) == 1

    print(node1.query(f"SHOW CREATE TABLE complex_schema.`{schema_name}.{table_name}`"))
    complex_data = (
        node1.query(
            f"SELECT * FROM complex_schema.`{schema_name}.{table_name}`",
            settings={"allow_experimental_delta_kernel_rs": use_delta_kernel},
        )
        .strip()
        .split("\t")
    )
    print(complex_data)
    assert complex_data[0] == "2024-10-01"
    assert complex_data[1] == "2024-10-01 00:12:00.000000"
    assert complex_data[2] == "[42,123,77]"
    assert complex_data[3] == "{7:'v7',5:'v5'}"
    assert complex_data[4] == "(34,'hello')"

    if use_delta_kernel == "1":
        assert node1.contains_in_log(f"DeltaLakeMetadata: Initializing snapshot")


@pytest.mark.parametrize("use_delta_kernel", ["1", "0"])
def test_timestamp_ntz(started_cluster, use_delta_kernel):
    node1 = started_cluster.instances["node1"]
    node1.query("drop database if exists ntz_schema")

    schema_name = f"schema_with_timetstamp_ntz_{use_delta_kernel}"
    execute_spark_query(node1, f"CREATE SCHEMA {schema_name}", ignore_exit_code=True)
    table_name = f"table_with_timestamp_{use_delta_kernel}"
    schema = "event_date DATE, event_time TIMESTAMP, event_time_ntz TIMESTAMP_NTZ"
    create_query = f"CREATE TABLE {schema_name}.{table_name} ({schema}) using Delta location '/tmp/ntz_schema/{table_name}'"
    execute_spark_query(node1, create_query, ignore_exit_code=True)
    execute_spark_query(
        node1,
        f"insert into {schema_name}.{table_name} SELECT to_date('2024-10-01', 'yyyy-MM-dd'), to_timestamp('2024-10-01 00:12:00'), to_timestamp_ntz('2024-10-01 00:12:00')",
        ignore_exit_code=True,
    )

    node1.query(
        f"""
drop database if exists ntz_schema;
create database ntz_schema
engine DataLakeCatalog('http://localhost:8080/api/2.1/unity-catalog')
settings warehouse = 'unity', catalog_type='unity', vended_credentials=false, allow_experimental_delta_kernel_rs={use_delta_kernel}
        """,
        settings={"allow_experimental_database_unity_catalog": "1"},
    )

    ntz_tables = list(
        sorted(
            node1.query(
                f"SHOW TABLES FROM ntz_schema LIKE '{schema_name}%'",
                settings={"use_hive_partitioning": "0"},
            )
            .strip()
            .split("\n")
        )
    )

    assert len(ntz_tables) == 1

    def get_schemas():
        return execute_spark_query(node1, f"SHOW SCHEMAS", ignore_exit_code=True)

    assert schema_name in get_schemas()

    ntz_data = ""
    for i in range(10):
        try:
            ntz_data = (
                node1.query(
                    f"SELECT * FROM ntz_schema.`{schema_name}.{table_name}`",
                    settings={"allow_experimental_delta_kernel_rs": use_delta_kernel},
                )
                .strip()
                .split("\t")
            )
            break
        except Exception as ex:
            if "Schema not found" not in str(ex):
                raise ex
            print(f"Retry {i + 1}, existing schemas: {get_schemas()}")

    assert len(ntz_data) != 0, f"Schemas: {get_schemas()}"
    print(ntz_data)
    assert ntz_data[0] == "2024-10-01"
    assert ntz_data[1] == "2024-10-01 00:12:00.000000"
    assert ntz_data[2] == "2024-10-01 00:12:00.000000"


def test_no_permission_and_list_tables(started_cluster):
    # this test supposed to test "SHOW TABLES" query when we have no permissions for some table
    # unfortunately Unity catalog open source (or integration with spark) doesn't fully supports grants
    # So this query fails and the test doesn't check anything :(
    pytest.skip("Skipping test because it doesn't check anything")
    node1 = started_cluster.instances["node1"]
    node1.query("drop database if exists schema_with_permissions")

    schema_name = f"schema_with_permissions"
    execute_spark_query(node1, f"CREATE SCHEMA {schema_name}", ignore_exit_code=True)
    table_name_1 = f"table_granted"
    table_name_2 = f"table_not_granted"

    create_query_1 = f"CREATE TABLE {schema_name}.{table_name_1} (id INT) using Delta location '/tmp/{schema_name}/{table_name_1}'"
    create_query_2 = f"CREATE TABLE {schema_name}.{table_name_2} (id INT) using Delta location '/tmp/{schema_name}/{table_name_2}'"

    execute_multiple_spark_queries(node1, [create_query_2, create_query_1], True)

    execute_spark_query(
        node1,
        f"REVOKE ALL PRIVILEGES ON TABLE {schema_name}.{table_name_1} FROM PUBLIC;",
        ignore_exit_code=True,
    )

    node1.query(
        f"""
drop database if exists {schema_name};
create database {schema_name}
engine DataLakeCatalog('http://localhost:8080/api/2.1/unity-catalog')
settings warehouse = 'unity', catalog_type='unity', vended_credentials=True
        """,
        settings={"allow_experimental_database_unity_catalog": "1"},
    )

    # This query will fail if bug exists
    print(node1.query(f"SHOW TABLES FROM {schema_name}"))
