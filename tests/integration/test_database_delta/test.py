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
            tag=os.environ.get("DOCKER_BASE_WITH_UNITY_CATALOG_TAG", "latest")
        )

        logging.info("Starting cluster...")
        cluster.start()

        start_unity_catalog(cluster.instances['node1'])

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
        ], nothrow=ignore_exit_code
    )

def execute_multiple_spark_queries(node, queries_list, ignore_exit_code=False):
    return execute_spark_query(node, ';'.join(queries_list), ignore_exit_code)

def test_embedded_database_and_tables(started_cluster):
    node1 = started_cluster.instances['node1']
    node1.query("create database unity_test engine DataLakeCatalog('http://localhost:8080/api/2.1/unity-catalog') settings warehouse = 'unity', catalog_type='unity', vended_credentials=false", settings={"allow_experimental_database_unity_catalog": "1"})
    default_tables = list(sorted(node1.query("SHOW TABLES FROM unity_test LIKE 'default%'", settings={'use_hive_partitioning':'0'}).strip().split('\n')))
    print("Default tables", default_tables)
    assert default_tables == ['default.marksheet', 'default.marksheet_uniform', 'default.numbers', 'default.user_countries']

    for table in default_tables:
        if table == "default.marksheet_uniform":
            continue
        assert "DeltaLake" in node1.query(f"show create table unity_test.`{table}`")
        if table in ('default.marksheet', 'default.user_countries'):
            data_clickhouse = TSV(node1.query(f"SELECT * FROM unity_test.`{table}` ORDER BY 1,2,3"))
            data_spark = TSV(execute_spark_query(node1, f"SELECT * FROM unity.{table} ORDER BY 1,2,3"))
            print("Data ClickHouse\n", data_clickhouse)
            print("Data Spark\n", data_spark)
            assert data_clickhouse == data_spark


def test_multiple_schemes_tables(started_cluster):
    node1 = started_cluster.instances['node1']
    execute_multiple_spark_queries(node1, [f'CREATE SCHEMA test_schema{i}' for i in range(10)], True)
    execute_multiple_spark_queries(node1, [f'CREATE TABLE test_schema{i}.test_table{i} (col1 int, col2 double) using Delta location \'/tmp/test_schema{i}/test_table{i}\'' for i in range(10)], True)
    execute_multiple_spark_queries(node1, [f'INSERT INTO test_schema{i}.test_table{i} VALUES ({i}, {i}.0)' for i in range(10)], True)

    node1.query("create database multi_schema_test engine DataLakeCatalog('http://localhost:8080/api/2.1/unity-catalog') settings warehouse = 'unity', catalog_type='unity', vended_credentials=false", settings={"allow_experimental_database_unity_catalog": "1"})
    multi_schema_tables = list(sorted(node1.query("SHOW TABLES FROM multi_schema_test LIKE 'test_schema%'", settings={'use_hive_partitioning':'0'}).strip().split('\n')))
    print(multi_schema_tables)

    for i, table in enumerate(multi_schema_tables):
        assert node1.query(f"SELECT col1 FROM multi_schema_test.`{table}`").strip() == str(i)
        assert int(node1.query(f"SELECT col2 FROM multi_schema_test.`{table}`").strip()) == i


def test_complex_table_schema(started_cluster):
    node1 = started_cluster.instances['node1']
    execute_spark_query(node1, "CREATE SCHEMA schema_with_complex_tables", ignore_exit_code=True)
    schema = "event_date DATE, event_time TIMESTAMP, hits ARRAY<integer>, ids MAP<int, string>, really_complex STRUCT<f1:int,f2:string>"
    create_query = f"CREATE TABLE schema_with_complex_tables.complex_table ({schema}) using Delta location '/tmp/complex_schema/complex_table'"
    execute_spark_query(node1, create_query, ignore_exit_code=True)
    execute_spark_query(node1, "insert into schema_with_complex_tables.complex_table SELECT to_date('2024-10-01', 'yyyy-MM-dd'), to_timestamp('2024-10-01 00:12:00'), array(42, 123, 77), map(7, 'v7', 5, 'v5'), named_struct(\\\"f1\\\", 34, \\\"f2\\\", 'hello')", ignore_exit_code=True)

    node1.query("create database complex_schema engine DataLakeCatalog('http://localhost:8080/api/2.1/unity-catalog') settings warehouse = 'unity', catalog_type='unity', vended_credentials=false", settings={"allow_experimental_database_unity_catalog": "1"})

    complex_schema_tables = list(sorted(node1.query("SHOW TABLES FROM complex_schema LIKE 'schema_with_complex_tables%'", settings={'use_hive_partitioning':'0'}).strip().split('\n')))

    assert len(complex_schema_tables) == 1

    print(node1.query("SHOW CREATE TABLE complex_schema.`schema_with_complex_tables.complex_table`"))
    complex_data = node1.query("SELECT * FROM complex_schema.`schema_with_complex_tables.complex_table`").strip().split('\t')
    print(complex_data)
    assert complex_data[0] == "2024-10-01"
    assert complex_data[1] == "2024-10-01 00:12:00.000000"
    assert complex_data[2] == "[42,123,77]"
    assert complex_data[3] == "{7:'v7',5:'v5'}"
    assert complex_data[4] == "(34,'hello')"
