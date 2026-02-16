#!/usr/bin/env python3

import glob
import json
import logging
import os
import re
import random
import time
import subprocess
import uuid
from datetime import datetime, timedelta
from helpers.cluster import ClickHouseCluster
import pytest
import requests
import urllib3
import pyspark
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, DateType, StringType
from pyspark.sql.utils import AnalysisException
from datetime import date
import uuid

from helpers.test_tools import TSV


def start_unity_catalog(node):
    node.exec_in_container(
        [
            "bash",
            "-c",
            f"""cp -r /unitycatalog /var/lib/clickhouse/user_files/ && cd /var/lib/clickhouse/user_files/unitycatalog && nohup bin/start-uc-server > uc.log 2>&1 &""",
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

        if int(cluster.instances["node1"].query("SELECT count() FROM system.table_engines WHERE name = 'DeltaLake'").strip()) == 0:
            pytest.skip(
                "DeltaLake engine is not available"
            )

        start_unity_catalog(cluster.instances["node1"])

        yield cluster

    finally:
        cluster.shutdown()


def execute_spark_query(node, query_text):
    # Kill any lingering Spark processes and remove the Derby metastore
    # before starting a new Spark session. The metastore_db is created inside
    # /spark-3.5.4-bin-hadoop3/ because spark-sql is run with cd to that directory.
    # We remove the entire metastore_db (not just the lock file) because after
    # SIGKILL the database can be left in a corrupted state, causing the next
    # Spark session to hang during initialization.
    node.exec_in_container(
        [
            "bash",
            "-c",
            """pkill -9 -f 'org.apache.spark' 2>/dev/null; sleep 1; rm -rf /spark-3.5.4-bin-hadoop3/metastore_db""",
        ],
        nothrow=True,
    )

    try:
        result = node.exec_in_container(
            [
                "bash",
                "-c",
                f"""
    cd /spark-3.5.4-bin-hadoop3 && bin/spark-sql --name "s3-uc-test" \\
        --master "local[1]" \\
        --packages "org.apache.hadoop:hadoop-aws:3.3.4,io.delta:delta-spark_2.12:3.2.1,io.unitycatalog:unitycatalog-spark_2.12:0.2.0" \\
        --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \\
        --conf "spark.databricks.delta.schema.autoMerge.enabled=true" \\
        --conf "spark.sql.catalog.spark_catalog=io.unitycatalog.spark.UCSingleCatalog" \\
        --conf "spark.hadoop.fs.s3.impl=org.apache.hadoop.fs.s3a.S3AFileSystem" \\
        --conf "spark.driver.allowMultipleContexts=false" \\
        --conf "spark.sql.catalog.unity=io.unitycatalog.spark.UCSingleCatalog" \\
        --conf "spark.sql.catalog.unity.uri=http://localhost:8080" \\
        --conf "spark.sql.catalog.unity.token=" \\
        --conf "spark.sql.defaultCatalog=unity" \\
        -S -e "{query_text}"
    """,
            ],
        )
    except subprocess.CalledProcessError as e:
        print("Command failed with exit code:", e.returncode)
        print("Command:", e.cmd)

        stdout = e.stdout.decode() if e.stdout else "<no stdout>"
        stderr = e.stderr.decode() if e.stderr else "<no stderr>"
        print("STDOUT:\n", stdout)
        print("STDERR:\n", stderr)

        try:
            logs = node.exec_in_container(
                [
                    "tail",
                    "-n",
                    "50",
                    "/var/lib/clickhouse/user_files/unitycatalog/uc.log",
                ]
            )
            print("Last 50 lines of UC log:\n", logs)
        except subprocess.CalledProcessError as log_e:
            print(f"Cannot read log file: {str(log_e)}")

        raise

    # We do not use "grep -v" for the above command,
    # because it will mess up the exit code.
    exclude_pattern = r"loading settings"
    lines = result.splitlines()
    filtered = [line for line in lines if not re.search(exclude_pattern, line)]
    result = "\n".join(filtered)
    return result


def execute_multiple_spark_queries(node, queries_list):
    return execute_spark_query(node, ";".join(queries_list))


@pytest.mark.parametrize("use_delta_kernel", ["1", "0"])
def test_embedded_database_and_tables(started_cluster, use_delta_kernel):
    test_uuid = str(uuid.uuid4()).replace("-", "_")
    node1 = started_cluster.instances["node1"]
    node1.query(f"drop database if exists unity_test_{test_uuid}")
    node1.query(
        f"create database unity_test_{test_uuid} engine DataLakeCatalog('http://localhost:8080/api/2.1/unity-catalog') settings warehouse = 'unity', catalog_type='unity', vended_credentials=false, allow_experimental_delta_kernel_rs={use_delta_kernel}",
        settings={"allow_experimental_database_unity_catalog": "1"},
    )
    default_tables = list(
        sorted(
            node1.query(
                f"SHOW TABLES FROM unity_test_{test_uuid} LIKE 'default%'",
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
        assert "DeltaLake" in node1.query(
            f"show create table unity_test_{test_uuid}.`{table}`"
        )
        if table in ("default.marksheet", "default.user_countries"):
            data_clickhouse = TSV(
                node1.query(
                    f"SELECT * FROM unity_test_{test_uuid}.`{table}` ORDER BY 1,2,3"
                )
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
    test_uuid = str(uuid.uuid4()).replace("-", "_")
    node1 = started_cluster.instances["node1"]
    execute_multiple_spark_queries(
        node1, [f"CREATE SCHEMA test_schema{test_uuid}{i}" for i in range(10)]
    )
    execute_multiple_spark_queries(
        node1,
        [
            f"CREATE TABLE test_schema{test_uuid}{i}.test_table{test_uuid}{i} (col1 int, col2 double) using Delta location '/var/lib/clickhouse/user_files/tmp/test_schema{test_uuid}{i}/test_table{test_uuid}{i}'"
            for i in range(10)
        ],
    )
    execute_multiple_spark_queries(
        node1,
        [
            f"INSERT INTO test_schema{test_uuid}{i}.test_table{test_uuid}{i} VALUES ({i}, {i}.0)"
            for i in range(10)
        ],
    )

    node1.query(
        f"create database multi_schema_test{test_uuid} engine DataLakeCatalog('http://localhost:8080/api/2.1/unity-catalog') settings warehouse = 'unity', catalog_type='unity', vended_credentials=false",
        settings={"allow_database_unity_catalog": "1"},
    )
    multi_schema_tables = list(
        sorted(
            node1.query(
                f"SHOW TABLES FROM multi_schema_test{test_uuid} LIKE 'test_schema{test_uuid}%'",
                settings={"use_hive_partitioning": "0"},
            )
            .strip()
            .split("\n")
        )
    )
    print(multi_schema_tables)

    for i, table in enumerate(multi_schema_tables):
        assert node1.query(
            f"SELECT col1 FROM multi_schema_test{test_uuid}.`{table}`"
        ).strip() == str(i)
        assert (
            int(
                node1.query(
                    f"SELECT col2 FROM multi_schema_test{test_uuid}.`{table}`"
                ).strip()
            )
            == i
        )


@pytest.mark.parametrize("use_delta_kernel", ["1", "0"])
def test_complex_table_schema(started_cluster, use_delta_kernel):
    node1 = started_cluster.instances["node1"]
    schema_name = (
        f"schema_with_complex_tables_{use_delta_kernel}_{uuid.uuid4()}".replace(
            "-", "_"
        )
    )
    table_name = f"complex_table_{use_delta_kernel}_{uuid.uuid4()}".replace("-", "_")
    schema = "event_date DATE, event_time TIMESTAMP, hits ARRAY<integer>, ids MAP<int, string>, really_complex STRUCT<f1:int,f2:string>"
    create_query = f"CREATE TABLE {schema_name}.{table_name} ({schema}) using Delta location '/var/lib/clickhouse/user_files/tmp/complex_schema/{table_name}'"
    insert_query = f"insert into {schema_name}.{table_name} SELECT to_date('2024-10-01', 'yyyy-MM-dd'), to_timestamp('2024-10-01 00:12:00'), array(42, 123, 77), map(7, 'v7', 5, 'v5'), named_struct(\\\"f1\\\", 34, \\\"f2\\\", 'hello')"
    execute_multiple_spark_queries(
        node1,
        [f"CREATE SCHEMA {schema_name}", create_query, insert_query],
    )

    node1.query(
        f"""
drop database if exists complex_schema;
create database complex_schema
engine DataLakeCatalog('http://localhost:8080/api/2.1/unity-catalog')
settings warehouse = 'unity', catalog_type='unity', vended_credentials=false, allow_experimental_delta_kernel_rs={use_delta_kernel}
        """,
        settings={"allow_database_unity_catalog": "1"},
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
            settings={
                "allow_experimental_delta_kernel_rs": use_delta_kernel
            },
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
    table_name_src = f"ntz_schema_{uuid.uuid4()}".replace("-", "_")
    node1 = started_cluster.instances["node1"]
    node1.query(f"drop database if exists {table_name_src}")

    schema_name = (
        f"schema_with_timetstamp_ntz_{use_delta_kernel}_{uuid.uuid4()}".replace(
            "-", "_"
        )
    )
    table_name = f"table_with_timestamp_{use_delta_kernel}_{uuid.uuid4()}".replace(
        "-", "_"
    )
    schema = "event_date DATE, event_time TIMESTAMP, event_time_ntz TIMESTAMP_NTZ"
    create_query = f"CREATE TABLE {schema_name}.{table_name} ({schema}) using Delta location '/var/lib/clickhouse/user_files/tmp/{table_name_src}/{table_name}'"
    insert_query = f"insert into {schema_name}.{table_name} SELECT to_date('2024-10-01', 'yyyy-MM-dd'), to_timestamp('2024-10-01 00:12:00'), to_timestamp_ntz('2024-10-01 00:12:00')"
    execute_multiple_spark_queries(
        node1,
        [f"CREATE SCHEMA {schema_name}", create_query, insert_query],
    )

    node1.query(
        f"""
drop database if exists {table_name};
create database {table_name_src}
engine DataLakeCatalog('http://localhost:8080/api/2.1/unity-catalog')
settings warehouse = 'unity', catalog_type='unity', vended_credentials=false, allow_experimental_delta_kernel_rs={use_delta_kernel}
        """,
        settings={"allow_database_unity_catalog": "1"},
    )

    ntz_tables = list(
        sorted(
            node1.query(
                f"SHOW TABLES FROM {table_name_src} LIKE '{schema_name}%'",
                settings={"use_hive_partitioning": "0"},
            )
            .strip()
            .split("\n")
        )
    )

    assert len(ntz_tables) == 1

    def get_schemas():
        return execute_spark_query(node1, f"SHOW SCHEMAS")

    assert schema_name in get_schemas()

    ntz_data = ""
    for i in range(10):
        try:
            ntz_data = (
                node1.query(
                    f"SELECT * FROM {table_name_src}.`{schema_name}.{table_name}`",
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
    execute_spark_query(node1, f"CREATE SCHEMA {schema_name}")
    table_name_1 = f"table_granted"
    table_name_2 = f"table_not_granted"

    create_query_1 = f"CREATE TABLE {schema_name}.{table_name_1} (id INT) using Delta location '/var/lib/clickhouse/user_files/tmp/{schema_name}/{table_name_1}'"
    create_query_2 = f"CREATE TABLE {schema_name}.{table_name_2} (id INT) using Delta location '/var/lib/clickhouse/user_files/tmp/{schema_name}/{table_name_2}'"

    execute_multiple_spark_queries(node1, [create_query_2, create_query_1])

    execute_spark_query(
        node1,
        f"REVOKE ALL PRIVILEGES ON TABLE {schema_name}.{table_name_1} FROM PUBLIC;",
    )

    node1.query(
        f"""
drop database if exists {schema_name};
create database {schema_name}
engine DataLakeCatalog('http://localhost:8080/api/2.1/unity-catalog')
settings warehouse = 'unity', catalog_type='unity', vended_credentials=True
        """,
        settings={"allow_database_unity_catalog": "1"},
    )

    # This query will fail if bug exists
    print(node1.query(f"SHOW TABLES FROM {schema_name}"))


@pytest.mark.parametrize("use_delta_kernel", ["1", "0"])
def test_view_with_void(started_cluster, use_delta_kernel):
    # 25/08/20 16:45:23 WARN ObjectStore: Version information not found in metastore. hive.metastore.schema.verification is not enabled so recording the schema version 2.3.0
    # 25/08/20 16:45:23 WARN ObjectStore: setMetaStoreSchemaVersion called but recording version is disabled: version = 2.3.0, comment = Set by MetaStore UNKNOWN@172.18.0.2
    # 25/08/20 16:45:24 WARN ObjectStore: Failed to get database global_temp, returning NoSuchObjectException
    # Catalog unity does not support views.
    pytest.skip("Unfortunately open source Unity Catalog doesn't support views")
    table_name_src = f"ntz_schema_{uuid.uuid4()}".replace("-", "_")
    node1 = started_cluster.instances["node1"]
    node1.query(f"drop database if exists {table_name_src}")

    schema_name = (
        f"schema_with_timetstamp_ntz_{use_delta_kernel}_{uuid.uuid4()}".replace(
            "-", "_"
        )
    )
    execute_spark_query(node1, f"CREATE SCHEMA {schema_name}")
    table_name = f"table_with_timestamp_{use_delta_kernel}_{uuid.uuid4()}".replace(
        "-", "_"
    )
    view_name = f"test_view_{table_name}"
    schema = "event_date DATE, event_time TIMESTAMP"
    create_query = f"CREATE TABLE {schema_name}.{table_name} ({schema}) using Delta location '/var/lib/clickhouse/user_files/tmp/{table_name_src}/{table_name}'"
    execute_spark_query(node1, create_query)
    execute_spark_query(
        node1,
        f"CREATE VIEW {schema_name}.{view_name} AS SELECT * FROM {schema_name}.{table_name}",
    )

    node1.query(
        f"""
drop database if exists {table_name};
create database {table_name_src}
engine DataLakeCatalog('http://localhost:8080/api/2.1/unity-catalog')
settings warehouse = 'unity', catalog_type='unity', vended_credentials=false, allow_experimental_delta_kernel_rs={use_delta_kernel}
        """,
        settings={"allow_experimental_database_unity_catalog": "1"},
    )

    ntz_tables = list(
        sorted(
            node1.query(
                f"SHOW TABLES FROM {table_name_src} LIKE '{schema_name}%'",
                settings={"use_hive_partitioning": "0"},
            )
            .strip()
            .split("\n")
        )
    )

    assert len(ntz_tables) == 1

    def get_schemas():
        return execute_spark_query(node1, f"SHOW SCHEMAS")

    assert schema_name in get_schemas()


def test_snapshot_version(started_cluster):
    """
    Test table in delta lake catalog with CDF settings
    (delta_lake_snapshot_start_version, delta_lake_snapshot_end_version).
    Checks that schema is correctly processed at each version after being altered.
    """
    node1 = started_cluster.instances["node1"]
    table_name = f"test_snapshot_version_{uuid.uuid4()}".replace("-", "_")
    table_path = f"/var/lib/clickhouse/user_files/tmp/{table_name}"
    db_name = f"db_{table_name}"

    def get_table_versions():
        history = execute_spark_query(
            node1, f"DESCRIBE HISTORY {schema_name}.{table_name}"
        )
        lines = [line.strip() for line in history.splitlines() if line.strip()]
        if "version" in lines[0].lower():
            lines = lines[1:]

        versions = [
            line.split("\t")[0].strip()
            for line in lines
            if line.split("\t")[0].strip().isdigit()
        ]
        versions = sorted(versions, key=int)

        print("Versions found:", versions)
        return versions

    schema_name = f"schema_{table_name}"

    schema = "event_date DATE, data STRING"
    create_query = f"""CREATE TABLE {schema_name}.{table_name} ({schema})
USING Delta location '{table_path}'
TBLPROPERTIES (
  delta.enableChangeDataFeed = true
)"""
    # Combine schema creation, table creation and initial insert into a single
    # Spark invocation to avoid multiple slow JVM startups.
    execute_multiple_spark_queries(
        node1,
        [
            f"CREATE SCHEMA {schema_name}",
            create_query,
            f"insert into {schema_name}.{table_name} SELECT to_date('2024-10-01', 'yyyy-MM-dd'), 'hello'",
        ],
    )

    # Create table with columns `event_date`, `data`
    node1.query(
        f"""
drop database if exists {db_name};
create database {db_name}
engine DataLakeCatalog('http://localhost:8080/api/2.1/unity-catalog')
settings warehouse = 'unity', catalog_type='unity', vended_credentials=false
        """,
        settings={"allow_database_unity_catalog": "1"},
    )

    # Validate data at version 1
    data_v1 = (
        node1.query(
            f"SELECT event_date, data, _commit_version FROM {db_name}.`{schema_name}.{table_name}`",
            settings={"delta_lake_snapshot_start_version": 1},
        )
        .strip()
        .split("\t")
    )

    assert data_v1[0] == "2024-10-01"
    assert data_v1[1] == "hello"
    assert data_v1[2] == "1"

    # Commit new data at version 2
    execute_spark_query(
        node1,
        f"insert into {schema_name}.{table_name} SELECT to_date('2024-10-02', 'yyyy-MM-dd'), 'world'",
    )

    # Check what commit versions we have now
    assert get_table_versions() == ["0", "1", "2"]

    # Validate data at version 2
    data_v2 = (
        node1.query(
            f"""
SELECT event_date, data, _commit_version
FROM {db_name}.`{schema_name}.{table_name}`
            """,
            settings={"delta_lake_snapshot_start_version": 2},
        )
        .strip()
        .split("\t")
    )

    assert data_v2[0] == "2024-10-02"
    assert data_v2[1] == "world"
    assert data_v2[2] == "2"

    # Validate schema at version 2
    data_v2 = (
        node1.query(
            f"""
SELECT event_date, data, _commit_version
FROM {db_name}.`{schema_name}.{table_name}`
            """,
            settings={"delta_lake_snapshot_start_version": 2},
        )
        .strip()
        .split("\t")
    )

    assert data_v2[0] == "2024-10-02"
    assert data_v2[1] == "world"
    assert data_v2[2] == "2"

    # Commit new data at version 3
    execute_spark_query(
        node1,
        f"insert into {schema_name}.{table_name} SELECT to_date('2024-10-03', 'yyyy-MM-dd'), 'from', 'Pepe' as name",
    )

    # Validate data between versions 1 and 2
    data_raw = node1.query(
        f"""
    SELECT event_date, data, _commit_version
    FROM {db_name}.`{schema_name}.{table_name}`
        """,
        settings={
            "delta_lake_snapshot_start_version": 1,
            "delta_lake_snapshot_end_version": 2,
        },
    ).strip()
    data_raw = [line.split("\t") for line in data_raw.splitlines()]
    data_v1_v2 = [item for row in data_raw for item in row]

    assert data_v1_v2[0] == "2024-10-01"
    assert data_v1_v2[1] == "hello"
    assert data_v1_v2[2] == "1"
    assert data_v1_v2[3] == "2024-10-02"
    assert data_v1_v2[4] == "world"
    assert data_v1_v2[5] == "2"

    # Validate data at version 3
    data_raw_v3 = node1.query(
        f"""
    SELECT event_date, data, name, _commit_version
    FROM {db_name}.`{schema_name}.{table_name}`
        """,
        settings={
            "delta_lake_snapshot_start_version": 3,
            "delta_lake_snapshot_end_version": 3,
        },
    ).strip()

    data_v3 = [line.split("\t") for line in data_raw_v3.splitlines()]
    flat_data_v3 = [item for row in data_v3 for item in row]

    assert flat_data_v3[0] == "2024-10-03"
    assert flat_data_v3[1] == "from"
    assert flat_data_v3[2] == "Pepe"
    assert flat_data_v3[3] == "3"

    # Validate latest schema
    assert (
        "event_date\tNullable(Date32)\t\t\t\t\t\n"
        "data\tNullable(String)\t\t\t\t\t\n"
        "name\tNullable(String)"
        == node1.query(f"DESCRIBE TABLE {db_name}.`{schema_name}.{table_name}`").strip()
    )

    # Validate schema at version 3
    assert (
        "event_date\tNullable(Date32)\t\t\t\t\t\n"
        "data\tNullable(String)\t\t\t\t\t\n"
        "name\tNullable(String)\t\t\t\t\t\n"
        "_change_type\tString\t\t\t\t\t\n"
        "_commit_version\tInt64\t\t\t\t\t\n"
        "_commit_timestamp\tDateTime64(6)"
        == node1.query(
            f"DESCRIBE TABLE {db_name}.`{schema_name}.{table_name}`",
            settings={
                "delta_lake_snapshot_start_version": 3,
            },
        ).strip()
    )

    # Validate schema at version 1 and 2
    assert (
        "event_date\tNullable(Date32)\t\t\t\t\t\n"
        "data\tNullable(String)\t\t\t\t\t\n"
        "_change_type\tString\t\t\t\t\t\n"
        "_commit_version\tInt64\t\t\t\t\t\n"
        "_commit_timestamp\tDateTime64(6)"
        == node1.query(
            f"DESCRIBE TABLE {db_name}.`{schema_name}.{table_name}`",
            settings={
                "delta_lake_snapshot_start_version": 1,
                "delta_lake_snapshot_end_version": 2,
            },
        ).strip()
    )
