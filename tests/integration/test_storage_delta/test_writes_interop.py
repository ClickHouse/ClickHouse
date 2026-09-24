"""
Delta Lake writes with a foreign reader or a second writer as the oracle: Spark and delta-rs
read-back of the type and partition matrix, Spark history/time travel/skipping, checkpoints, CDF,
VACUUM, concurrent writers, two-node read-after-write, cancel on S3/Azure, credential masking.
"""

import datetime
import decimal
import json
import logging
import os
import threading
import time

import pyarrow as pa
import pyspark
import pytest
from deltalake import DeltaTable
from deltalake.writer import write_deltalake

from helpers.cluster import ClickHouseCluster
from helpers.config_cluster import minio_access_key, minio_secret_key
from helpers.s3_tools import (
    AzureDownloader,
    AzureUploader,
    LocalDownloader,
    LocalUploader,
)
from helpers.spark_tools import ResilientSparkSession, write_spark_log_config
from test_storage_delta.test import (
    create_empty_delta_table,
    delta_engine_definition,
    delta_table_function,
    get_storage_options,
    list_delta_data_files,
    randomize_table_name,
)

USER_FILES = "/var/lib/clickhouse/user_files"

cluster = ClickHouseCluster(__file__, with_spark=True)


def get_spark(started_cluster):
    builder = (
        pyspark.sql.SparkSession.builder.appName("test_writes_interop")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.catalog.spark_catalog.warehouse", USER_FILES)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")
        # Spark reads the MinIO bucket directly (s3a), so ClickHouse writes to S3 need no download.
        .config(
            "spark.hadoop.fs.s3a.endpoint",
            f"http://{started_cluster.minio_ip}:{started_cluster.minio_port}/",
        )
        .config("spark.hadoop.fs.s3a.access.key", minio_access_key)
        .config("spark.hadoop.fs.s3a.secret.key", minio_secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.driver.memory", "2g")
        .config("spark.executor.memory", "2g")
        .master("local")
    )
    props_path = write_spark_log_config(started_cluster.instances_dir)
    builder = builder.config("spark.driver.extraJavaOptions", f"-Dlog4j2.configurationFile=file:{props_path}")
    return builder.getOrCreate()


@pytest.fixture(scope="module")
def started_cluster():
    try:
        for name in ("node1", "node2"):
            cluster.add_instance(
                name,
                main_configs=[
                    "configs/config.d/named_collections.xml",
                    "configs/config.d/disable_s3_retries.xml",
                ],
                user_configs=[
                    "configs/users.d/users.xml",
                    "configs/users.d/enable_writes.xml",
                ],
                with_minio=True,
                with_azurite=(name == "node1"),
                stay_alive=True,
            )
        logging.info("Starting cluster...")
        cluster.start()
        if int(cluster.instances["node1"].query("SELECT count() FROM system.table_engines WHERE name = 'DeltaLake'").strip()) == 0:
            pytest.skip("DeltaLake engine is not available")
        cluster.azure_container_name = "mycontainer"
        cluster.container_client = cluster.blob_service_client.create_container(cluster.azure_container_name)
        cluster.default_azure_uploader = AzureUploader(cluster.blob_service_client, cluster.azure_container_name)
        cluster.spark_session = ResilientSparkSession(lambda: get_spark(cluster))
        yield cluster
    finally:
        cluster.shutdown()


# ---------------------------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------------------------


def local_table_path(table_name):
    return f"{USER_FILES}/{table_name}"


def push_to_node(node, path):
    """Copy a table directory written by Spark/delta-rs (in the runner) into the ClickHouse node."""
    LocalUploader(node).upload_directory(f"{path}/", f"{path}/")


def pull_from_node(node, path):
    """Copy a table directory written by ClickHouse back into the runner for Spark/delta-rs."""
    LocalDownloader(node).download_directory(f"{path}/", f"{path}/")


def s3_path(started_cluster, path):
    return f"s3a://{started_cluster.minio_bucket}/{path}"


def deltars_table(started_cluster, storage_type, path):
    if storage_type == "s3":
        return DeltaTable(
            f"s3://{started_cluster.minio_bucket}/{path}",
            storage_options=get_storage_options(started_cluster),
        )
    return DeltaTable(f"file://{path}")


def normalize(value):
    """Reduce Spark / delta-rs / Python values to one comparable representation."""
    if isinstance(value, decimal.Decimal):
        return str(value)
    if isinstance(value, datetime.datetime):
        if value.tzinfo is not None:
            value = value.astimezone(datetime.timezone.utc).replace(tzinfo=None)
        return value.isoformat(sep=" ")
    if isinstance(value, datetime.date):
        return value.isoformat()
    if isinstance(value, (bytes, bytearray)):
        return bytes(value)
    if isinstance(value, float):
        return round(value, 6)
    if hasattr(value, "asDict"):  # pyspark Row (struct); a tuple subclass, so check it first
        return tuple(sorted((k, normalize(v)) for k, v in value.asDict().items()))
    if isinstance(value, dict):
        return tuple(sorted((k, normalize(v)) for k, v in value.items()))
    if isinstance(value, (list, tuple)):
        return tuple(normalize(v) for v in value)
    return value


def spark_rows(spark, path, order_by, columns="*"):
    df = spark.read.format("delta").load(path)
    return [tuple(normalize(v) for v in row) for row in df.selectExpr(*([columns] if isinstance(columns, str) else columns)).orderBy(order_by).collect()]


def deltars_rows(table, order_by):
    rows = table.to_pyarrow_table().to_pylist()
    rows.sort(key=lambda r: r[order_by])
    return [tuple(normalize(v) for v in r.values()) for r in rows]


def log_versions(started_cluster, storage_type, path):
    """Sorted list of committed version numbers from the `_delta_log` listing."""
    if storage_type == "s3":
        names = [obj.object_name for obj in started_cluster.minio_client.list_objects(started_cluster.minio_bucket, f"{path}/_delta_log/", recursive=True)]
    else:
        names = [blob.name for blob in started_cluster.container_client.list_blobs(name_starts_with=f"{path}/_delta_log/")]
    return sorted(int(os.path.basename(n)[:-5]) for n in names if n.endswith(".json"))


def committed_add_paths(started_cluster, storage_type, path):
    """Every `add.path` across all commits, relative to the table root."""
    result = []
    if storage_type == "s3":
        objects = started_cluster.minio_client.list_objects(started_cluster.minio_bucket, f"{path}/_delta_log/", recursive=True)
        for obj in objects:
            if not obj.object_name.endswith(".json"):
                continue
            body = started_cluster.minio_client.get_object(started_cluster.minio_bucket, obj.object_name).read()
            for line in body.decode().splitlines():
                action = json.loads(line)
                if "add" in action:
                    result.append(action["add"]["path"])
    return result


def assert_committed_files_exist(started_cluster, path):
    """Every `add.path` of every commit (from any writer) is an object in the bucket."""
    objects = {obj.object_name for obj in started_cluster.minio_client.list_objects(started_cluster.minio_bucket, f"{path}/", recursive=True)}
    missing = {f"{path}/{p}" for p in committed_add_paths(started_cluster, "s3", path)} - objects
    assert not missing, missing


# ---------------------------------------------------------------------------------------------
# type and partition matrix, read back by Spark and delta-rs
# ---------------------------------------------------------------------------------------------


def test_type_matrix_spark_and_deltars_readback(started_cluster):
    node = started_cluster.instances["node1"]
    spark = started_cluster.spark_session
    table_name = randomize_table_name("test_type_matrix")
    path = local_table_path(table_name)

    spark.sql(
        f"""
        CREATE TABLE delta.`{path}` (
            i INT, b TINYINT, sh SMALLINT, l BIGINT, f FLOAT, d DOUBLE,
            dec9 DECIMAL(9, 2), dec18 DECIMAL(18, 4), dec38 DECIMAL(38, 10),
            s STRING, bin BINARY, dt DATE, ts TIMESTAMP, bo BOOLEAN,
            arr ARRAY<INT>, mp MAP<STRING, INT>, st STRUCT<x: INT, y: STRING>
        ) USING delta
        """
    )
    push_to_node(node, path)

    node.query(
        f"""
        CREATE TABLE {table_name} (
            i Int32, b Nullable(Int8), sh Int16, l Int64, f Float32, d Float64,
            dec9 Decimal(9, 2), dec18 Decimal(18, 4), dec38 Decimal(38, 10),
            s Nullable(String), bin String, dt Nullable(Date32), ts DateTime64(6), bo Bool,
            arr Array(Int32), mp Map(String, Int32), st Tuple(x Int32, y String)
        ) ENGINE = DeltaLakeLocal('{path}')
        """
    )
    node.query(
        f"""
        INSERT INTO {table_name} VALUES
            (1, 127, -32768, 9223372036854775807, 1.5, 2.25, 1234567.89, 12345678901234.5678,
             1234567890123456789012345678.0123456789, 'text', 'bytes\\x00\\xff', '2024-06-01',
             '2024-06-01 09:00:00.123456', true, [1, 2, 3], {{'a': 1, 'b': 2}}, (7, 'seven')),
            (2, NULL, 0, 0, -0.25, 0, -0.01, 0, 0, NULL, '', NULL,
             '1970-01-01 00:00:00', false, [], {{}}, (0, ''))
        """
    )
    pull_from_node(node, path)

    expected = [
        (
            1,
            127,
            -32768,
            9223372036854775807,
            1.5,
            2.25,
            "1234567.89",
            "12345678901234.5678",
            "1234567890123456789012345678.0123456789",
            "text",
            b"bytes\x00\xff",
            "2024-06-01",
            "2024-06-01 09:00:00.123456",
            True,
            (1, 2, 3),
            (("a", 1), ("b", 2)),
            (("x", 7), ("y", "seven")),
        ),
        (
            2,
            None,
            0,
            0,
            -0.25,
            0.0,
            "-0.01",
            "0.0000",
            "0E-10",
            None,
            b"",
            None,
            "1970-01-01 00:00:00",
            False,
            (),
            (),
            (("x", 0), ("y", "")),
        ),
    ]

    spark_result = spark_rows(spark, path, "i")
    assert spark_result == expected, f"Spark read back {spark_result}"

    deltars_result = deltars_rows(deltars_table(started_cluster, "local", path), "i")
    assert deltars_result == expected, f"delta-rs read back {deltars_result}"

    assert node.query(f"SELECT count(), sum(i) FROM {table_name}").strip() == "2\t3"


def test_partition_types_spark_readback_and_pruning(started_cluster):
    # Partition values of every partitionable type as ClickHouse serializes them, read by Spark
    # both as a column and as a pruning predicate (different code paths), and by delta-rs.
    node = started_cluster.instances["node1"]
    spark = started_cluster.spark_session
    path = randomize_table_name("test_partition_types")
    # Spark creates the table on MinIO (delta-rs cannot create a decimal-partitioned table).
    spark.sql(
        f"""
        CREATE TABLE delta.`{s3_path(started_cluster, path)}` (
            id INT, p_bool BOOLEAN, p_date DATE, p_ts TIMESTAMP, p_dec DECIMAL(10, 2),
            p_int INT, p_str STRING
        ) USING delta PARTITIONED BY (p_bool, p_date, p_ts, p_dec, p_int, p_str)
        """
    )
    node.query(
        f"""
        CREATE TABLE {path} (
            id Int32, p_bool Bool, p_date Date32, p_ts DateTime64(6), p_dec Decimal(10, 2),
            p_int Nullable(Int32), p_str Nullable(String)
        ) ENGINE = {delta_engine_definition(started_cluster, "s3", path)}
        """
    )
    rows = [
        (1, "true", "'2024-01-31'", "'2024-01-31 23:59:59.5'", "1.5", "7", "'plain'"),
        (2, "false", "'1970-01-01'", "'1970-01-01 00:00:00'", "0", "-1", "'a b'"),
        (3, "true", "'2000-02-29'", "'2000-02-29 12:00:00'", "12345678.99", "NULL", "'x/y=z%'"),
        (4, "false", "'2024-01-31'", "'2024-01-31 23:59:59.5'", "1.5", "7", "'日本語'"),
        (5, "true", "'2024-01-31'", "'2024-01-31 23:59:59.5'", "1.5", "7", "NULL"),
    ]
    node.query(f"INSERT INTO {path} VALUES " + ", ".join("(" + ", ".join(str(v) for v in row) + ")" for row in rows))

    location = s3_path(started_cluster, path)
    table = f"delta.`{location}`"
    assert {r.id for r in spark.sql(f"SELECT id FROM {table}").collect()} == {1, 2, 3, 4, 5}

    def ids(where):
        return {r.id for r in spark.sql(f"SELECT id FROM {table} WHERE {where}").collect()}

    assert ids("p_bool") == {1, 3, 5}
    assert ids("p_date = DATE '2024-01-31'") == {1, 4, 5}
    assert ids("p_ts = TIMESTAMP '2024-01-31 23:59:59.5'") == {1, 4, 5}
    assert ids("p_ts = TIMESTAMP '1970-01-01 00:00:00'") == {2}
    assert ids("p_dec = 1.50") == {1, 4, 5}
    assert ids("p_dec = 12345678.99") == {3}
    assert ids("p_int = 7") == {1, 4, 5}
    assert ids("p_int IS NULL") == {3}
    assert ids("p_str = 'a b'") == {2}
    assert ids("p_str = 'x/y=z%'") == {3}
    assert ids("p_str = '日本語'") == {4}
    assert ids("p_str IS NULL") == {5}

    # Pruning, not just filtering: the plan for a single-partition predicate reads one file.
    def files_read(where):
        return len(spark.sql(f"SELECT id FROM {table} WHERE {where}").inputFiles())

    assert files_read("1 = 1") == 5
    assert files_read("p_str = 'a b'") == 1
    assert files_read("p_dec = 12345678.99") == 1
    assert files_read("p_ts = TIMESTAMP '1970-01-01 00:00:00'") == 1
    assert files_read("p_int = 7") == 3

    got = spark_rows(
        spark,
        location,
        "id",
        ["id", "p_bool", "p_date", "cast(p_ts as string)", "cast(p_dec as string)", "p_int", "p_str"],
    )
    assert got == [
        (1, True, "2024-01-31", "2024-01-31 23:59:59.5", "1.50", 7, "plain"),
        (2, False, "1970-01-01", "1970-01-01 00:00:00", "0.00", -1, "a b"),
        (3, True, "2000-02-29", "2000-02-29 12:00:00", "12345678.99", None, "x/y=z%"),
        (4, False, "2024-01-31", "2024-01-31 23:59:59.5", "1.50", 7, "日本語"),
        (5, True, "2024-01-31", "2024-01-31 23:59:59.5", "1.50", 7, None),
    ], got

    node.query(f"DROP TABLE {path}")

    # delta-rs (kernel-based) cannot open a table with the decimal partition column: ClickHouse
    # commits "1.5" instead of the scale-exact "1.50" (https://github.com/ClickHouse/ClickHouse/issues/120521),
    # Spark tolerates it. The same rows without the decimal column must read back exactly in delta-rs.
    path_rs = randomize_table_name("test_partition_types_rs")
    spark.sql(
        f"""
        CREATE TABLE delta.`{s3_path(started_cluster, path_rs)}` (
            id INT, p_bool BOOLEAN, p_date DATE, p_ts TIMESTAMP, p_int INT, p_str STRING
        ) USING delta PARTITIONED BY (p_bool, p_date, p_ts, p_int, p_str)
        """
    )
    node.query(
        f"""
        CREATE TABLE {path_rs} (
            id Int32, p_bool Bool, p_date Date32, p_ts DateTime64(6), p_int Nullable(Int32), p_str Nullable(String)
        ) ENGINE = {delta_engine_definition(started_cluster, "s3", path_rs)}
        """
    )
    node.query(f"INSERT INTO {path_rs} VALUES " + ", ".join("(" + ", ".join(str(v) for v in (row[0], row[1], row[2], row[3], row[5], row[6])) + ")" for row in rows))
    got = deltars_rows(deltars_table(started_cluster, "s3", path_rs), "id")
    assert got == [
        (1, True, "2024-01-31", "2024-01-31 23:59:59.500000", 7, "plain"),
        (2, False, "1970-01-01", "1970-01-01 00:00:00", -1, "a b"),
        (3, True, "2000-02-29", "2000-02-29 12:00:00", None, "x/y=z%"),
        (4, False, "2024-01-31", "2024-01-31 23:59:59.500000", 7, "日本語"),
        (5, True, "2024-01-31", "2024-01-31 23:59:59.500000", 7, None),
    ], got
    node.query(f"DROP TABLE {path_rs}")


# ---------------------------------------------------------------------------------------------
# Spark metadata operations over ClickHouse commits
# ---------------------------------------------------------------------------------------------


def test_spark_history_time_travel_and_data_skipping(started_cluster):
    node = started_cluster.instances["node1"]
    spark = started_cluster.spark_session
    table_name = randomize_table_name("test_history")
    path = local_table_path(table_name)

    spark.sql(f"CREATE TABLE delta.`{path}` (id INT, v STRING) USING delta")
    push_to_node(node, path)
    node.query(f"CREATE TABLE {table_name} (id Int32, v String) ENGINE = DeltaLakeLocal('{path}')")
    for batch in range(3):
        node.query(f"INSERT INTO {table_name} SELECT number + {batch * 100}, toString(number) FROM numbers(10)")
    pull_from_node(node, path)

    table = f"delta.`{path}`"
    history = spark.sql(f"DESCRIBE HISTORY {table}").collect()
    assert sorted(r.version for r in history) == [0, 1, 2, 3]
    for r in history:
        if r.version > 0:
            assert r.engineInfo is not None and "ClickHouse" in r.engineInfo, r

    assert spark.sql(f"SELECT count(*) AS c FROM {table} VERSION AS OF 1").collect()[0].c == 10
    assert spark.sql(f"SELECT count(*) AS c FROM {table} VERSION AS OF 2").collect()[0].c == 20
    assert spark.sql(f"SELECT count(*) AS c FROM {table}").collect()[0].c == 30

    # The same rows with and without stats-based file skipping (Spark must not skip a
    # ClickHouse-written file it should read, nor read one it must not).
    predicate = "id BETWEEN 105 AND 210"
    with_skipping = sorted(r.id for r in spark.sql(f"SELECT id FROM {table} WHERE {predicate}").collect())
    spark.conf.set("spark.databricks.delta.stats.skipping", "false")
    try:
        without_skipping = sorted(r.id for r in spark.sql(f"SELECT id FROM {table} WHERE {predicate}").collect())
    finally:
        spark.conf.set("spark.databricks.delta.stats.skipping", "true")
    assert with_skipping == without_skipping == list(range(105, 110)) + list(range(200, 210))

    # ClickHouse time travel over its own commits agrees with Spark.
    assert node.query(f"SELECT count() FROM {table_name} SETTINGS delta_lake_snapshot_version = 1").strip() == "10"


def test_interleaved_spark_and_clickhouse_appends(started_cluster):
    node = started_cluster.instances["node1"]
    spark = started_cluster.spark_session
    table_name = randomize_table_name("test_interleaved")
    path = local_table_path(table_name)
    table = f"delta.`{path}`"

    spark.sql(f"CREATE TABLE {table} (id INT, who STRING) USING delta")
    spark.sql(f"INSERT INTO {table} SELECT id, 'spark' FROM range(0, 5)")
    push_to_node(node, path)

    node.query(f"CREATE TABLE {table_name} (id Int32, who String) ENGINE = DeltaLakeLocal('{path}')")
    node.query(f"INSERT INTO {table_name} SELECT number + 5, 'clickhouse' FROM numbers(5)")
    pull_from_node(node, path)

    spark.sql(f"INSERT INTO {table} SELECT id, 'spark' FROM range(10, 15)")
    push_to_node(node, path)

    node.query(f"INSERT INTO {table_name} SELECT number + 15, 'clickhouse' FROM numbers(5)")
    pull_from_node(node, path)

    expected = [(i, "spark" if i < 5 or 10 <= i < 15 else "clickhouse") for i in range(20)]
    assert spark_rows(spark, path, "id") == expected
    assert deltars_rows(deltars_table(started_cluster, "local", path), "id") == expected
    assert node.query(f"SELECT id, who FROM {table_name} ORDER BY id FORMAT TSV") == "".join(f"{i}\t{w}\n" for i, w in expected)
    assert sorted(r.version for r in spark.sql(f"DESCRIBE HISTORY {table}").collect()) == [0, 1, 2, 3, 4]


def test_checkpoint_interplay(started_cluster):
    node = started_cluster.instances["node1"]
    spark = started_cluster.spark_session
    table_name = randomize_table_name("test_checkpoint")
    path = local_table_path(table_name)
    table = f"delta.`{path}`"

    spark.sql(f"CREATE TABLE {table} (id INT) USING delta TBLPROPERTIES (delta.checkpointInterval = 3)")
    for batch in range(3):  # versions 1..3, Spark checkpoints at 3
        spark.sql(f"INSERT INTO {table} SELECT id FROM range({batch * 10}, {batch * 10 + 10})")
    push_to_node(node, path)
    assert "3" in node.exec_in_container(["bash", "-c", f"cat {path}/_delta_log/_last_checkpoint"])

    node.query(f"CREATE TABLE {table_name} (id Int32) ENGINE = DeltaLakeLocal('{path}')")
    for batch in range(3, 8):  # versions 4..8 by ClickHouse
        node.query(f"INSERT INTO {table_name} SELECT number + {batch * 10} FROM numbers(10)")
    assert node.query(f"SELECT count() FROM {table_name}").strip() == "80"
    pull_from_node(node, path)

    spark.sql(f"INSERT INTO {table} SELECT id FROM range(80, 90)")  # version 9: Spark checkpoints
    checkpoints = spark.sql(f"DESCRIBE HISTORY {table}").count()
    assert checkpoints == 10
    last_checkpoint = json.loads(open(f"{path}/_delta_log/_last_checkpoint").read())
    assert last_checkpoint["version"] == 9, last_checkpoint
    push_to_node(node, path)

    node.query(f"INSERT INTO {table_name} SELECT number + 90 FROM numbers(10)")  # version 10
    fresh = randomize_table_name("test_checkpoint_fresh")
    node.query(f"CREATE TABLE {fresh} (id Int32) ENGINE = DeltaLakeLocal('{path}')")
    assert node.query(f"SELECT count(), min(id), max(id), uniqExact(id) FROM {fresh}").strip() == "100\t0\t99\t100"
    pull_from_node(node, path)

    assert spark.sql(f"SELECT count(*) AS c, count(DISTINCT id) AS d FROM {table}").collect()[0][:] == (100, 100)
    assert len(deltars_rows(deltars_table(started_cluster, "local", path), "id")) == 100


def test_write_to_table_with_unsupported_writer_feature_is_rejected(started_cluster):
    # Enabling CDF in Spark raises the writer protocol to a level whose legacy features the
    # kernel does not implement; the write must fail closed and leave the table as Spark left it.
    node = started_cluster.instances["node1"]
    spark = started_cluster.spark_session
    table_name = randomize_table_name("test_unsupported_feature")
    path = local_table_path(table_name)
    table = f"delta.`{path}`"

    spark.sql(f"CREATE TABLE {table} (id INT, v STRING) USING delta TBLPROPERTIES (delta.enableChangeDataFeed = true)")
    spark.sql(f"INSERT INTO {table} VALUES (1, 'spark')")
    push_to_node(node, path)

    node.query(f"CREATE TABLE {table_name} (id Int32, v String) ENGINE = DeltaLakeLocal('{path}')")
    error = node.query_and_get_error(f"INSERT INTO {table_name} VALUES (2, 'clickhouse'), (3, 'clickhouse')")
    assert "DELTA_KERNEL_ERROR" in error and "is not supported" in error, error
    assert node.query(f"SELECT count() FROM {table_name}").strip() == "1"
    pull_from_node(node, path)
    assert sorted(f for f in os.listdir(f"{path}/_delta_log") if f.endswith(".json")) == [
        "00000000000000000000.json",
        "00000000000000000001.json",
    ]
    assert len([f for f in os.listdir(path) if f.endswith(".parquet")]) == 1
    assert spark_rows(spark, path, "id") == [(1, "spark")]


def test_vacuum_after_clickhouse_writes(started_cluster):
    node = started_cluster.instances["node1"]
    spark = started_cluster.spark_session
    table_name = randomize_table_name("test_vacuum")
    path = local_table_path(table_name)
    table = f"delta.`{path}`"

    spark.sql(f"CREATE TABLE {table} (id INT) USING delta")
    push_to_node(node, path)
    node.query(f"CREATE TABLE {table_name} (id Int32) ENGINE = DeltaLakeLocal('{path}')")
    node.query(f"INSERT INTO {table_name} SELECT number FROM numbers(10)")
    node.query(f"INSERT INTO {table_name} SELECT number + 10 FROM numbers(10)")
    # A failed write must not leave an orphan for VACUUM to find.
    error = node.query_and_get_error(f"INSERT INTO {table_name} SELECT throwIf(number = 5, 'boom') + number FROM numbers(10) SETTINGS max_block_size = 1")
    assert "FUNCTION_THROW_IF_VALUE_IS_NON_ZERO" in error
    pull_from_node(node, path)

    live_files = sorted(f for f in os.listdir(path) if f.endswith(".parquet"))
    assert len(live_files) == 2, live_files

    # Plant an orphan (an uncommitted data file) to prove VACUUM does run.
    orphan = f"{path}/orphan.parquet"
    with open(f"{path}/{live_files[0]}", "rb") as src, open(orphan, "wb") as dst:
        dst.write(src.read())
    os.utime(orphan, (0, 0))

    spark.sql(f"VACUUM {table} RETAIN 0 HOURS")

    remaining = sorted(f for f in os.listdir(path) if f.endswith(".parquet"))
    assert remaining == live_files, remaining
    assert spark.sql(f"SELECT count(*) AS c FROM {table}").collect()[0].c == 20
    push_to_node(node, path)
    assert node.query(f"SELECT count() FROM {table_name}").strip() == "20"


# ---------------------------------------------------------------------------------------------
# multi-node and object-storage read-back
# ---------------------------------------------------------------------------------------------


def test_read_after_write_on_other_node(started_cluster):
    node1 = started_cluster.instances["node1"]
    node2 = started_cluster.instances["node2"]
    path = randomize_table_name("test_read_after_write")
    schema = pa.schema([("id", pa.int32(), False), ("who", pa.string(), False)])
    create_empty_delta_table(started_cluster, "s3", path, schema)

    engine = delta_engine_definition(started_cluster, "s3", path)
    for node in (node1, node2):
        node.query(f"CREATE TABLE t_raw (id Int32, who String) ENGINE = {engine}")

    assert node2.query("SELECT count() FROM t_raw").strip() == "0"
    node1.query("INSERT INTO t_raw SELECT number, 'node1' FROM numbers(10)")
    assert node2.query("SELECT count() FROM t_raw").strip() == "10"
    node2.query("INSERT INTO t_raw SELECT number + 10, 'node2' FROM numbers(5)")
    assert node1.query("SELECT count(), uniqExact(id) FROM t_raw").strip() == "15\t15"
    assert log_versions(started_cluster, "s3", path) == [0, 1, 2]
    for node in (node1, node2):
        node.query("DROP TABLE t_raw")


@pytest.mark.parametrize("storage_type", ["s3", "azure"])
@pytest.mark.parametrize("partitioned", [False, True])
def test_spark_and_deltars_read_object_storage_writes(started_cluster, storage_type, partitioned):
    node = started_cluster.instances["node1"]
    spark = started_cluster.spark_session
    path = randomize_table_name(f"test_readback_{storage_type}")
    schema = pa.schema([("id", pa.int32(), False), ("part", pa.string(), True), ("v", pa.float64(), False)])
    create_empty_delta_table(started_cluster, storage_type, path, schema, partition_by=["part"] if partitioned else None)
    node.query(f"CREATE TABLE {path} (id Int32, part Nullable(String), v Float64) ENGINE = {delta_engine_definition(started_cluster, storage_type, path)}")
    node.query(f"INSERT INTO {path} SELECT number, if(number % 3 = 0, NULL, 'p ' || toString(number % 3)), number / 4 FROM numbers(12)")
    node.query(f"INSERT INTO {path} SELECT number + 12, 'q/r', 0 FROM numbers(3)")

    expected = [(i, None if i % 3 == 0 else f"p {i % 3}", i / 4) for i in range(12)] + [(12 + i, "q/r", 0.0) for i in range(3)]

    if storage_type == "s3":
        spark_location = s3_path(started_cluster, path)
        deltars = deltars_table(started_cluster, "s3", path)
    else:
        local = f"{USER_FILES}/{path}_azure_copy"
        os.makedirs(local, exist_ok=True)
        AzureDownloader(started_cluster.blob_service_client, started_cluster.azure_container_name).download_directory(local, path)
        spark_location = local
        deltars = deltars_table(started_cluster, "local", local)

    assert spark_rows(spark, spark_location, "id") == expected
    assert deltars_rows(deltars, "id") == expected
    assert node.query(f"SELECT count(), sum(v) FROM {path}").strip() == "15\t16.5"
    node.query(f"DROP TABLE {path}")


# ---------------------------------------------------------------------------------------------
# concurrency
# ---------------------------------------------------------------------------------------------


@pytest.mark.parametrize("partitioned", [False, True])
def test_barrier_synchronised_concurrent_appends(started_cluster, partitioned):
    node = started_cluster.instances["node1"]
    spark = started_cluster.spark_session
    path = randomize_table_name("test_barrier_appends")
    schema = pa.schema([("id", pa.int32(), False), ("part", pa.string(), False)])
    create_empty_delta_table(started_cluster, "s3", path, schema, partition_by=["part"] if partitioned else None)
    node.query(f"CREATE TABLE {path} (id Int32, part String) ENGINE = {delta_engine_definition(started_cluster, 's3', path)}")

    writers = 6
    rows_per_insert = 20
    barrier = threading.Barrier(writers)
    outcomes = [None] * writers

    def writer(i):
        barrier.wait()
        try:
            node.query(f"INSERT INTO {path} SELECT number + {i * 1000}, toString(number % 2) FROM numbers({rows_per_insert})")
            outcomes[i] = "ok"
        except Exception as e:  # pylint: disable=broad-except
            outcomes[i] = str(e)

    for _ in range(3):  # a round may see no conflict at all on a fast machine
        threads = [threading.Thread(target=writer, args=(i,)) for i in range(writers)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        if any(o != "ok" for o in outcomes):
            break
        barrier = threading.Barrier(writers)

    successes = sum(1 for o in outcomes if o == "ok")
    failures = [o for o in outcomes if o != "ok"]
    logging.info("barrier round: %s successes, %s failures", successes, len(failures))
    assert failures, "no writer lost the race in three rounds: the conflict path was not exercised"
    for f in failures:
        assert "commit conflict at version" in f, f

    versions = log_versions(started_cluster, "s3", path)
    total_inserts = versions[-1]
    assert versions == list(range(total_inserts + 1)), versions
    assert node.query(f"SELECT count() FROM {path}").strip() == str(total_inserts * rows_per_insert)

    data_files = list_delta_data_files(started_cluster, "s3", path)
    adds = committed_add_paths(started_cluster, "s3", path)
    assert len(adds) == len(set(adds))
    # Loser cleanup: every ClickHouse data file in the bucket is referenced by a commit.
    referenced = {f"{path}/{p}" for p in adds}
    assert set(data_files) == referenced, set(data_files) ^ referenced
    assert len(data_files) == total_inserts * (2 if partitioned else 1)

    assert spark.sql(f"SELECT count(*) AS c FROM delta.`{s3_path(started_cluster, path)}`").collect()[0].c == total_inserts * rows_per_insert
    history = spark.sql(f"DESCRIBE HISTORY delta.`{s3_path(started_cluster, path)}`").collect()
    assert sorted(r.version for r in history) == versions
    node.query(f"DROP TABLE {path}")


def test_concurrent_clickhouse_and_deltars_appends(started_cluster):
    node = started_cluster.instances["node1"]
    path = randomize_table_name("test_ch_deltars_race")
    schema = pa.schema([("id", pa.int64(), False), ("who", pa.string(), False)])
    create_empty_delta_table(started_cluster, "s3", path, schema)
    node.query(f"CREATE TABLE {path} (id Int64, who String) ENGINE = {delta_engine_definition(started_cluster, 's3', path)}")
    safe_storage_options = {k: v for k, v in get_storage_options(started_cluster).items() if k != "AWS_S3_ALLOW_UNSAFE_RENAME"}
    safe_storage_options["conditional_put"] = "etag"

    def deltars_append(r):
        batch = pa.Table.from_pydict({"id": pa.array([10000 + r * 100 + i for i in range(10)], pa.int64()), "who": pa.array(["delta-rs"] * 10)})
        write_deltalake(f"s3://{started_cluster.minio_bucket}/{path}", batch, storage_options=safe_storage_options, mode="append")

    # Deterministic conflict: the ClickHouse transaction is opened when the sink is created, before the
    # pipeline starts executing, so a delta-rs commit during the slow SELECT makes the ClickHouse commit
    # lose. The delta-rs append is fired only once the INSERT is observably executing (it has read rows),
    # not after a fixed delay.
    slow_query_id = f"{path}_slow_insert"
    slow_result = []
    slow_insert = threading.Thread(
        target=lambda: slow_result.append(
            node.query_and_get_answer_with_error(
                f"INSERT INTO {path} SELECT number, 'clickhouse' FROM (SELECT number FROM numbers(10) WHERE sleepEachRow(1) = 0)"
                " SETTINGS max_block_size = 1, min_insert_block_size_rows = 1, max_threads = 1",
                query_id=slow_query_id,
            )
        )
    )
    slow_insert.start()
    deadline = time.monotonic() + 60
    while int(node.query(f"SELECT coalesce(max(read_rows), 0) FROM system.processes WHERE query_id = '{slow_query_id}'").strip()) < 1:
        assert time.monotonic() < deadline, "the slow INSERT did not start reading rows in 60s"
        assert slow_insert.is_alive(), slow_result
        time.sleep(0.1)
    deltars_append(0)
    slow_insert.join()
    assert "commit conflict at version 1" in slow_result[0][1], slow_result
    assert log_versions(started_cluster, "s3", path) == [0, 1]
    assert node.query(f"SELECT who, count() FROM {path} GROUP BY who FORMAT TSV") == "delta-rs\t10\n"
    # The loser removed its own data files, and the winner's file is still there.
    assert list_delta_data_files(started_cluster, "s3", path) == []
    assert_committed_files_exist(started_cluster, path)

    # Interleaved appends from both writers; every acknowledged commit must be visible to both readers.
    rounds = 8
    ch_ok, rs_ok, ch_errors, rs_errors = [], [0], [], []
    barrier = threading.Barrier(2)

    def clickhouse_writer():
        for r in range(1, rounds):
            barrier.wait()
            try:
                node.query(f"INSERT INTO {path} SELECT number + {r * 100}, 'clickhouse' FROM numbers(10)")
                ch_ok.append(r)
            except Exception as e:  # pylint: disable=broad-except
                ch_errors.append(str(e))

    def deltars_writer():
        for r in range(1, rounds):
            barrier.wait()
            try:
                deltars_append(r)
                rs_ok.append(r)
            except Exception as e:  # pylint: disable=broad-except
                rs_errors.append(str(e))

    threads = [threading.Thread(target=clickhouse_writer), threading.Thread(target=deltars_writer)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    logging.info("clickhouse ok=%s errors=%s; delta-rs ok=%s errors=%s", len(ch_ok), ch_errors, len(rs_ok), rs_errors)

    for e in ch_errors:
        assert "commit conflict at version" in e, e
    assert rs_ok, f"delta-rs never committed: {rs_errors}"
    for e in rs_errors:
        assert any(m in e.lower() for m in ("conflict", "already exists", "version", "precondition")), e

    versions = log_versions(started_cluster, "s3", path)
    assert versions == list(range(len(ch_ok) + len(rs_ok) + 1)), versions
    result = node.query(f"SELECT who, count() FROM {path} GROUP BY who ORDER BY who FORMAT TSV")
    expected = ""
    if ch_ok:
        expected += f"clickhouse\t{len(ch_ok) * 10}\n"
    if rs_ok:
        expected += f"delta-rs\t{len(rs_ok) * 10}\n"
    assert result == expected, result
    assert len(deltars_rows(deltars_table(started_cluster, "s3", path), "id")) == (len(ch_ok) + len(rs_ok)) * 10
    # No orphan from a losing ClickHouse commit, no committed file missing from either writer.
    assert len(list_delta_data_files(started_cluster, "s3", path)) == len(ch_ok)
    assert_committed_files_exist(started_cluster, path)
    node.query(f"DROP TABLE {path}")


# ---------------------------------------------------------------------------------------------
# cancel / failure on object storage (local storage is covered in test.py)
# ---------------------------------------------------------------------------------------------


@pytest.mark.parametrize("partitioned", [False, True])
@pytest.mark.parametrize("storage_type", ["s3", "azure"])
def test_kill_query_on_object_storage_leaves_no_orphans(started_cluster, storage_type, partitioned):
    node = started_cluster.instances["node1"]
    path = randomize_table_name(f"test_kill_{storage_type}")
    schema = pa.schema([("id", pa.int32(), False), ("part", pa.int32(), False)])
    create_empty_delta_table(started_cluster, storage_type, path, schema, partition_by=["part"] if partitioned else None)
    node.query(f"CREATE TABLE {path} (id Int32, part Int32) ENGINE = {delta_engine_definition(started_cluster, storage_type, path)}")
    query_id = f"{path}_insert"
    outcome = {}

    def slow_insert():
        # Hold each row so the KILL lands while the object-storage buffers are open.
        outcome["result"] = node.query_and_get_answer_with_error(
            f"INSERT INTO {path} SELECT number, number % 2 FROM numbers(30) "
            f"WHERE sleepEachRow(0.2) = 0 "
            f"SETTINGS max_block_size = 1, max_insert_threads = 1, "
            f"function_sleep_max_microseconds_per_block = 2000000, max_execution_time = 120",
            query_id=query_id,
        )

    thread = threading.Thread(target=slow_insert)
    thread.start()
    # Objects only appear in the listing once finalized, so "writing started" is observed through
    # the rows the INSERT has already pulled into the sink.
    started_writing = False
    for _ in range(150):
        read_rows = node.query(f"SELECT read_rows FROM system.processes WHERE query_id = '{query_id}'").strip()
        if read_rows and int(read_rows) >= 3:
            started_writing = True
            break
        time.sleep(0.2)
    node.query(f"KILL QUERY WHERE query_id = '{query_id}' SYNC")
    thread.join()
    assert started_writing, f"the INSERT never started consuming rows before the KILL: {outcome}"
    _, error = outcome["result"]
    assert "QUERY_WAS_CANCELLED" in error, error

    assert node.query("SELECT 1").strip() == "1"
    assert log_versions(started_cluster, storage_type, path) == [0]
    assert list_delta_data_files(started_cluster, storage_type, path) == []
    assert node.query(f"SELECT count() FROM {path}").strip() == "0"
    node.query(f"DROP TABLE {path}")


@pytest.mark.parametrize("partitioned", [False, True])
@pytest.mark.parametrize("storage_type", ["s3", "azure"])
def test_cancel_in_commit_window_on_object_storage_keeps_data(started_cluster, storage_type, partitioned):
    node = started_cluster.instances["node1"]
    failpoint = "delta_lake_write_cancel_in_commit_window"
    path = randomize_table_name(f"test_commit_window_{storage_type}")
    schema = pa.schema([("id", pa.int32(), False), ("part", pa.int32(), False)])
    create_empty_delta_table(started_cluster, storage_type, path, schema, partition_by=["part"] if partitioned else None)
    node.query(f"CREATE TABLE {path} (id Int32, part Int32) ENGINE = {delta_engine_definition(started_cluster, storage_type, path)}")
    node.query(f"SYSTEM ENABLE FAILPOINT {failpoint}")
    try:
        _, error = node.query_and_get_answer_with_error(f"INSERT INTO {path} SELECT number, number % 2 FROM numbers(6) SETTINGS max_block_size = 1")
        assert "QUERY_WAS_CANCELLED" in error, error
    finally:
        node.query(f"SYSTEM DISABLE FAILPOINT {failpoint}")

    assert node.query("SELECT 1").strip() == "1"
    # The commit landed before the cancel: the data must survive and stay referenced.
    assert log_versions(started_cluster, storage_type, path) == [0, 1]
    assert len(list_delta_data_files(started_cluster, storage_type, path)) == (2 if partitioned else 1)
    fresh = f"{path}_fresh"
    node.query(f"CREATE TABLE {fresh} (id Int32, part Int32) ENGINE = {delta_engine_definition(started_cluster, storage_type, path)}")
    assert node.query(f"SELECT count() FROM {fresh}").strip() == "6"
    node.query(f"DROP TABLE {fresh}")
    node.query(f"DROP TABLE {path}")


# ---------------------------------------------------------------------------------------------
# credentials never leak from the write path
# ---------------------------------------------------------------------------------------------


def test_failed_write_does_not_leak_credentials(started_cluster):
    node = started_cluster.instances["node1"]
    path = randomize_table_name("test_no_leak")
    schema = pa.schema([("id", pa.int32(), False)])
    create_empty_delta_table(started_cluster, "s3", path, schema)
    bad_secret = "WrongSecretKeyThatMustNeverAppearInLogs"
    url = f"http://{started_cluster.minio_ip}:{started_cluster.minio_port}/{started_cluster.minio_bucket}/{path}/"
    failed_id, ok_id = f"{path}_failed", f"{path}_ok"

    _, error = node.query_and_get_answer_with_error(
        f"INSERT INTO TABLE FUNCTION deltaLake('{url}', 'minio', '{bad_secret}') VALUES (1)",
        query_id=failed_id,
    )
    assert error != ""
    # The client echoes the query text after "(query:"; the server-side message must not contain it.
    assert bad_secret not in error.split("(query:")[0], error
    assert log_versions(started_cluster, "s3", path) == [0]
    assert list_delta_data_files(started_cluster, "s3", path) == []

    node.query(
        f"INSERT INTO TABLE FUNCTION {delta_table_function(started_cluster, 's3', path)} VALUES (2)",
        query_id=ok_id,
    )
    node.query("SYSTEM FLUSH LOGS")
    # Everything either query logged, correlated by query_id (compared in Python: a query text
    # containing the secrets would itself be a leak).
    logged = node.query(f"SELECT query, exception FROM system.query_log WHERE query_id IN ('{failed_id}', '{ok_id}') FORMAT TSVRaw")
    assert logged.count("INSERT") >= 2, logged
    assert bad_secret not in logged and minio_secret_key not in logged, logged
    assert "[HIDDEN]" in logged, logged
    text_log = node.query(f"SELECT message FROM system.text_log WHERE query_id IN ('{failed_id}', '{ok_id}') FORMAT TSVRaw")
    assert text_log != ""
    assert bad_secret not in text_log and minio_secret_key not in text_log
