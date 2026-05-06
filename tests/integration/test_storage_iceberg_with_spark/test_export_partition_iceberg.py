"""
Tests for EXPORT PARTITION to an Iceberg table that was created by Apache Spark.

The destination Iceberg metadata — including field IDs and the partition spec —
is written by Spark, not by ClickHouse, which removes any bias from tests where
both source and destination are ClickHouse-created.

A separate module-level fixture is used because the package-level
started_cluster_iceberg_with_spark does not include ZooKeeper (which is
required for ReplicatedMergeTree / EXPORT PARTITION).

Transform coverage (ClickHouse → Iceberg):
    identity             → identity
    toYearNumSinceEpoch  → year
    toMonthNumSinceEpoch → month
    toRelativeDayNum     → day
    toRelativeHourNum    → hour
    icebergBucket(N)     → bucket(N)
    icebergTruncate(N)   → truncate(N)
    compound             → multiple fields
"""

import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import pytest
import pyspark

from helpers.cluster import ClickHouseCluster
from helpers.export_partition_helpers import (
    first_partition_id,
    make_iceberg_s3,
    make_rmt,
    unique_suffix,
    wait_for_export_status,
)
from helpers.iceberg_utils import (
    create_iceberg_table,
    default_upload_directory,
)
from helpers.s3_tools import S3Uploader, prepare_s3_bucket


# ---------------------------------------------------------------------------
# Spark session
# ---------------------------------------------------------------------------

def get_spark():
    builder = (
        pyspark.sql.SparkSession.builder
        .appName("test_export_partition_spark_iceberg")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.iceberg.spark.SparkSessionCatalog",
        )
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.spark_catalog.type", "hadoop")
        .config(
            "spark.sql.catalog.spark_catalog.warehouse",
            "/var/lib/clickhouse/user_files/iceberg_data",
        )
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .master("local")
    )
    return builder.getOrCreate()


# ---------------------------------------------------------------------------
# Cluster fixture
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def export_cluster():
    try:
        cluster = ClickHouseCluster(__file__, with_spark=True)
        cluster.add_instance(
            "node1",
            main_configs=[
                "configs/config.d/named_collections.xml",
                "configs/config.d/allow_export_partition.xml",
            ],
            user_configs=[
                "configs/users.d/allow_export_partition.xml",
            ],
            with_minio=True,
            stay_alive=True,
            with_zookeeper=True,
            keeper_required_feature_flags=["multi_read"],
        )
        for name in ["replica1", "replica2", "replica3"]:
            cluster.add_instance(
                name,
                main_configs=[
                    "configs/config.d/named_collections.xml",
                    "configs/config.d/allow_export_partition.xml",
                ],
                user_configs=[
                    "configs/users.d/allow_export_partition.xml",
                ],
                stay_alive=True,
                with_zookeeper=True,
                keeper_required_feature_flags=["multi_read"],
            )
        logging.info("Starting export_cluster...")
        cluster.start()
        prepare_s3_bucket(cluster)
        cluster.spark_session = get_spark()
        cluster.default_s3_uploader = S3Uploader(cluster.minio_client, cluster.minio_bucket)
        yield cluster
    finally:
        cluster.shutdown()


@pytest.fixture(autouse=True)
def drop_tables(export_cluster):
    yield
    for node_name in ["node1", "replica1", "replica2", "replica3"]:
        node = export_cluster.instances[node_name]
        try:
            tables = node.query(
                "SELECT name FROM system.tables WHERE database = 'default' FORMAT TabSeparated"
            ).strip()
            for table in tables.splitlines():
                table = table.strip()
                if table:
                    node.query(f"DROP TABLE IF EXISTS default.`{table}` SYNC")
        except Exception as e:
            logging.warning(f"drop_tables cleanup failed on {node_name}: {e}")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def spark_iceberg(cluster, spark, iceberg_name: str, ddl: str):
    """Execute a Spark DDL and upload the resulting Iceberg files to MinIO."""
    spark.sql(ddl)
    default_upload_directory(
        cluster,
        "s3",
        f"/iceberg_data/default/{iceberg_name}/",
        f"/iceberg_data/default/{iceberg_name}/",
    )


def attach_ch_iceberg(node, iceberg_name: str, schema: str, cluster):
    """
    Attach a ClickHouse IcebergS3 table to an existing Spark-written Iceberg path.
    No PARTITION BY is specified — the spec is read from Spark's metadata.
    """
    create_iceberg_table(
        "s3",
        node,
        iceberg_name,
        cluster,
        schema=f"({schema})",
        if_not_exists=True,
    )



def run_accepted(export_cluster, label, spark_ddl, ch_schema, rmt_columns, rmt_partition_by, insert_values):
    """
    Create a Spark-created Iceberg table, attach ClickHouse to it, create the
    source RMT, export, wait, and return (node, source, iceberg, partition_id)
    so the caller can do additional assertions.
    """
    node = export_cluster.instances["node1"]
    spark = export_cluster.spark_session

    uid = unique_suffix()
    source = f"rmt_{label}_{uid}"
    iceberg = f"spark_{label}_{uid}"

    spark_iceberg(export_cluster, spark, iceberg, spark_ddl.format(TABLE=iceberg))
    attach_ch_iceberg(node, iceberg, ch_schema, export_cluster)
    make_rmt(node, source, rmt_columns, rmt_partition_by, order_by="id")
    node.query(f"INSERT INTO {source} VALUES {insert_values}")

    pid = first_partition_id(node, source)
    node.query(f"ALTER TABLE {source} EXPORT PARTITION ID '{pid}' TO TABLE {iceberg}")
    wait_for_export_status(node, source, iceberg, pid)

    return node, source, iceberg, pid


def run_rejected(export_cluster, label, spark_ddl, ch_schema, rmt_columns, rmt_partition_by, insert_values):
    """
    Create a mismatched pair and assert that EXPORT PARTITION fails with BAD_ARGUMENTS.
    The check fires synchronously before any task is enqueued.
    """
    node = export_cluster.instances["node1"]
    spark = export_cluster.spark_session

    uid = unique_suffix()
    source = f"rmt_{label}_{uid}"
    iceberg = f"spark_{label}_{uid}"

    spark_iceberg(export_cluster, spark, iceberg, spark_ddl.format(TABLE=iceberg))
    attach_ch_iceberg(node, iceberg, ch_schema, export_cluster)
    make_rmt(node, source, rmt_columns, rmt_partition_by, order_by="id")
    node.query(f"INSERT INTO {source} VALUES {insert_values}")

    pid = first_partition_id(node, source)
    error = node.query_and_get_error(
        f"ALTER TABLE {source} EXPORT PARTITION ID '{pid}' TO TABLE {iceberg}"
    )
    return error


# ---------------------------------------------------------------------------
# Replicated helpers
# ---------------------------------------------------------------------------


def create_iceberg_s3_table(node, iceberg_table: str, if_not_exists: bool = False):
    """Create (or attach to an existing) IcebergS3 table at a per-test MinIO prefix."""
    make_iceberg_s3(
        node, iceberg_table, "id Int64, year Int32",
        partition_by="year", if_not_exists=if_not_exists,
    )


def setup_replicas(cluster, mt_table: str, iceberg_table: str, replica_names: list):
    """
    Create RMT on each replica with a per-replica replica_name so all instances share
    the same ZooKeeper path. Create IcebergS3 on the primary; attach with IF NOT EXISTS
    on the rest. No data is inserted here — callers manage their own test data.
    """
    instances = [cluster.instances[n] for n in replica_names]
    primary = instances[0]

    for rname, instance in zip(replica_names, instances):
        make_rmt(instance, mt_table, "id Int64, year Int32", "year", replica_name=rname)

    create_iceberg_s3_table(primary, iceberg_table)
    for instance in instances[1:]:
        create_iceberg_s3_table(instance, iceberg_table, if_not_exists=True)



# ---------------------------------------------------------------------------
# Happy-path tests — one per transform
# ---------------------------------------------------------------------------

def test_identity_transform(export_cluster):
    """Spark identity(year)  <->  PARTITION BY year."""
    node, _, iceberg, _ = run_accepted(
        export_cluster,
        "identity",
        spark_ddl="CREATE TABLE {TABLE} (id BIGINT, year INT)"
                  " USING iceberg PARTITIONED BY (identity(year)) OPTIONS('format-version'='2')",
        ch_schema="id Int64, year Int32",
        rmt_columns="id Int64, year Int32",
        rmt_partition_by="year",
        insert_values="(1, 2024), (2, 2024), (3, 2024)",
    )
    assert int(node.query(f"SELECT count() FROM {iceberg}").strip()) == 3


def test_year_transform(export_cluster):
    """Spark years(dt)  <->  PARTITION BY toYearNumSinceEpoch(dt)."""
    node, _, iceberg, _ = run_accepted(
        export_cluster,
        "year",
        spark_ddl="CREATE TABLE {TABLE} (id BIGINT, dt DATE)"
                  " USING iceberg PARTITIONED BY (years(dt)) OPTIONS('format-version'='2')",
        ch_schema="id Int64, dt Date",
        rmt_columns="id Int64, dt Date",
        rmt_partition_by="toYearNumSinceEpoch(dt)",
        insert_values="(1, '2021-03-01'), (2, '2021-07-15'), (3, '2021-12-31')",
    )
    assert int(node.query(f"SELECT count() FROM {iceberg}").strip()) == 3


def test_month_transform(export_cluster):
    """Spark months(dt)  <->  PARTITION BY toMonthNumSinceEpoch(dt)."""
    node, _, iceberg, _ = run_accepted(
        export_cluster,
        "month",
        spark_ddl="CREATE TABLE {TABLE} (id BIGINT, dt DATE)"
                  " USING iceberg PARTITIONED BY (months(dt)) OPTIONS('format-version'='2')",
        ch_schema="id Int64, dt Date",
        rmt_columns="id Int64, dt Date",
        rmt_partition_by="toMonthNumSinceEpoch(dt)",
        insert_values="(1, '2020-06-01'), (2, '2020-06-15'), (3, '2020-06-30')",
    )
    assert int(node.query(f"SELECT count() FROM {iceberg}").strip()) == 3


def test_day_transform(export_cluster):
    """Spark days(dt)  <->  PARTITION BY toRelativeDayNum(dt)."""
    node, _, iceberg, _ = run_accepted(
        export_cluster,
        "day",
        spark_ddl="CREATE TABLE {TABLE} (id BIGINT, dt DATE)"
                  " USING iceberg PARTITIONED BY (days(dt)) OPTIONS('format-version'='2')",
        ch_schema="id Int64, dt Date",
        rmt_columns="id Int64, dt Date",
        rmt_partition_by="toRelativeDayNum(dt)",
        insert_values="(1, '2023-03-15'), (2, '2023-03-15'), (3, '2023-03-15')",
    )
    assert int(node.query(f"SELECT count() FROM {iceberg}").strip()) == 3


def test_hour_transform(export_cluster):
    """Spark hours(ts)  <->  PARTITION BY toRelativeHourNum(ts).

    Spark TIMESTAMP maps to Iceberg 'timestamp' which ClickHouse reads as DateTime64(6).
    All three rows fall within the same hour so a single partition is exported.
    """
    node, _, iceberg, _ = run_accepted(
        export_cluster,
        "hour",
        spark_ddl="CREATE TABLE {TABLE} (id BIGINT, ts TIMESTAMP)"
                  " USING iceberg PARTITIONED BY (hours(ts)) OPTIONS('format-version'='2')",
        ch_schema="id Int64, ts DateTime64(6)",
        rmt_columns="id Int64, ts DateTime64(6)",
        rmt_partition_by="toRelativeHourNum(ts)",
        insert_values=(
            "(1, '2023-03-15 10:00:00'), "
            "(2, '2023-03-15 10:30:00'), "
            "(3, '2023-03-15 10:59:00')"
        ),
    )
    assert int(node.query(f"SELECT count() FROM {iceberg}").strip()) == 3


def test_bucket_transform(export_cluster):
    """Spark bucket(8, user_id)  <->  PARTITION BY icebergBucket(8, user_id)."""
    node, _, iceberg, _ = run_accepted(
        export_cluster,
        "bucket",
        spark_ddl="CREATE TABLE {TABLE} (id BIGINT, user_id BIGINT)"
                  " USING iceberg PARTITIONED BY (bucket(8, user_id)) OPTIONS('format-version'='2')",
        ch_schema="id Int64, user_id Int64",
        rmt_columns="id Int64, user_id Int64",
        rmt_partition_by="icebergBucket(8, user_id)",
        # All rows share the same user_id → same bucket → single partition.
        insert_values="(1, 42), (2, 42), (3, 42)",
    )
    assert int(node.query(f"SELECT count() FROM {iceberg}").strip()) == 3


def test_truncate_transform(export_cluster):
    """Spark truncate(4, category)  <->  PARTITION BY icebergTruncate(4, category)."""
    node, _, iceberg, _ = run_accepted(
        export_cluster,
        "truncate",
        spark_ddl="CREATE TABLE {TABLE} (id BIGINT, category STRING)"
                  " USING iceberg PARTITIONED BY (truncate(4, category)) OPTIONS('format-version'='2')",
        ch_schema="id Int64, category String",
        rmt_columns="id Int64, category String",
        rmt_partition_by="icebergTruncate(4, category)",
        # All share the 4-char prefix 'clic' → same truncate bucket.
        insert_values="(1, 'clickhouse'), (2, 'click'), (3, 'clickstream')",
    )
    assert int(node.query(f"SELECT count() FROM {iceberg}").strip()) == 3


def test_compound_transform(export_cluster):
    """Spark (identity(year), identity(region))  <->  PARTITION BY (year, region)."""
    node, _, iceberg, _ = run_accepted(
        export_cluster,
        "compound",
        spark_ddl="CREATE TABLE {TABLE} (id BIGINT, year INT, region STRING)"
                  " USING iceberg PARTITIONED BY (identity(year), identity(region))"
                  " OPTIONS('format-version'='2')",
        ch_schema="id Int64, year Int32, region String",
        rmt_columns="id Int64, year Int32, region String",
        rmt_partition_by="(year, region)",
        insert_values="(1, 2022, 'EU'), (2, 2022, 'EU'), (3, 2022, 'EU')",
    )
    assert int(node.query(f"SELECT count() FROM {iceberg}").strip()) == 3


def test_identity_int64(export_cluster):
    """Spark identity(user_id) on BIGINT  <->  PARTITION BY user_id (Int64).

    Int64 → Avro 'long' is already handled by getAvroType(). This test covers
    the identity transform on a 64-bit integer column, which is not covered by
    the existing test_identity_transform (which uses Int32).
    """
    node, _, iceberg, _ = run_accepted(
        export_cluster,
        "identity_int64",
        spark_ddl="CREATE TABLE {TABLE} (id BIGINT, user_id BIGINT)"
                  " USING iceberg PARTITIONED BY (identity(user_id))"
                  " OPTIONS('format-version'='2')",
        ch_schema="id Int64, user_id Int64",
        rmt_columns="id Int64, user_id Int64",
        rmt_partition_by="user_id",
        insert_values="(1, 100), (2, 100), (3, 100)",
    )
    assert int(node.query(f"SELECT count() FROM {iceberg}").strip()) == 3


def test_identity_date(export_cluster):
    """Spark identity(event_date) on DATE  <->  PARTITION BY event_date (Date32).

    Date32 → Avro 'int' is already handled by getAvroType(). This test covers
    the identity transform directly on a date column. Existing date-related tests
    (test_year_transform, test_month_transform, etc.) use time-based transforms
    such as years() and months(), not identity().
    """
    node, _, iceberg, _ = run_accepted(
        export_cluster,
        "identity_date",
        spark_ddl="CREATE TABLE {TABLE} (id BIGINT, event_date DATE)"
                  " USING iceberg PARTITIONED BY (identity(event_date))"
                  " OPTIONS('format-version'='2')",
        ch_schema="id Int64, event_date Date32",
        rmt_columns="id Int64, event_date Date32",
        rmt_partition_by="event_date",
        insert_values="(1, '2024-03-15'), (2, '2024-03-15'), (3, '2024-03-15')",
    )
    assert int(node.query(f"SELECT count() FROM {iceberg}").strip()) == 3


def test_identity_string(export_cluster):
    """Spark identity(region) on STRING  <->  PARTITION BY region (String).

    String → Avro 'string' is already handled by getAvroType(). This test covers
    identity on a string column as the sole partition field. The existing
    test_compound_transform uses identity(region) only as part of a multi-field spec,
    so a standalone string identity partition was not previously exercised end-to-end.
    """
    node, _, iceberg, _ = run_accepted(
        export_cluster,
        "identity_str",
        spark_ddl="CREATE TABLE {TABLE} (id BIGINT, region STRING)"
                  " USING iceberg PARTITIONED BY (identity(region))"
                  " OPTIONS('format-version'='2')",
        ch_schema="id Int64, region String",
        rmt_columns="id Int64, region String",
        rmt_partition_by="region",
        insert_values="(1, 'EU'), (2, 'EU'), (3, 'EU')",
    )
    assert int(node.query(f"SELECT count() FROM {iceberg}").strip()) == 3


def test_truncate_int64(export_cluster):
    """Spark truncate(10, amount) on BIGINT  <->  PARTITION BY icebergTruncate(10, amount) (Int64).

    Int64 truncate produces floor(v / 10) * 10, so all rows with amount=42 land in
    partition value 40 (same partition). This is a distinct code path from truncate on
    String (which trims a character prefix). The existing test_truncate_transform uses
    String only, leaving the integer truncate path untested.
    """
    node, _, iceberg, _ = run_accepted(
        export_cluster,
        "truncate_int64",
        spark_ddl="CREATE TABLE {TABLE} (id BIGINT, amount BIGINT)"
                  " USING iceberg PARTITIONED BY (truncate(10, amount))"
                  " OPTIONS('format-version'='2')",
        ch_schema="id Int64, amount Int64",
        rmt_columns="id Int64, amount Int64",
        rmt_partition_by="icebergTruncate(10, amount)",
        # All rows have amount=42 → truncated partition value is 40.
        insert_values="(1, 42), (2, 42), (3, 42)",
    )
    assert int(node.query(f"SELECT count() FROM {iceberg}").strip()) == 3


def test_bucket_string(export_cluster):
    """Spark bucket(8, name) on STRING  <->  PARTITION BY icebergBucket(8, name) (String).

    Bucket on strings uses Murmur3 hash of the UTF-8 bytes, a different hash path than
    bucket on integers. All rows share the same name so they land in the same bucket.
    The existing test_bucket_transform uses BIGINT only, leaving string bucketing untested.
    """
    node, _, iceberg, _ = run_accepted(
        export_cluster,
        "bucket_str",
        spark_ddl="CREATE TABLE {TABLE} (id BIGINT, name STRING)"
                  " USING iceberg PARTITIONED BY (bucket(8, name))"
                  " OPTIONS('format-version'='2')",
        ch_schema="id Int64, name String",
        rmt_columns="id Int64, name String",
        rmt_partition_by="icebergBucket(8, name)",
        # All rows share the same name → same Murmur3 bucket.
        insert_values="(1, 'alice'), (2, 'alice'), (3, 'alice')",
    )
    assert int(node.query(f"SELECT count() FROM {iceberg}").strip()) == 3


def test_year_transform_timestamp(export_cluster):
    """Spark years(ts) on TIMESTAMP  <->  PARTITION BY toYearNumSinceEpoch(ts) (DateTime64(6)).

    DateTime64 → Avro 'long' is already handled by getAvroType(). The year transform on
    TIMESTAMP follows a different branch than on DATE (long vs int in Avro). The existing
    test_year_transform uses DATE only. All three rows fall within the same year.
    """
    node, _, iceberg, _ = run_accepted(
        export_cluster,
        "year_ts",
        spark_ddl="CREATE TABLE {TABLE} (id BIGINT, ts TIMESTAMP)"
                  " USING iceberg PARTITIONED BY (years(ts))"
                  " OPTIONS('format-version'='2')",
        ch_schema="id Int64, ts DateTime64(6)",
        rmt_columns="id Int64, ts DateTime64(6)",
        rmt_partition_by="toYearNumSinceEpoch(ts)",
        insert_values=(
            "(1, '2023-01-15 08:00:00'), "
            "(2, '2023-06-01 12:00:00'), "
            "(3, '2023-12-31 23:59:59')"
        ),
    )
    assert int(node.query(f"SELECT count() FROM {iceberg}").strip()) == 3


def test_month_transform_timestamp(export_cluster):
    """Spark months(ts) on TIMESTAMP  <->  PARTITION BY toMonthNumSinceEpoch(ts) (DateTime64(6)).

    Analogous to test_year_transform_timestamp but for the month transform.
    All three rows fall within the same calendar month.
    """
    node, _, iceberg, _ = run_accepted(
        export_cluster,
        "month_ts",
        spark_ddl="CREATE TABLE {TABLE} (id BIGINT, ts TIMESTAMP)"
                  " USING iceberg PARTITIONED BY (months(ts))"
                  " OPTIONS('format-version'='2')",
        ch_schema="id Int64, ts DateTime64(6)",
        rmt_columns="id Int64, ts DateTime64(6)",
        rmt_partition_by="toMonthNumSinceEpoch(ts)",
        insert_values=(
            "(1, '2023-06-01 00:00:00'), "
            "(2, '2023-06-15 12:00:00'), "
            "(3, '2023-06-30 23:59:59')"
        ),
    )
    assert int(node.query(f"SELECT count() FROM {iceberg}").strip()) == 3


def test_day_transform_timestamp(export_cluster):
    """Spark days(ts) on TIMESTAMP  <->  PARTITION BY toRelativeDayNum(ts) (DateTime64(6)).

    Analogous to test_year_transform_timestamp but for the day transform.
    All three rows fall within the same calendar day.
    """
    node, _, iceberg, _ = run_accepted(
        export_cluster,
        "day_ts",
        spark_ddl="CREATE TABLE {TABLE} (id BIGINT, ts TIMESTAMP)"
                  " USING iceberg PARTITIONED BY (days(ts))"
                  " OPTIONS('format-version'='2')",
        ch_schema="id Int64, ts DateTime64(6)",
        rmt_columns="id Int64, ts DateTime64(6)",
        rmt_partition_by="toRelativeDayNum(ts)",
        insert_values=(
            "(1, '2023-06-15 00:00:00'), "
            "(2, '2023-06-15 12:00:00'), "
            "(3, '2023-06-15 23:59:59')"
        ),
    )
    assert int(node.query(f"SELECT count() FROM {iceberg}").strip()) == 3


# ---------------------------------------------------------------------------
# Unhappy-path tests — BAD_ARGUMENTS must be raised synchronously
# ---------------------------------------------------------------------------

def test_rejected_column_mismatch(export_cluster):
    """Spark identity(year) — RMT PARTITION BY id: different column."""
    error = run_rejected(
        export_cluster,
        "rej_col_mismatch",
        spark_ddl="CREATE TABLE {TABLE} (id BIGINT, year INT)"
                  " USING iceberg PARTITIONED BY (identity(year)) OPTIONS('format-version'='2')",
        ch_schema="id Int64, year Int32",
        rmt_columns="id Int64, year Int32",
        rmt_partition_by="id",
        insert_values="(1, 2024)",
    )
    assert "BAD_ARGUMENTS" in error, f"Expected BAD_ARGUMENTS, got: {error!r}"


def test_rejected_transform_mismatch(export_cluster):
    """Spark years(dt) — RMT PARTITION BY dt (identity, not year-transform)."""
    error = run_rejected(
        export_cluster,
        "rej_xform_mismatch",
        spark_ddl="CREATE TABLE {TABLE} (id BIGINT, dt DATE)"
                  " USING iceberg PARTITIONED BY (years(dt)) OPTIONS('format-version'='2')",
        ch_schema="id Int64, dt Date",
        rmt_columns="id Int64, dt Date",
        rmt_partition_by="dt",
        insert_values="(1, '2021-06-01')",
    )
    assert "BAD_ARGUMENTS" in error, f"Expected BAD_ARGUMENTS, got: {error!r}"


def test_rejected_bucket_count_mismatch(export_cluster):
    """Spark bucket(8, user_id) — RMT icebergBucket(16, user_id): wrong N."""
    error = run_rejected(
        export_cluster,
        "rej_bucket_n",
        spark_ddl="CREATE TABLE {TABLE} (id BIGINT, user_id BIGINT)"
                  " USING iceberg PARTITIONED BY (bucket(8, user_id)) OPTIONS('format-version'='2')",
        ch_schema="id Int64, user_id Int64",
        rmt_columns="id Int64, user_id Int64",
        rmt_partition_by="icebergBucket(16, user_id)",
        insert_values="(1, 42)",
    )
    assert "BAD_ARGUMENTS" in error, f"Expected BAD_ARGUMENTS, got: {error!r}"


def test_rejected_truncate_width_mismatch(export_cluster):
    """Spark truncate(4, category) — RMT icebergTruncate(8, category): wrong width."""
    error = run_rejected(
        export_cluster,
        "rej_trunc_w",
        spark_ddl="CREATE TABLE {TABLE} (id BIGINT, category STRING)"
                  " USING iceberg PARTITIONED BY (truncate(4, category)) OPTIONS('format-version'='2')",
        ch_schema="id Int64, category String",
        rmt_columns="id Int64, category String",
        rmt_partition_by="icebergTruncate(8, category)",
        insert_values="(1, 'clickhouse')",
    )
    assert "BAD_ARGUMENTS" in error, f"Expected BAD_ARGUMENTS, got: {error!r}"


def test_rejected_field_count_mismatch(export_cluster):
    """Spark 1-field identity(year) — RMT 2-field (year, region)."""
    error = run_rejected(
        export_cluster,
        "rej_field_n",
        spark_ddl="CREATE TABLE {TABLE} (id BIGINT, year INT, region STRING)"
                  " USING iceberg PARTITIONED BY (identity(year)) OPTIONS('format-version'='2')",
        ch_schema="id Int64, year Int32, region String",
        rmt_columns="id Int64, year Int32, region String",
        rmt_partition_by="(year, region)",
        insert_values="(1, 2024, 'EU')",
    )
    assert "BAD_ARGUMENTS" in error, f"Expected BAD_ARGUMENTS, got: {error!r}"


def test_rejected_compound_order_reversed(export_cluster):
    """Spark (identity(year), identity(region)) — RMT (region, year): reversed order."""
    error = run_rejected(
        export_cluster,
        "rej_compound_rev",
        spark_ddl="CREATE TABLE {TABLE} (id BIGINT, year INT, region STRING)"
                  " USING iceberg PARTITIONED BY (identity(year), identity(region))"
                  " OPTIONS('format-version'='2')",
        ch_schema="id Int64, year Int32, region String",
        rmt_columns="id Int64, year Int32, region String",
        rmt_partition_by="(region, year)",
        insert_values="(1, 2024, 'EU')",
    )
    assert "BAD_ARGUMENTS" in error, f"Expected BAD_ARGUMENTS, got: {error!r}"


def test_idempotency_after_commit_crash(export_cluster):
    """
    Verify that an Iceberg export commit is idempotent when ClickHouse crashes (via
    std::terminate() in a failpoint) after the Iceberg metadata is written but before
    ZooKeeper is updated to COMPLETED.
    Expected behaviour:
    - The failpoint fires once: std::terminate() kills the process immediately after the
    Iceberg commit; ZK task remains PENDING.
    - ClickHouse is restarted.  The scheduler picks up the PENDING task and retries the
    commit.  commitExportPartitionTransaction finds the transaction_id already present in
    the Iceberg snapshot summary and skips re-committing.
    - The task eventually reaches COMPLETED.
    - The row count in the Iceberg table is exactly the number inserted (no duplicates).
    """
    node = export_cluster.instances["node1"]
    spark = export_cluster.spark_session
    uid = unique_suffix()
    source = f"rmt_{uid}"
    iceberg = f"spark_{uid}"
    spark_iceberg(
        export_cluster,
        spark,
        iceberg,
        f"CREATE TABLE {iceberg} (id BIGINT, year INT)"
        f" USING iceberg PARTITIONED BY (identity(year)) OPTIONS('format-version'='2')",
    )
    attach_ch_iceberg(node, iceberg, "id Int64, year Int32", export_cluster)
    make_rmt(node, source, "id Int64, year Int32", "year")
    node.query(f"INSERT INTO {source} VALUES (1, 2024), (2, 2024), (3, 2024)")
    pid = first_partition_id(node, source)
    # Enable the ONCE failpoint. When the background scheduler thread reaches the
    # injection point (after a successful Iceberg commit), std::terminate() is called
    # and the process exits immediately without setting ZK COMPLETED.
    node.query("SYSTEM ENABLE FAILPOINT iceberg_export_after_commit_before_zk_completed")
    node.query(f"ALTER TABLE {source} EXPORT PARTITION ID '{pid}' TO TABLE {iceberg}")
    # the fail point will sleep for 10 seconds. Wait for 5 and then re-start clickhouse.
    time.sleep(5)
    # Restart ClickHouse. The ZK task is still PENDING; the scheduler will pick it up.
    node.restart_clickhouse()
    time.sleep(5)
    # On restart the scheduler retries the commit. commitExportPartitionTransaction
    # detects the transaction_id in the existing Iceberg snapshot summary and returns
    # without re-writing any data, then sets ZK COMPLETED.
    wait_for_export_status(node, source, iceberg, pid, timeout=60)
    # Exactly 3 rows — no duplicates from the idempotent re-commit.
    count = int(node.query(f"SELECT count() FROM {iceberg}").strip())
    assert count == 3, f"Expected 3 rows (no duplicates), got {count}"


def test_commit_attempts_budget_transitions_to_failed(export_cluster):
    """
    Verify that the commit-attempts budget transitions a stuck task to FAILED
    instead of leaving it in PENDING forever.

    Reproduction:
    - Parts export successfully.
    - A REGULAR failpoint (``export_partition_commit_always_throw``) makes every
      ``ExportPartitionUtils::commit`` attempt throw before talking to Iceberg.
    - ``ExportPartitionUtils::handleCommitFailure`` bumps ``<entry>/commit_attempts``
      on each failure and transitions ``/status`` to FAILED once the counter
      reaches ``export_merge_tree_partition_max_retries``.

    Expected behaviour:
    - The first attempt is made synchronously when the last part completes
      (scheduler's ``handlePartExportSuccess``).
    - Subsequent attempts come from the manifest-updating task's ``tryCleanup``
      path, polling every 30s.
    - With max_retries=2, the task reaches FAILED within roughly one poll cycle.
    - The ``commit_attempts`` znode reaches at least max_retries.
    """
    node = export_cluster.instances["node1"]
    spark = export_cluster.spark_session

    uid = unique_suffix()
    source = f"rmt_commit_budget_{uid}"
    iceberg = f"spark_commit_budget_{uid}"

    spark_iceberg(
        export_cluster,
        spark,
        iceberg,
        f"CREATE TABLE {iceberg} (id BIGINT, year INT)"
        f" USING iceberg PARTITIONED BY (identity(year)) OPTIONS('format-version'='2')",
    )
    attach_ch_iceberg(node, iceberg, "id Int64, year Int32", export_cluster)
    make_rmt(node, source, "id Int64, year Int32", "year")
    node.query(f"INSERT INTO {source} VALUES (1, 2024), (2, 2024), (3, 2024)")
    pid = first_partition_id(node, source)

    # Force every commit attempt to throw. REGULAR failpoint fires on every hit,
    # unlike ONCE which would only fire for the first call.
    node.query("SYSTEM ENABLE FAILPOINT export_partition_commit_always_throw")

    # try block exists so we can add a finally that disables the failpoint
    try:
        # max_retries=2 bounds the test: one attempt from handlePartExportSuccess
        # plus one from the manifest-updating task's next poll (~30s) is enough
        # to exhaust the budget and flip the task to FAILED.
        node.query(
            f"ALTER TABLE {source} EXPORT PARTITION ID '{pid}' TO TABLE {iceberg}"
            f" SETTINGS export_merge_tree_partition_max_retries = 2"
        )

        # Timeout must cover: at least one manifest-updating poll cycle (30s)
        # plus slack for task scheduling and keeper RTT.
        wait_for_export_status(
            node, source, iceberg, pid,
            expected_status="FAILED",
            timeout=90,
        )

        # The commit_attempts znode must have reached (at least) max_retries — the
        # counter is the direct mechanism that drove the FAILED transition.
        # Locate the export's ZK root via the RMT's zookeeper_path and the
        # partition_id_destination_db.destination_table export key convention.
        export_key = f"{pid}_default.{iceberg}"
        commit_attempts = int(node.query(
            f"SELECT value FROM system.zookeeper"
            f" WHERE path = '/clickhouse/tables/{source}/exports/{export_key}'"
            f"   AND name = 'commit_attempts'"
        ).strip())
        assert commit_attempts >= 2, (
            f"Expected commit_attempts >= 2 (two commit attempts), got {commit_attempts}"
        )
    finally:
        node.query("SYSTEM DISABLE FAILPOINT export_partition_commit_always_throw")


# ---------------------------------------------------------------------------
# Replicated tests — IcebergS3, no catalog
# ---------------------------------------------------------------------------


def test_export_initiated_from_replica2(export_cluster):
    """
    Export is initiated from replica2 (not the inserting replica).
    Validates that any replica can start the export, not just the writer.
    """
    uid = unique_suffix()
    mt_table = f"rmt_from_replica2_{uid}"
    iceberg_table = f"iceberg_from_replica2_{uid}"

    setup_replicas(export_cluster, mt_table, iceberg_table, ["replica1", "replica2"])

    r1 = export_cluster.instances["replica1"]
    r2 = export_cluster.instances["replica2"]

    r1.query(f"INSERT INTO {mt_table} VALUES (1, 2020), (2, 2020), (3, 2020)")
    r2.query(f"SYSTEM SYNC REPLICA {mt_table}")

    r2.query(f"ALTER TABLE {mt_table} EXPORT PARTITION ID '2020' TO TABLE {iceberg_table}")
    wait_for_export_status(r2, mt_table, iceberg_table, "2020")

    count_r1 = int(r1.query(f"SELECT count() FROM {iceberg_table}").strip())
    assert count_r1 == 3, f"Expected 3 rows from replica1, got {count_r1}"
    count_r2 = int(r2.query(f"SELECT count() FROM {iceberg_table}").strip())
    assert count_r2 == 3, f"Expected 3 rows from replica2, got {count_r2}"


def test_concurrent_exports_different_partitions_across_replicas(export_cluster):
    """
    Three replicas concurrently export distinct partitions (2020, 2021, 2022) to the
    same IcebergS3 table. All three commits must succeed and the total row count must
    equal the sum of all inserted rows.
    """
    uid = unique_suffix()
    mt_table = f"rmt_concurrent_diff_parts_{uid}"
    iceberg_table = f"iceberg_concurrent_diff_parts_{uid}"

    setup_replicas(
        export_cluster, mt_table, iceberg_table,
        ["replica1", "replica2", "replica3"],
    )

    r1 = export_cluster.instances["replica1"]
    r2 = export_cluster.instances["replica2"]
    r3 = export_cluster.instances["replica3"]

    r1.query(f"INSERT INTO {mt_table} VALUES (1, 2020), (2, 2020), (3, 2020)")
    r1.query(f"INSERT INTO {mt_table} VALUES (4, 2021), (5, 2021), (6, 2021)")
    r1.query(f"INSERT INTO {mt_table} VALUES (7, 2022), (8, 2022), (9, 2022)")
    r2.query(f"SYSTEM SYNC REPLICA {mt_table}")
    r3.query(f"SYSTEM SYNC REPLICA {mt_table}")

    errors: list = []

    def export_from(node, pid):
        try:
            node.query(
                f"ALTER TABLE {mt_table} EXPORT PARTITION ID '{pid}' TO TABLE {iceberg_table}"
            )
            wait_for_export_status(node, mt_table, iceberg_table, pid)
        except Exception as exc:
            errors.append(exc)

    threads = [
        threading.Thread(target=export_from, args=(r1, "2020")),
        threading.Thread(target=export_from, args=(r2, "2021")),
        threading.Thread(target=export_from, args=(r3, "2022")),
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert not errors, f"Export threads raised errors: {errors}"

    count = int(r1.query(f"SELECT count() FROM {iceberg_table}").strip())
    assert count == 9, f"Expected 9 rows total (3 per partition), got {count}"


def test_three_replica_concurrent_exports(export_cluster):
    """
    ThreadPoolExecutor with 3 workers: each replica exports its own distinct partition.
    All futures must complete successfully; total row count must be correct.
    """
    uid = unique_suffix()
    mt_table = f"rmt_three_replicas_concurrent_{uid}"
    iceberg_table = f"iceberg_three_replicas_concurrent_{uid}"

    setup_replicas(
        export_cluster, mt_table, iceberg_table,
        ["replica1", "replica2", "replica3"],
    )

    r1 = export_cluster.instances["replica1"]
    r2 = export_cluster.instances["replica2"]
    r3 = export_cluster.instances["replica3"]

    r1.query(f"INSERT INTO {mt_table} VALUES (1, 2020), (2, 2020), (3, 2020)")
    r1.query(f"INSERT INTO {mt_table} VALUES (4, 2021), (5, 2021), (6, 2021)")
    r1.query(f"INSERT INTO {mt_table} VALUES (7, 2022), (8, 2022), (9, 2022)")
    r2.query(f"SYSTEM SYNC REPLICA {mt_table}")
    r3.query(f"SYSTEM SYNC REPLICA {mt_table}")

    def export_fn(node_pid):
        node, pid = node_pid
        node.query(
            f"ALTER TABLE {mt_table} EXPORT PARTITION ID '{pid}' TO TABLE {iceberg_table}"
        )
        wait_for_export_status(node, mt_table, iceberg_table, pid)

    with ThreadPoolExecutor(max_workers=3) as executor:
        futures = [
            executor.submit(export_fn, (r1, "2020")),
            executor.submit(export_fn, (r2, "2021")),
            executor.submit(export_fn, (r3, "2022")),
        ]
    for fut in futures:
        fut.result()

    count = int(r1.query(f"SELECT count() FROM {iceberg_table}").strip())
    assert count == 9, f"Expected 9 rows total (3 per partition), got {count}"
