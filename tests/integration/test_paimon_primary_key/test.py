"""A full scan of a Paimon primary-key table reads the buckets Paimon itself reads without merging, with the same result
as Spark, and throws for every other bucket, because merge-on-read is not implemented. Paimon reads a bucket without
merging when all its files are above level 0, have no retract rows and are in one level; with deletion vectors or the
first-row merge engine the files may be in several levels, and level-0 files are not read at all.

The tables are written at test time by the Paimon Spark bundle baked into the runner image, as in
test_paimon_deletion_vectors. The file layout is asserted before every read.
"""

import os
import re
import shutil
import uuid

import pyspark
import pytest

# Imported for its fork-safe gateway launch.
from helpers import spark_tools  # noqa: F401
from helpers.cluster import ClickHouseCluster
from helpers.s3_tools import LocalUploader

_WORKER = os.environ.get("PYTEST_XDIST_WORKER", "master")
_RUN_ID = re.sub(r"[^A-Za-z0-9_]", "_", os.environ.get("INTEGRATION_TESTS_RUN_ID", ""))
WAREHOUSE = f"/var/lib/clickhouse/user_files/paimon_primary_key_{_WORKER}" + (
    f"_{_RUN_ID}" if _RUN_ID else ""
)


def get_spark():
    return (
        pyspark.sql.SparkSession.builder.appName("paimon_primary_key")
        .config("spark.sql.catalog.paimon", "org.apache.paimon.spark.SparkCatalog")
        .config("spark.sql.catalog.paimon.warehouse", f"file:{WAREHOUSE}")
        .config(
            "spark.sql.extensions",
            "org.apache.paimon.spark.extensions.PaimonSparkSessionExtensions",
        )
        .config("spark.sql.shuffle.partitions", "1")
        .master("local")
        .getOrCreate()
    )


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster = ClickHouseCluster(__file__)
        cluster.add_instance("node1", stay_alive=True, with_zookeeper=True)
        cluster.start()
        shutil.rmtree(WAREHOUSE, ignore_errors=True)
        cluster.spark_session = get_spark()
        cluster.spark_session.sql("CREATE DATABASE IF NOT EXISTS paimon.default")
        yield cluster
    finally:
        cluster.shutdown()


def create_table(
    spark,
    prefix,
    columns="id INT, val STRING",
    properties="",
    primary_key="id",
    partitioned_by="",
):
    name = f"{prefix}_{uuid.uuid4().hex[:8]}"
    spark.sql(
        f"CREATE TABLE paimon.default.{name} ({columns}) {partitioned_by} TBLPROPERTIES "
        f"('primary-key' = '{primary_key}', 'bucket' = '1', 'file.format' = 'parquet'{properties})"
    )
    return name


def insert_range(spark, name, rows=1000):
    spark.sql(
        f"INSERT INTO paimon.default.{name} SELECT CAST(id AS INT), 'old' FROM range({rows})"
    )


def compact(spark, name, arguments="compact_strategy => 'full'"):
    spark.sql(f"CALL paimon.sys.compact(table => 'default.{name}', {arguments})")


def upload(node, name):
    table_dir = f"{WAREHOUSE}/default.db/{name}"
    LocalUploader(node).upload_directory(table_dir, table_dir)
    return table_dir


def data_files(spark, name):
    return spark.sql(
        f"SELECT partition, level, deleteRowCount, file_path FROM paimon.default.`{name}$files`"
    ).collect()


def assert_files_layout(spark, name, expected):
    """Checks (partition, level, deleteRowCount) of every data file of the latest snapshot."""
    layout = sorted(
        (row.partition, row.level, row.deleteRowCount)
        for row in data_files(spark, name)
    )
    assert layout == expected, layout


def spark_tsv(spark, query):
    return "".join(
        "\t".join("\\N" if value is None else str(value) for value in row) + "\n"
        for row in spark.sql(query).collect()
    )


def assert_reads_as_spark(
    node, spark, name, table_dir, expected, condition="1 = 1", settings=None
):
    spark_result = spark_tsv(
        spark, f"SELECT * FROM paimon.default.{name} WHERE {condition} ORDER BY id"
    )
    assert spark_result == expected, spark_result
    result = node.query(
        f"SELECT * FROM paimonLocal('{table_dir}') WHERE {condition} ORDER BY id",
        settings=settings,
    )
    assert result == expected, result


def assert_count_as_spark(node, spark, name, table_dir, expected):
    spark_result = spark.sql(f"SELECT count(*) FROM paimon.default.{name}").first()[0]
    assert spark_result == expected, spark_result
    assert (
        node.query(f"SELECT count() FROM paimonLocal('{table_dir}')") == f"{expected}\n"
    )


def assert_refused(node, table_dir, bucket):
    for query in [
        f"SELECT * FROM paimonLocal('{table_dir}')",
        f"SELECT count() FROM paimonLocal('{table_dir}')",
    ]:
        error = node.query_and_get_error(query)
        assert (
            "NOT_IMPLEMENTED" in error
            and "merge-on-read is not implemented" in error
            and f"Reading bucket '{bucket}'" in error
        ), error


def test_compacted_buckets(started_cluster):
    node = started_cluster.instances["node1"]
    spark = started_cluster.spark_session
    name = create_table(
        spark,
        "pk_dedup",
        columns="id INT, val STRING, p INT",
        primary_key="id,p",
        partitioned_by="PARTITIONED BY (p)",
    )
    spark.sql(
        f"INSERT INTO paimon.default.{name} VALUES (1, 'old', 1), (2, 'two', 1), (3, 'old', 2)"
    )
    spark.sql(f"INSERT INTO paimon.default.{name} VALUES (1, 'new', 1), (3, 'new', 2)")
    table_dir = upload(node, name)
    assert_files_layout(
        spark, name, [("{1}", 0, 0), ("{1}", 0, 0), ("{2}", 0, 0), ("{2}", 0, 0)]
    )
    assert_refused(node, table_dir, "p=1/bucket-0")

    # A compacted partition reads while another one still needs merging.
    compact(spark, name, "partitions => 'p=1'")
    table_dir = upload(node, name)
    assert_files_layout(spark, name, [("{1}", 5, 0), ("{2}", 0, 0), ("{2}", 0, 0)])
    assert_reads_as_spark(
        node,
        spark,
        name,
        table_dir,
        "1\tnew\t1\n2\ttwo\t1\n",
        condition="p = 1",
        settings={"use_paimon_partition_pruning": 1},
    )
    assert_refused(node, table_dir, "p=2/bucket-0")

    compact(spark, name)
    table_dir = upload(node, name)
    assert_files_layout(spark, name, [("{1}", 5, 0), ("{2}", 5, 0)])
    assert_reads_as_spark(
        node, spark, name, table_dir, "1\tnew\t1\n2\ttwo\t1\n3\tnew\t2\n"
    )
    assert_count_as_spark(node, spark, name, table_dir, 3)

    # The compaction of a single level-0 file moves it to the top level under the same name.
    spark.sql(f"INSERT INTO paimon.default.{name} VALUES (5, 'five', 3)")
    level_zero = [row.file_path for row in data_files(spark, name) if row.level == 0]
    compact(spark, name)
    table_dir = upload(node, name)
    assert_files_layout(spark, name, [("{1}", 5, 0), ("{2}", 5, 0), ("{3}", 5, 0)])
    assert [
        row.file_path for row in data_files(spark, name) if row.partition == "{3}"
    ] == level_zero
    assert_reads_as_spark(
        node, spark, name, table_dir, "1\tnew\t1\n2\ttwo\t1\n3\tnew\t2\n5\tfive\t3\n"
    )

    table = f"{name}_engine"
    node.query(
        f"CREATE TABLE {table} ENGINE = PaimonLocal('{table_dir}')",
        settings={"allow_experimental_paimon_storage_engine": 1},
    )
    assert node.query(f"SELECT * FROM {table} ORDER BY id") == (
        "1\tnew\t1\n2\ttwo\t1\n3\tnew\t2\n5\tfive\t3\n"
    )
    node.query(f"DROP TABLE {table} SYNC")

    # The delta of a snapshot lists changes to the keys, not their state.
    incremental = f"{name}_incremental"
    node.query(
        f"CREATE TABLE {incremental} ENGINE = PaimonLocal('{table_dir}') SETTINGS "
        f"paimon_incremental_read = 1, paimon_keeper_path = '/clickhouse/paimon/{incremental}', "
        "paimon_replica_name = 'r1'",
        settings={"allow_experimental_paimon_storage_engine": 1},
    )
    error = node.query_and_get_error(f"SELECT * FROM {incremental}")
    assert (
        "NOT_IMPLEMENTED" in error
        and "Incremental read of Paimon primary-key table" in error
    ), error
    node.query(f"DROP TABLE {incremental} SYNC")


def test_overlapping_levels(started_cluster):
    node = started_cluster.instances["node1"]
    spark = started_cluster.spark_session
    name = create_table(
        spark, "pk_lookup", properties=", 'changelog-producer' = 'lookup'"
    )
    insert_range(spark, name)
    spark.sql(f"INSERT INTO paimon.default.{name} VALUES (1, 'new')")
    table_dir = upload(node, name)
    # The new version of key 1 is in level 4, the old one in level 5.
    assert_files_layout(spark, name, [("{}", 4, 0), ("{}", 5, 0)])
    assert spark_tsv(spark, f"SELECT * FROM paimon.default.{name} WHERE id = 1") == (
        "1\tnew\n"
    )
    assert_refused(node, table_dir, "bucket-0")


def test_first_row(started_cluster):
    node = started_cluster.instances["node1"]
    spark = started_cluster.spark_session
    name = create_table(
        spark, "pk_first_row", properties=", 'merge-engine' = 'first-row'"
    )
    insert_range(spark, name)
    spark.sql(f"INSERT INTO paimon.default.{name} VALUES (1, 'second'), (1000, 'new')")
    table_dir = upload(node, name)
    assert_files_layout(spark, name, [("{}", 4, 0), ("{}", 5, 0)])
    assert_reads_as_spark(
        node, spark, name, table_dir, "1\told\n1000\tnew\n", condition="id IN (1, 1000)"
    )
    assert_count_as_spark(node, spark, name, table_dir, 1001)


def test_deletion_vectors_enabled(started_cluster):
    node = started_cluster.instances["node1"]
    spark = started_cluster.spark_session
    name = create_table(
        spark, "pk_dv", properties=", 'deletion-vectors.enabled' = 'true'"
    )
    insert_range(spark, name)
    table_dir = upload(node, name)
    # The commit moves its level-0 file to the top level under the same name.
    assert_files_layout(spark, name, [("{}", 5, 0)])
    assert_count_as_spark(node, spark, name, table_dir, 1000)

    spark.sql(f"INSERT INTO paimon.default.{name} VALUES (1000, 'new')")
    table_dir = upload(node, name)
    assert_files_layout(spark, name, [("{}", 4, 0), ("{}", 5, 0)])
    assert (
        spark.sql(f"SELECT * FROM paimon.default.`{name}$table_indexes`").count() == 0
    )
    assert_reads_as_spark(
        node, spark, name, table_dir, "1000\tnew\n", condition="id = 1000"
    )
    assert_count_as_spark(node, spark, name, table_dir, 1001)

    spark.sql(f"UPDATE paimon.default.{name} SET val = 'upd' WHERE id = 1")
    table_dir = upload(node, name)
    error = node.query_and_get_error(f"SELECT * FROM paimonLocal('{table_dir}')")
    assert (
        "NOT_IMPLEMENTED" in error and "deletion vectors are not applied" in error
    ), error


@pytest.mark.parametrize(
    "properties",
    [
        pytest.param(", 'deletion-vectors.enabled' = 'true'", id="deletion-vectors"),
        pytest.param(", 'merge-engine' = 'first-row'", id="first-row"),
    ],
)
def test_level_zero_not_read(started_cluster, properties):
    node = started_cluster.instances["node1"]
    spark = started_cluster.spark_session
    name = create_table(
        spark, "pk_write_only", properties=properties + ", 'write-only' = 'true'"
    )
    spark.sql(f"INSERT INTO paimon.default.{name} VALUES (1, 'a'), (2, 'b')")
    table_dir = upload(node, name)
    assert_files_layout(spark, name, [("{}", 0, 0)])
    assert_reads_as_spark(node, spark, name, table_dir, "")
    assert_count_as_spark(node, spark, name, table_dir, 0)

    # With another batch scan mode Paimon reads level 0, merging it.
    spark.sql(
        f"ALTER TABLE paimon.default.{name} SET TBLPROPERTIES ('batch-scan-mode' = 'compact')"
    )
    table_dir = upload(node, name)
    assert spark.sql(f"SELECT count(*) FROM paimon.default.{name}").first()[0] == 2
    assert_refused(node, table_dir, "bucket-0")

    spark.sql(
        f"ALTER TABLE paimon.default.{name} SET TBLPROPERTIES ('batch-scan-mode' = 'none')"
    )
    table_dir = upload(node, name)
    assert_files_layout(spark, name, [("{}", 0, 0)])
    assert_count_as_spark(node, spark, name, table_dir, 0)
    assert_reads_as_spark(node, spark, name, table_dir, "")

    compact(spark, name)
    table_dir = upload(node, name)
    assert_files_layout(spark, name, [("{}", 5, 0)])
    assert_reads_as_spark(node, spark, name, table_dir, "1\ta\n2\tb\n")


@pytest.mark.parametrize(
    "columns, properties, inserts, expected",
    [
        pytest.param(
            "id INT, a STRING, b STRING",
            ", 'merge-engine' = 'partial-update'",
            ["(1, 'a', NULL)", "(1, NULL, 'b')"],
            "1\ta\tb\n",
            id="partial-update",
        ),
        pytest.param(
            "id INT, s BIGINT",
            ", 'merge-engine' = 'aggregation', 'fields.s.aggregate-function' = 'sum'",
            ["(1, 10)", "(1, 5)"],
            "1\t15\n",
            id="aggregation",
        ),
        pytest.param(
            "id INT, val STRING",
            ", 'data-file.thin-mode' = 'true'",
            ["(1, 'old')", "(1, 'new')"],
            "1\tnew\n",
            id="thin-mode",
        ),
    ],
)
def test_full_compaction(started_cluster, columns, properties, inserts, expected):
    node = started_cluster.instances["node1"]
    spark = started_cluster.spark_session
    name = create_table(spark, "pk_compacted", columns=columns, properties=properties)
    for values in inserts:
        spark.sql(f"INSERT INTO paimon.default.{name} VALUES {values}")
    table_dir = upload(node, name)
    assert_files_layout(spark, name, [("{}", 0, 0), ("{}", 0, 0)])
    assert_refused(node, table_dir, "bucket-0")

    compact(spark, name)
    table_dir = upload(node, name)
    assert_files_layout(spark, name, [("{}", 5, 0)])
    assert_reads_as_spark(node, spark, name, table_dir, expected)


def test_retract_rows(started_cluster):
    node = started_cluster.instances["node1"]
    spark = started_cluster.spark_session
    name = create_table(
        spark,
        "pk_retract",
        properties=", 'changelog-producer' = 'lookup', 'deletion-vectors.modifiable' = 'true'",
    )
    insert_range(spark, name)
    spark.sql(f"DELETE FROM paimon.default.{name} WHERE id = 1")
    # Several levels are readable with deletion vectors, but not a file with the retract row of the delete.
    spark.sql(
        f"ALTER TABLE paimon.default.{name} SET TBLPROPERTIES ('deletion-vectors.enabled' = 'true')"
    )
    table_dir = upload(node, name)
    assert_files_layout(spark, name, [("{}", 4, 1), ("{}", 5, 0)])
    assert spark_tsv(spark, f"SELECT * FROM paimon.default.{name} WHERE id = 1") == ""
    assert_refused(node, table_dir, "bucket-0")
