"""Reading a Paimon snapshot whose index manifest lists deletion vectors must throw in every read mode
(full scan, targeted snapshot, incremental), because the deletion vectors are not applied, and a refused
incremental read must not advance its Keeper watermark. This holds after 'deletion-vectors.enabled' is
turned off, while snapshots without deletion vectors keep reading.

The tables are written at test time by the Paimon Spark bundle baked into the runner image, as in
test_paimon_spark_smoke.
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
WAREHOUSE = f"/var/lib/clickhouse/user_files/paimon_deletion_vectors_{_WORKER}" + (
    f"_{_RUN_ID}" if _RUN_ID else ""
)


def get_spark():
    return (
        pyspark.sql.SparkSession.builder.appName("paimon_deletion_vectors")
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


def create_table(spark, prefix, properties=""):
    """Creates an append-only table with deletion vectors enabled; snapshot 1 holds (1, 'old'), (2, 'two')."""
    name = f"{prefix}_{uuid.uuid4().hex[:8]}"
    spark.sql(
        f"CREATE TABLE paimon.default.{name} (id INT, val STRING) TBLPROPERTIES "
        f"('deletion-vectors.enabled' = 'true', 'file.format' = 'parquet'{properties})"
    )
    spark.sql(f"INSERT INTO paimon.default.{name} VALUES (1, 'old'), (2, 'two')")
    return name


def update(spark, name):
    """Snapshot 2 OVERWRITE writes (1, 'new') and the deletion vector for (1, 'old')."""
    spark.sql(f"UPDATE paimon.default.{name} SET val = 'new' WHERE id = 1")


def upload(node, name):
    table_dir = f"{WAREHOUSE}/default.db/{name}"
    LocalUploader(node).upload_directory(table_dir, table_dir)
    return table_dir


UPDATED = [(1, "APPEND"), (2, "OVERWRITE")]


def assert_snapshot_layout(spark, name, snapshots, deletion_vectors):
    """Checks (id, commit kind) of every snapshot and the row count of each deletion vectors index file of the latest one."""
    layout = [
        (row.snapshot_id, row.commit_kind)
        for row in spark.sql(
            f"SELECT snapshot_id, commit_kind FROM paimon.default.`{name}$snapshots` ORDER BY snapshot_id"
        ).collect()
    ]
    indexes = [
        row.row_count
        for row in spark.sql(
            f"SELECT row_count FROM paimon.default.`{name}$table_indexes` WHERE index_type = 'DELETION_VECTORS'"
        ).collect()
    ]
    assert (layout, indexes) == (snapshots, deletion_vectors), (layout, indexes)


FULL_COMPACTION_HINT = "A full compaction of the table in Paimon"


def cursor_hint(table):
    return f"set '/clickhouse/paimon/{table}/committed_snapshot'"


def assert_refused(node, query, settings=None, hint=FULL_COMPACTION_HINT):
    error = node.query_and_get_error(query, settings=settings)
    assert (
        "NOT_IMPLEMENTED" in error
        and "deletion vectors are not applied" in error
        and hint in error
    ), error


def create_engine_table(node, table_dir, table):
    node.query(
        f"CREATE TABLE {table} ENGINE = PaimonLocal('{table_dir}') SETTINGS "
        f"paimon_incremental_read = 1, paimon_keeper_path = '/clickhouse/paimon/{table}', "
        "paimon_replica_name = 'r1'",
        settings={"allow_experimental_paimon_storage_engine": 1},
    )


def committed_snapshot(node, table):
    return node.query(
        f"SELECT value FROM system.zookeeper WHERE path = '/clickhouse/paimon/{table}' "
        "AND name = 'committed_snapshot'"
    )


def test_full_scan(started_cluster):
    node = started_cluster.instances["node1"]
    spark = started_cluster.spark_session
    name = create_table(spark, "dv")
    update(spark, name)
    table_dir = upload(node, name)
    assert_snapshot_layout(spark, name, UPDATED, [1])

    query = f"SELECT * FROM paimonLocal('{table_dir}') ORDER BY id, val"
    assert_refused(node, query)
    assert_refused(node, f"SELECT count() FROM paimonLocal('{table_dir}')")
    # The table stays inspectable.
    assert node.query(f"DESCRIBE paimonLocal('{table_dir}')").startswith(
        "id\tNullable(Int32)"
    )

    # A cached index manifest keeps refusing the read after the file is gone.
    cached = {"use_paimon_metadata_files_cache": 1}
    assert_refused(node, query, cached)
    node.exec_in_container(["bash", "-c", f"rm {table_dir}/manifest/index-manifest-*"])
    assert_refused(node, query, cached)


def test_deletion_vectors_turned_off(started_cluster):
    node = started_cluster.instances["node1"]
    spark = started_cluster.spark_session
    name = create_table(spark, "dv_off", ", 'deletion-vectors.modifiable' = 'true'")
    update(spark, name)
    # Snapshot 3 is on a schema with the option off, but its index manifest still lists the deletion vector.
    spark.sql(
        f"ALTER TABLE paimon.default.{name} SET TBLPROPERTIES ('deletion-vectors.enabled' = 'false')"
    )
    spark.sql(f"INSERT INTO paimon.default.{name} VALUES (3, 'three')")
    table_dir = upload(node, name)
    assert_snapshot_layout(spark, name, UPDATED + [(3, "APPEND")], [1])
    options = spark.sql(
        f"SELECT value FROM paimon.default.`{name}$options` WHERE key = 'deletion-vectors.enabled'"
    ).collect()
    assert [row.value for row in options] == ["false"], options

    assert_refused(node, f"SELECT * FROM paimonLocal('{table_dir}') ORDER BY id, val")


def test_full_compaction(started_cluster):
    node = started_cluster.instances["node1"]
    spark = started_cluster.spark_session
    name = create_table(spark, "dv_compacted", ", 'bucket' = '1', 'bucket-key' = 'id'")
    update(spark, name)
    # Applies the deletion vector and leaves an empty deletion vectors index file in its place.
    spark.sql(
        f"CALL paimon.sys.compact(table => 'default.{name}', compact_strategy => 'full')"
    )
    table_dir = upload(node, name)
    assert_snapshot_layout(spark, name, UPDATED + [(3, "COMPACT")], [0])

    query = f"SELECT * FROM paimonLocal('{table_dir}') ORDER BY id, val"
    cached = {"use_paimon_metadata_files_cache": 1}
    assert node.query(query) == "1\tnew\n2\ttwo\n"
    assert node.query(query, settings=cached) == "1\tnew\n2\ttwo\n"

    # Once cached, the index manifest is not read from storage again.
    node.exec_in_container(["bash", "-c", f"rm {table_dir}/manifest/index-manifest-*"])
    assert node.query(query, settings=cached) == "1\tnew\n2\ttwo\n"
    assert "index-manifest-" in node.query_and_get_error(query)


def test_table_engine_read_modes(started_cluster):
    node = started_cluster.instances["node1"]
    spark = started_cluster.spark_session
    name = create_table(spark, "dv_engine")
    table_dir = upload(node, name)

    resumed = f"{name}_resumed"
    create_engine_table(node, table_dir, resumed)
    assert_snapshot_layout(spark, name, [(1, "APPEND")], [])
    assert node.query(f"SELECT count() FROM {resumed}") == "2\n"
    assert committed_snapshot(node, resumed) == "1\n"

    update(spark, name)
    upload(node, name)
    assert_snapshot_layout(spark, name, UPDATED, [1])

    # The incremental read reaches the OVERWRITE snapshot 2 that added the deletion vector.
    assert_refused(node, f"SELECT count() FROM {resumed}", hint=cursor_hint(resumed))
    assert committed_snapshot(node, resumed) == "1\n"

    assert_refused(
        node, f"SELECT count() FROM {resumed} SETTINGS paimon_target_snapshot_id = 2"
    )
    assert (
        node.query(
            f"SELECT * FROM {resumed} ORDER BY id SETTINGS paimon_target_snapshot_id = 1"
        )
        == "1\told\n2\ttwo\n"
    )

    # A reader that has consumed nothing records no watermark.
    first = f"{name}_first"
    create_engine_table(node, table_dir, first)
    assert_refused(node, f"SELECT count() FROM {first}")
    assert committed_snapshot(node, first) == ""

    # A full compaction does not unblock the incremental read; moving the cursor to its snapshot does.
    spark.sql(
        f"CALL paimon.sys.compact(table => 'default.{name}', compact_strategy => 'full')"
    )
    upload(node, name)
    assert_snapshot_layout(spark, name, UPDATED + [(3, "COMPACT")], [])
    assert_refused(node, f"SELECT count() FROM {resumed}", hint=cursor_hint(resumed))
    assert committed_snapshot(node, resumed) == "1\n"

    compaction = spark.sql(
        f"SELECT max(snapshot_id) FROM paimon.default.`{name}$snapshots`"
    ).collect()[0][0]
    zk = started_cluster.get_kazoo_client("zoo1")
    zk.set(f"/clickhouse/paimon/{resumed}/committed_snapshot", str(compaction).encode())
    zk.stop()
    spark.sql(f"INSERT INTO paimon.default.{name} VALUES (5, 'five')")
    upload(node, name)
    assert_snapshot_layout(spark, name, UPDATED + [(3, "COMPACT"), (4, "APPEND")], [])
    assert node.query(f"SELECT * FROM {resumed}") == "5\tfive\n"
    assert committed_snapshot(node, resumed) == f"{compaction + 1}\n"

    node.query(f"DROP TABLE {resumed} SYNC")
    node.query(f"DROP TABLE {first} SYNC")


def test_skipped_compaction(started_cluster):
    node = started_cluster.instances["node1"]
    spark = started_cluster.spark_session
    name = f"dv_partial_{uuid.uuid4().hex[:8]}"
    spark.sql(
        f"CREATE TABLE paimon.default.{name} (id INT, val STRING, p INT) PARTITIONED BY (p) "
        "TBLPROPERTIES ('deletion-vectors.enabled' = 'true', 'file.format' = 'parquet')"
    )
    spark.sql(f"INSERT INTO paimon.default.{name} VALUES (1, 'old', 1), (2, 'old', 2)")
    table_dir = upload(node, name)

    table = f"{name}_engine"
    create_engine_table(node, table_dir, table)
    assert node.query(f"SELECT count() FROM {table}") == "2\n"
    assert committed_snapshot(node, table) == "1\n"

    # Snapshot 2 OVERWRITE adds a deletion vector in each partition; snapshot 3 COMPACT applies the p=1 one only.
    spark.sql(f"UPDATE paimon.default.{name} SET val = 'new' WHERE val = 'old'")
    spark.sql(
        f"CALL paimon.sys.compact(table => 'default.{name}', partitions => 'p=1')"
    )
    upload(node, name)
    assert_snapshot_layout(spark, name, UPDATED + [(3, "COMPACT")], [1])

    # With the cursor past snapshot 2, the next one is the COMPACT snapshot the incremental read skips.
    zk = started_cluster.get_kazoo_client("zoo1")
    zk.set(f"/clickhouse/paimon/{table}/committed_snapshot", b"2")
    zk.stop()
    assert_refused(node, f"SELECT count() FROM {table}", hint=cursor_hint(table))
    assert committed_snapshot(node, table) == "2\n"

    node.query(f"DROP TABLE {table} SYNC")
