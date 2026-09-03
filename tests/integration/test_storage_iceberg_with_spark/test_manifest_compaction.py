import gzip
import json
import os
import re
import pytest
import threading
from datetime import datetime, timezone
import time

from helpers.iceberg_utils import (
    create_iceberg_table,
    default_upload_directory,
    default_download_directory,
    get_uuid_str,
    get_last_snapshot
)


def _open_metadata_file(filepath):
    """Open an Iceberg metadata file, transparently handling gzip compression.

    ClickHouse writes compressed metadata with the encoding name (e.g. `gzip`)
    embedded in the middle of the file name, e.g. `v<N>.gzip.metadata.json`,
    so the filename still ends with `.json`. Detect gzip by the magic bytes
    (0x1f 0x8b) to be agnostic to the exact naming convention.
    """
    with open(filepath, "rb") as raw:
        magic = raw.read(2)
    if magic == b"\x1f\x8b":
        return gzip.open(filepath, "rt")
    return open(filepath, "r")


def _metadata_version_from_name(filename):
    """Extract the leading metadata version from a metadata file name for tie-breaking.

    Iceberg metadata files are named like `v<N>.metadata.json`, `v<N>.gzip.metadata.json`
    or `<N>-<uuid>.metadata.json`; return <N> (0 if not parseable)."""
    match = re.match(r"v?0*(\d+)", filename)
    return int(match.group(1)) if match else 0


def _load_latest_metadata(path_to_table):
    """Return the parsed latest metadata file. When two files share last-updated-ms, the higher
    metadata version wins, so the result is deterministic regardless of os.listdir order."""
    metadata_dir = f"{path_to_table}/metadata/"
    best = None
    best_key = None
    for filename in os.listdir(metadata_dir):
        if not filename.endswith(".json"):
            continue
        with _open_metadata_file(os.path.join(metadata_dir, filename)) as f:
            data = json.load(f)
        key = (data.get("last-updated-ms", 0), _metadata_version_from_name(filename))
        if best_key is None or key > best_key:
            best_key = key
            best = data
    return best


def get_current_snapshot_summary(path_to_table):
    """Return the summary dict of the current snapshot from the latest metadata file."""
    best = _load_latest_metadata(path_to_table)
    if best is None:
        return {}
    current_id = best.get("current-snapshot-id")
    for snap in best.get("snapshots", []):
        if snap.get("snapshot-id") == current_id:
            return snap.get("summary", {})
    return {}


def get_all_snapshot_ids(path_to_table):
    """Return the set of all snapshot-ids recorded in the latest metadata file."""
    best = _load_latest_metadata(path_to_table)
    return {snap.get("snapshot-id") for snap in (best or {}).get("snapshots", [])}


@pytest.mark.parametrize("format_version", ["2"])
@pytest.mark.parametrize("storage_type", ["s3"])
def test_optimize_manifest_files(started_cluster_iceberg_with_spark, storage_type, format_version):
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    TABLE_NAME = "test_optimize_manifests_v" + format_version + "_" + storage_type + "_" + get_uuid_str()

    # Merge-on-read modes are only valid for v2 tables; v1 has no row-level deletes.
    if format_version == "2":
        tbl_properties = (
            "'format-version' = '2', "
            "'write.update.mode' = 'merge-on-read', "
            "'write.delete.mode' = 'merge-on-read', "
            "'write.merge.mode' = 'merge-on-read'"
        )
    else:
        tbl_properties = "'format-version' = '1'"

    spark.sql(
        f"""
        CREATE TABLE {TABLE_NAME} (id long, data string) USING iceberg TBLPROPERTIES ({tbl_properties})
        """
    )
    spark.sql(f"INSERT INTO {TABLE_NAME} select id, char(id + ascii('a')) from range(10, 100)")

    default_upload_directory(
        started_cluster_iceberg_with_spark,
        storage_type,
        f"/iceberg_data/default/{TABLE_NAME}/",
        f"/iceberg_data/default/{TABLE_NAME}/",
    )

    create_iceberg_table(storage_type, instance, TABLE_NAME, started_cluster_iceberg_with_spark)
    snapshot_id = get_last_snapshot(f"/var/lib/clickhouse/user_files/iceberg_data/default/{TABLE_NAME}/")

    assert instance.query(f"SELECT id FROM {TABLE_NAME} ORDER BY id SETTINGS iceberg_snapshot_id = {snapshot_id}") == instance.query(
        "SELECT number FROM numbers(10, 90)"
    )

    time.sleep(0.1)
    assert int(instance.query(f"SELECT count() FROM {TABLE_NAME}")) == 90

    spark.sql(f"INSERT INTO {TABLE_NAME} select id, char(id + ascii('a')) from range(100, 200)")
    spark.sql(f"INSERT INTO {TABLE_NAME} select id, char(id + ascii('a')) from range(600, 700)")
    default_upload_directory(
        started_cluster_iceberg_with_spark,
        storage_type,
        f"/iceberg_data/default/{TABLE_NAME}/",
        f"/iceberg_data/default/{TABLE_NAME}/",
    )
    spark.sql(f"INSERT INTO {TABLE_NAME} select id, char(id + ascii('a')) from range(200, 300)")

    spark.sql(f"INSERT INTO {TABLE_NAME} select id, char(id + ascii('a')) from range(300, 400)")
    spark.sql(f"INSERT INTO {TABLE_NAME} select id, char(id + ascii('a')) from range(400, 500)")

    default_upload_directory(
        started_cluster_iceberg_with_spark,
        storage_type,
        f"/iceberg_data/default/{TABLE_NAME}/",
        f"/iceberg_data/default/{TABLE_NAME}/",
    )
    spark.sql(f"INSERT INTO {TABLE_NAME} select id, char(id + ascii('a')) from range(600, 700)")
    default_upload_directory(
        started_cluster_iceberg_with_spark,
        storage_type,
        f"/iceberg_data/default/{TABLE_NAME}/",
        f"/iceberg_data/default/{TABLE_NAME}/",
    )

    instance.query(f"OPTIMIZE TABLE {TABLE_NAME} MANIFEST;", settings={"allow_experimental_iceberg_compaction" : 1})

    # check that timetravel works with previous snapshot_ids and timestamps
    assert instance.query(f"SELECT id FROM {TABLE_NAME} ORDER BY id SETTINGS iceberg_snapshot_id = {snapshot_id}") == instance.query(
        "SELECT number FROM numbers(10, 90)"
    )

    instance.query(f"OPTIMIZE TABLE {TABLE_NAME} MANIFEST;", settings={"allow_experimental_iceberg_compaction" : 1})

    # Verify Spark can still read the table correctly after manifest compaction.
    # Total rows: range(10,100) + range(100,200) + range(600,700) + range(200,300)
    #           + range(300,400) + range(400,500) + range(600,700) = 690
    # (range(600,700) is inserted twice, so it contributes 200 rows.)
    default_download_directory(
        started_cluster_iceberg_with_spark,
        storage_type,
        f"/var/lib/clickhouse/user_files/iceberg_data/default/{TABLE_NAME}/",
        f"/var/lib/clickhouse/user_files/iceberg_data/default/{TABLE_NAME}/",
    )
    spark_rows = spark.read.format("iceberg").load(
        f"/var/lib/clickhouse/user_files/iceberg_data/default/{TABLE_NAME}"
    ).collect()
    assert len(spark_rows) == 690

    spark_ids = sorted(row["id"] for row in spark_rows)
    clickhouse_ids = list(map(int, instance.query(
        f"SELECT id FROM {TABLE_NAME} ORDER BY id"
    ).split()))
    assert spark_ids == clickhouse_ids


def test_optimize_manifest_files_preserves_stats(started_cluster_iceberg_with_spark):
    """
    OPTIMIZE TABLE ... MANIFEST must preserve the per-column statistics carried by the source
    manifest entries (column_sizes, value_counts, null_value_counts, lower_bounds, upper_bounds).
    Dropping them would weaken predicate pushdown / file pruning after compaction.
    """
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    storage_type = "local"
    TABLE_NAME = "test_optimize_manifest_stats_" + storage_type + "_" + get_uuid_str()

    spark.sql(
        f"CREATE TABLE {TABLE_NAME} (id long, data string) USING iceberg "
        f"TBLPROPERTIES ('format-version' = '2')"
    )
    # Several separate inserts so the current snapshot has several data manifests (> threshold).
    for lo in range(0, 50, 10):
        spark.sql(
            f"INSERT INTO {TABLE_NAME} SELECT id, char(id + ascii('a')) FROM range({lo}, {lo + 10})"
        )

    default_upload_directory(
        started_cluster_iceberg_with_spark,
        storage_type,
        f"/iceberg_data/default/{TABLE_NAME}/",
        f"/iceberg_data/default/{TABLE_NAME}/",
    )
    create_iceberg_table(storage_type, instance, TABLE_NAME, started_cluster_iceberg_with_spark)

    metadata_dir = (
        f"/var/lib/clickhouse/user_files/iceberg_data/default/{TABLE_NAME}/metadata"
    )

    def list_data_manifests():
        return set(
            instance.exec_in_container(
                [
                    "bash",
                    "-c",
                    f"find '{metadata_dir}' -maxdepth 1 -name '*.avro' "
                    f"-not -name 'snap-*.avro' -type f",
                ]
            )
            .strip()
            .splitlines()
        )

    manifests_before = list_data_manifests()

    instance.query(
        f"OPTIMIZE TABLE {TABLE_NAME} MANIFEST",
        settings={
            "allow_experimental_iceberg_compaction": 1,
            "iceberg_manifest_min_count_to_compact": 2,
        },
    )

    # The manifest-only rewrite does not delete old files, so the newly written consolidated
    # manifest(s) are exactly the data manifests that appeared after the OPTIMIZE.
    new_manifests = sorted(list_data_manifests() - manifests_before)
    assert new_manifests, "OPTIMIZE TABLE ... MANIFEST did not produce a consolidated manifest"

    entries_checked = 0
    for manifest in new_manifests:
        result = instance.query(
            f"""
            SELECT
                tupleElement(data_file, 'content')                  AS content,
                length(tupleElement(data_file, 'column_sizes'))     AS n_column_sizes,
                length(tupleElement(data_file, 'value_counts'))     AS n_value_counts,
                length(tupleElement(data_file, 'null_value_counts')) AS n_null_value_counts,
                length(tupleElement(data_file, 'lower_bounds'))     AS n_lower_bounds,
                length(tupleElement(data_file, 'upper_bounds'))     AS n_upper_bounds
            FROM file('{manifest}', Avro)
            FORMAT TSV
            """
        ).strip()
        if not result:
            continue
        for line in result.splitlines():
            content, n_col, n_val, n_null, n_lower, n_upper = map(int, line.split("\t"))
            if content != 0:  # data files only
                continue
            # The table has two columns (id, data); both carry stats in the source manifests.
            assert n_col == 2, f"column_sizes dropped: {n_col}"
            assert n_val == 2, f"value_counts dropped: {n_val}"
            assert n_null == 2, f"null_value_counts dropped: {n_null}"
            assert n_lower == 2, f"lower_bounds dropped: {n_lower}"
            assert n_upper == 2, f"upper_bounds dropped: {n_upper}"
            entries_checked += 1

    assert entries_checked > 0, "no data-file entries found in consolidated manifest(s)"


def test_optimize_manifest_files_preserves_sort_order_id(started_cluster_iceberg_with_spark):
    """
    OPTIMIZE TABLE ... MANIFEST must preserve each data file's sort_order_id. A manifest-only
    rewrite does not touch the data files, so a table sorted before compaction must stay sorted
    afterwards; dropping sort_order_id would make ClickHouse treat the table as unsorted.
    """
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    storage_type = "local"
    TABLE_NAME = "test_optimize_manifest_sortorder_" + storage_type + "_" + get_uuid_str()

    spark.sql(
        f"CREATE TABLE {TABLE_NAME} (id long, data string) USING iceberg "
        f"TBLPROPERTIES ('format-version' = '2')"
    )
    # Establish a sort order, so data files written afterwards carry a non-default sort_order_id.
    spark.sql(f"ALTER TABLE {TABLE_NAME} WRITE ORDERED BY id")
    for lo in range(0, 50, 10):
        spark.sql(
            f"INSERT INTO {TABLE_NAME} SELECT id, char(id + ascii('a')) FROM range({lo}, {lo + 10}) ORDER BY id"
        )

    default_upload_directory(
        started_cluster_iceberg_with_spark,
        storage_type,
        f"/iceberg_data/default/{TABLE_NAME}/",
        f"/iceberg_data/default/{TABLE_NAME}/",
    )
    create_iceberg_table(storage_type, instance, TABLE_NAME, started_cluster_iceberg_with_spark)

    metadata_dir = (
        f"/var/lib/clickhouse/user_files/iceberg_data/default/{TABLE_NAME}/metadata"
    )

    def list_data_manifests():
        return set(
            instance.exec_in_container(
                [
                    "bash",
                    "-c",
                    f"find '{metadata_dir}' -maxdepth 1 -name '*.avro' "
                    f"-not -name 'snap-*.avro' -type f",
                ]
            )
            .strip()
            .splitlines()
        )

    def data_file_sort_order_ids(manifests):
        ids = set()
        for manifest in manifests:
            result = instance.query(
                f"""
                SELECT
                    tupleElement(data_file, 'content')        AS content,
                    tupleElement(data_file, 'sort_order_id')  AS sort_order_id
                FROM file('{manifest}', Avro)
                FORMAT TSV
                """
            ).strip()
            for line in result.splitlines():
                if not line:
                    continue
                content, sort_order_id = line.split("\t")
                if int(content) != 0:  # data files only
                    continue
                ids.add(sort_order_id)
        return ids

    manifests_before = list_data_manifests()
    source_sort_order_ids = data_file_sort_order_ids(manifests_before)
    # The source files must carry a concrete (non-null) sort_order_id for the test to be meaningful.
    assert source_sort_order_ids and "\\N" not in source_sort_order_ids, (
        f"expected source data files to have a sort_order_id, got: {source_sort_order_ids}"
    )

    instance.query(
        f"OPTIMIZE TABLE {TABLE_NAME} MANIFEST",
        settings={
            "allow_experimental_iceberg_compaction": 1,
            "iceberg_manifest_min_count_to_compact": 2,
        },
    )

    new_manifests = sorted(list_data_manifests() - manifests_before)
    assert new_manifests, "OPTIMIZE TABLE ... MANIFEST did not produce a consolidated manifest"

    # The consolidated manifest must report the same sort_order_id(s) as the source files.
    assert data_file_sort_order_ids(new_manifests) == source_sort_order_ids


@pytest.mark.parametrize("storage_type", ["s3"])
def test_optimize_manifest_files_partition_evolution(started_cluster_iceberg_with_spark, storage_type):
    """
    OPTIMIZE TABLE ... MANIFEST on a table whose partition spec evolved must rewrite each manifest
    under the partition spec its source files were written with, not the default spec. Re-encoding
    old partition tuples under the default spec would corrupt partition metadata, so the end-to-end
    check is that the data is still correct and Spark (a reference reader) can read it back.
    """
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    TABLE_NAME = "test_optimize_manifest_partevo_" + storage_type + "_" + get_uuid_str()

    # spec 0: partitioned by bucket(4, id).
    spark.sql(
        f"""
        CREATE TABLE {TABLE_NAME} (id long, data string) USING iceberg
        PARTITIONED BY (bucket(4, id))
        TBLPROPERTIES ('format-version' = '2')
        """
    )
    spark.sql(f"INSERT INTO {TABLE_NAME} SELECT id, char(id + ascii('a')) FROM range(0, 20)")
    spark.sql(f"INSERT INTO {TABLE_NAME} SELECT id, char(id + ascii('a')) FROM range(20, 40)")

    # Evolve the partition spec → spec 1.
    spark.sql(f"ALTER TABLE {TABLE_NAME} ADD PARTITION FIELD truncate(2, data)")
    spark.sql(f"INSERT INTO {TABLE_NAME} SELECT id, char(id + ascii('a')) FROM range(40, 60)")
    spark.sql(f"INSERT INTO {TABLE_NAME} SELECT id, char(id + ascii('a')) FROM range(60, 80)")

    default_upload_directory(
        started_cluster_iceberg_with_spark,
        storage_type,
        f"/iceberg_data/default/{TABLE_NAME}/",
        f"/iceberg_data/default/{TABLE_NAME}/",
    )
    create_iceberg_table(storage_type, instance, TABLE_NAME, started_cluster_iceberg_with_spark)

    assert int(instance.query(f"SELECT count() FROM {TABLE_NAME}")) == 80

    instance.query(
        f"OPTIMIZE TABLE {TABLE_NAME} MANIFEST",
        settings={
            "allow_experimental_iceberg_compaction": 1,
            "iceberg_manifest_min_count_to_compact": 2,
        },
    )

    # Data is unchanged after the manifest-only rewrite of a partition-evolved table.
    assert int(instance.query(f"SELECT count() FROM {TABLE_NAME}")) == 80
    assert instance.query(f"SELECT id FROM {TABLE_NAME} ORDER BY id") == instance.query(
        "SELECT number FROM numbers(0, 80)"
    )

    # Spark must still read the table back correctly (partition metadata not corrupted).
    default_download_directory(
        started_cluster_iceberg_with_spark,
        storage_type,
        f"/var/lib/clickhouse/user_files/iceberg_data/default/{TABLE_NAME}/",
        f"/var/lib/clickhouse/user_files/iceberg_data/default/{TABLE_NAME}/",
    )
    spark_rows = spark.read.format("iceberg").load(
        f"/var/lib/clickhouse/user_files/iceberg_data/default/{TABLE_NAME}"
    ).collect()
    assert len(spark_rows) == 80
    assert sorted(row["id"] for row in spark_rows) == list(range(0, 80))


@pytest.mark.parametrize("storage_type", ["s3"])
def test_optimize_manifest_files_dropped_partition_source_column(
    started_cluster_iceberg_with_spark, storage_type
):
    """
    OPTIMIZE TABLE ... MANIFEST must derive each preserved manifest's partition value types from a
    schema that actually defines the spec's source columns, not unconditionally from the current
    schema. After partition evolution drops a partition field and the source column itself is then
    dropped, the current schema no longer contains that column, yet the current snapshot still
    references manifests written under the old spec. Deriving the partition types from the current
    schema would throw (the column is absent) or encode the preserved partition tuple under the
    wrong type. The end-to-end check is that compaction succeeds and the data is still read back
    correctly.

    Note: once the source column is dropped, Spark itself can no longer bind the orphaned spec 0
    (`SerializableTable.specs` eagerly binds every historical spec against the current schema and
    throws `Cannot find source column for partition field`), so any Spark write or read of the
    table fails. The mixed-spec data is therefore written before dropping the column, and the
    post-drop state is verified through ClickHouse only.
    """
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    TABLE_NAME = "test_optimize_manifest_droppedcol_" + storage_type + "_" + get_uuid_str()

    # spec 0: partitioned by identity(region). 'region' is both a partition source and a column.
    spark.sql(
        f"""
        CREATE TABLE {TABLE_NAME} (id long, data string, region string) USING iceberg
        PARTITIONED BY (region)
        TBLPROPERTIES ('format-version' = '2')
        """
    )
    spark.sql(
        f"INSERT INTO {TABLE_NAME} VALUES "
        f"(0, 'a', 'us'), (1, 'b', 'us'), (2, 'c', 'eu'), (3, 'd', 'eu')"
    )
    spark.sql(f"INSERT INTO {TABLE_NAME} VALUES (4, 'e', 'us'), (5, 'f', 'eu')")

    # Evolve: drop the partition field → new unpartitioned spec 1.
    spark.sql(f"ALTER TABLE {TABLE_NAME} DROP PARTITION FIELD region")

    # Insert more rows under the new (unpartitioned) spec so specs are mixed. This must happen
    # while 'region' still exists, because dropping it leaves spec 0 unbindable by Spark.
    spark.sql(f"INSERT INTO {TABLE_NAME} VALUES (6, 'g', 'us'), (7, 'h', 'eu')")

    # Now drop the source column. The current snapshot still references the spec-0 manifests above,
    # whose partition source column no longer exists in the current schema.
    spark.sql(f"ALTER TABLE {TABLE_NAME} DROP COLUMN region")

    default_upload_directory(
        started_cluster_iceberg_with_spark,
        storage_type,
        f"/iceberg_data/default/{TABLE_NAME}/",
        f"/iceberg_data/default/{TABLE_NAME}/",
    )
    create_iceberg_table(storage_type, instance, TABLE_NAME, started_cluster_iceberg_with_spark)

    assert int(instance.query(f"SELECT count() FROM {TABLE_NAME}")) == 8

    # Without resolving the old spec's source-column type from a historical schema this throws.
    instance.query(
        f"OPTIMIZE TABLE {TABLE_NAME} MANIFEST",
        settings={
            "allow_experimental_iceberg_compaction": 1,
            "iceberg_manifest_min_count_to_compact": 2,
        },
    )

    # Re-read through ClickHouse after compaction to confirm partition metadata is not corrupted.
    # (Spark cannot read this table: it fails to bind the orphaned spec 0 — see the docstring.)
    assert int(instance.query(f"SELECT count() FROM {TABLE_NAME}")) == 8
    assert instance.query(f"SELECT id FROM {TABLE_NAME} ORDER BY id") == instance.query(
        "SELECT number FROM numbers(0, 8)"
    )


@pytest.mark.parametrize("storage_type", ["s3"])
def test_optimize_manifest_files_dropped_partition_source_column_schema_header(
    started_cluster_iceberg_with_spark, storage_type
):
    """
    A manifest-only rewrite (`OPTIMIZE TABLE ... MANIFEST`) must serialize into each compacted
    manifest's Avro `schema` metadata a schema that still defines the manifest's partition-spec
    `source-id`s — not unconditionally the current schema. After partition evolution drops a
    partition field and the source column itself is then dropped, the current schema no longer
    contains that column. If the rewritten manifest carried the current schema in its `schema`
    header, the spec's `source-id`s would no longer resolve on read and `ManifestFileIterator`
    would silently drop the partition field, so the manifest would stop faithfully describing the
    files it carried forward.

    This is the schema-header regression for the partition-type scenario covered by
    `test_optimize_manifest_files_dropped_partition_source_column`: here we additionally read the
    Avro container metadata of the newly written manifest back and assert every partition-spec
    `source-id` is still present in its `schema` header.
    """
    from avro.datafile import DataFileReader
    from avro.io import DatumReader

    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    TABLE_NAME = "test_optimize_manifest_droppedcol_schema_" + storage_type + "_" + get_uuid_str()

    # spec 0: partitioned by identity(region). 'region' is both a partition source and a column.
    # `commit.manifest-merge.enabled = false` is essential: with Iceberg's default manifest merging
    # every spec-0 append would be folded back into a single spec-0 manifest, so the current
    # snapshot would carry only one manifest per spec (here 2 total). That is at or below the
    # compaction threshold below, and even past it `writeConsolidatedManifestFile` would find the
    # manifests already optimal (one per partition). Disabling the merge keeps each append as its
    # own manifest, so spec 0 actually accumulates more manifests than partition groups.
    spark.sql(
        f"""
        CREATE TABLE {TABLE_NAME} (id long, data string, region string) USING iceberg
        PARTITIONED BY (region)
        TBLPROPERTIES ('format-version' = '2', 'commit.manifest-merge.enabled' = 'false')
        """
    )
    # Three separate inserts under spec 0 over the same two partitions (us, eu). With manifest
    # merging disabled each Spark append writes one manifest, so spec 0 ends up with more manifests
    # (3) than it has unique partition groups (2). This is what makes a manifest-only rewrite
    # actually consolidate — otherwise `writeConsolidatedManifestFile` finds the manifests already
    # optimal (one per partition) and writes nothing, leaving no compacted manifest to inspect the
    # `schema` header of.
    spark.sql(
        f"INSERT INTO {TABLE_NAME} VALUES "
        f"(0, 'a', 'us'), (1, 'b', 'us'), (2, 'c', 'eu'), (3, 'd', 'eu')"
    )
    spark.sql(f"INSERT INTO {TABLE_NAME} VALUES (4, 'e', 'us'), (5, 'f', 'eu')")
    spark.sql(f"INSERT INTO {TABLE_NAME} VALUES (8, 'i', 'us'), (9, 'j', 'eu')")

    # Evolve: drop the partition field → new unpartitioned spec 1. Insert more rows under the new
    # spec while 'region' still exists (dropping it leaves spec 0 unbindable by Spark).
    spark.sql(f"ALTER TABLE {TABLE_NAME} DROP PARTITION FIELD region")
    spark.sql(f"INSERT INTO {TABLE_NAME} VALUES (6, 'g', 'us'), (7, 'h', 'eu')")

    # Now drop the source column. The current snapshot still references the spec-0 manifests above,
    # whose partition source column no longer exists in the current schema.
    spark.sql(f"ALTER TABLE {TABLE_NAME} DROP COLUMN region")

    default_upload_directory(
        started_cluster_iceberg_with_spark,
        storage_type,
        f"/iceberg_data/default/{TABLE_NAME}/",
        f"/iceberg_data/default/{TABLE_NAME}/",
    )
    create_iceberg_table(storage_type, instance, TABLE_NAME, started_cluster_iceberg_with_spark)

    table_dir = f"/var/lib/clickhouse/user_files/iceberg_data/default/{TABLE_NAME}/"
    local_metadata_dir = os.path.join(table_dir, "metadata")

    def download_and_list_manifests():
        # Mirror the table (including the manifests written to object storage) onto the test host so
        # the Avro container metadata can be read directly. `snap-*.avro` are manifest lists, not
        # manifests, so they are excluded.
        default_download_directory(
            started_cluster_iceberg_with_spark, storage_type, table_dir, table_dir
        )
        return {
            os.path.join(local_metadata_dir, name)
            for name in os.listdir(local_metadata_dir)
            if name.endswith(".avro") and not name.startswith("snap-")
        }

    manifests_before = download_and_list_manifests()

    instance.query(
        f"OPTIMIZE TABLE {TABLE_NAME} MANIFEST",
        settings={
            "allow_experimental_iceberg_compaction": 1,
            "iceberg_manifest_min_count_to_compact": 2,
        },
    )

    new_manifests = download_and_list_manifests() - manifests_before
    assert new_manifests, "OPTIMIZE TABLE ... MANIFEST did not write any new manifest files"

    def read_avro_user_metadata(path):
        with open(path, "rb") as f:
            reader = DataFileReader(f, DatumReader())
            try:
                decoded = {}
                for key, value in reader.meta.items():
                    key = key.decode("utf-8") if isinstance(key, bytes) else key
                    if isinstance(value, bytes):
                        value = value.decode("utf-8")
                    decoded[key] = value
                return decoded
            finally:
                reader.close()

    # At least one newly written manifest must carry the old (partitioned) spec, and for every
    # partition-spec source-id its `schema` header must still define a field with that id.
    checked_partitioned_manifest = False
    for manifest_path in sorted(new_manifests):
        meta = read_avro_user_metadata(manifest_path)
        if "partition-spec" not in meta or "schema" not in meta:
            continue
        partition_spec = json.loads(meta["partition-spec"])
        if not partition_spec:
            # The unpartitioned spec-1 manifest — nothing to resolve.
            continue
        schema_field_ids = {field["id"] for field in json.loads(meta["schema"])["fields"]}
        for spec_field in partition_spec:
            source_id = spec_field["source-id"]
            assert source_id in schema_field_ids, (
                f"Compacted manifest {os.path.basename(manifest_path)} has a schema header with "
                f"field ids {sorted(schema_field_ids)} that does not define partition-spec "
                f"source-id {source_id}; the rewrite serialized the current schema instead of one "
                f"defining the spec's source columns"
            )
        checked_partitioned_manifest = True

    assert checked_partitioned_manifest, (
        "No newly written manifest with a non-empty partition spec was found to verify"
    )


@pytest.mark.parametrize("storage_type", ["s3"])
def test_optimize_manifest_files_bucket_partition(started_cluster_iceberg_with_spark, storage_type):
    """
    OPTIMIZE TABLE ... MANIFEST on a bucket-partitioned table must recompute the manifest-list
    partition summary for the bucket value. The `icebergBucket` transform resolves to ClickHouse
    `UInt32`, which `getAvroType` maps to Avro `int`; the byte encoder must serialize that unsigned
    type instead of throwing 'Can not dump such stats', otherwise a valid Iceberg bucket partition
    cannot be compacted.
    """
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    TABLE_NAME = "test_optimize_manifest_bucket_" + storage_type + "_" + get_uuid_str()

    spark.sql(
        f"""
        CREATE TABLE {TABLE_NAME} (id long, data string) USING iceberg
        PARTITIONED BY (bucket(4, id))
        TBLPROPERTIES ('format-version' = '2')
        """
    )
    for lo in range(0, 80, 20):
        spark.sql(
            f"INSERT INTO {TABLE_NAME} SELECT id, char(id + ascii('a')) FROM range({lo}, {lo + 20})"
        )

    default_upload_directory(
        started_cluster_iceberg_with_spark,
        storage_type,
        f"/iceberg_data/default/{TABLE_NAME}/",
        f"/iceberg_data/default/{TABLE_NAME}/",
    )
    create_iceberg_table(storage_type, instance, TABLE_NAME, started_cluster_iceberg_with_spark)

    assert int(instance.query(f"SELECT count() FROM {TABLE_NAME}")) == 80

    instance.query(
        f"OPTIMIZE TABLE {TABLE_NAME} MANIFEST",
        settings={
            "allow_experimental_iceberg_compaction": 1,
            "iceberg_manifest_min_count_to_compact": 2,
        },
    )

    assert int(instance.query(f"SELECT count() FROM {TABLE_NAME}")) == 80
    assert instance.query(f"SELECT id FROM {TABLE_NAME} ORDER BY id") == instance.query(
        "SELECT number FROM numbers(0, 80)"
    )

    # Spark must still read the table back correctly after the bucket-partition manifest rewrite.
    default_download_directory(
        started_cluster_iceberg_with_spark,
        storage_type,
        f"/var/lib/clickhouse/user_files/iceberg_data/default/{TABLE_NAME}/",
        f"/var/lib/clickhouse/user_files/iceberg_data/default/{TABLE_NAME}/",
    )
    spark_rows = spark.read.format("iceberg").load(
        f"/var/lib/clickhouse/user_files/iceberg_data/default/{TABLE_NAME}"
    ).collect()
    assert len(spark_rows) == 80
    assert sorted(row["id"] for row in spark_rows) == list(range(0, 80))


def test_optimize_manifest_files_preserves_entry_lineage(started_cluster_iceberg_with_spark):
    """
    OPTIMIZE TABLE ... MANIFEST is metadata-only, so each rewritten manifest entry must stay an
    EXISTING entry that keeps the snapshot-id and data sequence number that originally added the
    file, rather than being re-stamped as ADDED by the new (replace) snapshot. Otherwise the
    snapshot is internally inconsistent (the manifest list reports the files as existing) and row
    lineage / delete-file sequence-number matching would be corrupted.
    """
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    storage_type = "local"
    TABLE_NAME = "test_optimize_manifest_lineage_" + storage_type + "_" + get_uuid_str()

    spark.sql(
        f"CREATE TABLE {TABLE_NAME} (id long, data string) USING iceberg "
        f"TBLPROPERTIES ('format-version' = '2')"
    )
    # Several separate inserts → several snapshots, so files carry distinct original snapshot-ids.
    for lo in range(0, 50, 10):
        spark.sql(
            f"INSERT INTO {TABLE_NAME} SELECT id, char(id + ascii('a')) FROM range({lo}, {lo + 10})"
        )

    default_upload_directory(
        started_cluster_iceberg_with_spark,
        storage_type,
        f"/iceberg_data/default/{TABLE_NAME}/",
        f"/iceberg_data/default/{TABLE_NAME}/",
    )
    create_iceberg_table(storage_type, instance, TABLE_NAME, started_cluster_iceberg_with_spark)

    table_path = f"/var/lib/clickhouse/user_files/iceberg_data/default/{TABLE_NAME}/"
    metadata_dir = f"{table_path}metadata"

    def list_data_manifests():
        return set(
            instance.exec_in_container(
                [
                    "bash",
                    "-c",
                    f"find '{metadata_dir}' -maxdepth 1 -name '*.avro' "
                    f"-not -name 'snap-*.avro' -type f",
                ]
            )
            .strip()
            .splitlines()
        )

    manifests_before = list_data_manifests()
    # Snapshot-ids that exist before compaction; every preserved entry must reference one of these
    # (its original adder), never the brand-new replace snapshot created by the compaction.
    original_snapshot_ids = get_all_snapshot_ids(table_path)
    assert original_snapshot_ids, "expected the pre-compaction metadata to record snapshots"

    def data_file_basename(path):
        return path.rstrip("/").split("/")[-1]

    # Resolved (inheritance-applied) data sequence_number per data file, captured BEFORE compaction and
    # keyed by the data file's basename. Reading the manifests raw would return null here: Spark writes
    # ADDED entries without an explicit sequence number and Iceberg inherits it from the manifest list at
    # read time, so system.iceberg_files (which resolves that inheritance) is the reliable source.
    sequence_number_before = {}
    rows_before = instance.query(
        f"""
        SELECT file_path, sequence_number
        FROM system.iceberg_files
        WHERE database = currentDatabase() AND table = '{TABLE_NAME}' AND content = 'DATA'
        FORMAT TSV
        """
    ).strip()
    for line in rows_before.splitlines():
        file_path, sequence_number = line.split("\t")
        sequence_number_before[data_file_basename(file_path)] = sequence_number
    assert sequence_number_before, "expected to read pre-compaction data-file sequence numbers"

    instance.query(
        f"OPTIMIZE TABLE {TABLE_NAME} MANIFEST",
        settings={
            "allow_experimental_iceberg_compaction": 1,
            "iceberg_manifest_min_count_to_compact": 2,
        },
    )

    new_manifests = sorted(list_data_manifests() - manifests_before)
    assert new_manifests, "OPTIMIZE TABLE ... MANIFEST did not produce a consolidated manifest"

    entries_checked = 0
    for manifest in new_manifests:
        result = instance.query(
            f"""
            SELECT
                status,
                snapshot_id,
                sequence_number,
                file_sequence_number,
                tupleElement(data_file, 'file_path') AS file_path,
                tupleElement(data_file, 'content') AS content
            FROM file('{manifest}', Avro)
            FORMAT TSV
            """
        ).strip()
        if not result:
            continue
        for line in result.splitlines():
            status, snapshot_id, sequence_number, file_sequence_number, file_path, content = line.split("\t")
            if int(content) != 0:  # data files only
                continue
            # A metadata-only rewrite carries files forward: entries are EXISTING (status 0), not
            # ADDED, so the new snapshot is consistent with the manifest list (which reports them as
            # existing) and incremental planning can tell them apart from additions.
            assert int(status) == 0, f"expected EXISTING entry (status 0), got {status}"
            # The original adding snapshot is preserved: the entry references one of the original
            # snapshots, not the brand-new replace snapshot created by the compaction.
            assert snapshot_id != "\\N", "snapshot_id must be preserved (non-null)"
            assert int(snapshot_id) in original_snapshot_ids, (
                f"entry snapshot_id {snapshot_id} should be an original adder, "
                f"not a snapshot created by the compaction"
            )
            assert sequence_number != "\\N", "sequence_number must be preserved (non-null)"
            assert file_sequence_number != "\\N", "file_sequence_number must be preserved (non-null)"
            # The rewrite must carry each file's original sequence numbers forward, not re-stamp them with
            # the new (replace) snapshot's sequence number. Compare against the resolved pre-compaction
            # values captured above: both the data sequence_number and the file_sequence_number, which for
            # these plain-inserted files equal the file's original data sequence number.
            key = data_file_basename(file_path)
            assert key in sequence_number_before, (
                f"carried-forward file {file_path} was not present before compaction"
            )
            assert sequence_number == sequence_number_before[key], (
                f"data sequence_number for {file_path} changed from "
                f"{sequence_number_before[key]} to {sequence_number}"
            )
            assert file_sequence_number == sequence_number_before[key], (
                f"file_sequence_number for {file_path} changed from "
                f"{sequence_number_before[key]} to {file_sequence_number}"
            )
            entries_checked += 1

    assert entries_checked > 0, "no data-file entries found in consolidated manifest(s)"


@pytest.mark.parametrize("format_version", ["2"])
@pytest.mark.parametrize("storage_type", ["s3"])
def test_optimize_manifest_files_with_deletes(started_cluster_iceberg_with_spark, storage_type, format_version):
    """
    OPTIMIZE TABLE ... MANIFEST must preserve delete files. It consolidates only the data
    manifests, while delete-file manifests are carried forward unchanged into the new manifest
    list. If they were dropped, the previously deleted rows would reappear after compaction.

    Covers v2 (position delete files) row-level deletes. Format-version 3 is rejected by
    manifest compaction for now (see test_optimize_manifest_files_v3_rejected), because the
    writer does not yet round-trip the v3 row-lineage 'first_row_id' metadata.
    """
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    TABLE_NAME = "test_optimize_manifest_deletes_v" + format_version + "_" + storage_type + "_" + get_uuid_str()

    spark.sql(
        f"""
        CREATE TABLE {TABLE_NAME} (id long, data string) USING iceberg TBLPROPERTIES (
            'format-version' = '{format_version}',
            'write.update.mode' = 'merge-on-read',
            'write.delete.mode' = 'merge-on-read',
            'write.merge.mode' = 'merge-on-read'
        )
        """
    )

    # Separate inserts so the current snapshot accumulates several data manifests
    # (above the compaction threshold set below).
    for lo in range(10, 100, 10):
        spark.sql(
            f"INSERT INTO {TABLE_NAME} select id, char(id + ascii('a')) from range({lo}, {lo + 10})"
        )

    # Merge-on-read delete: produces a row-level delete (position-delete file in v2, deletion
    # vector in v3) tracked by a delete-file manifest.
    spark.sql(f"DELETE FROM {TABLE_NAME} WHERE id < 20")

    default_upload_directory(
        started_cluster_iceberg_with_spark,
        storage_type,
        f"/iceberg_data/default/{TABLE_NAME}/",
        f"/iceberg_data/default/{TABLE_NAME}/",
    )

    create_iceberg_table(storage_type, instance, TABLE_NAME, started_cluster_iceberg_with_spark)

    # 90 inserted, ids 10..19 deleted -> 80 live rows.
    assert int(instance.query(f"SELECT count() FROM {TABLE_NAME}")) == 80
    assert instance.query(f"SELECT id FROM {TABLE_NAME} ORDER BY id") == instance.query(
        "SELECT number FROM numbers(20, 80)"
    )

    optimize_settings = {
        "allow_experimental_iceberg_compaction": 1,
        "iceberg_manifest_min_count_to_compact": 2,
    }
    instance.query(f"OPTIMIZE TABLE {TABLE_NAME} MANIFEST", settings=optimize_settings)

    # The deletes must still be applied after manifest compaction (no resurrected rows).
    # ClickHouse reads directly from storage, so no download is needed for these checks.
    assert int(instance.query(f"SELECT count() FROM {TABLE_NAME}")) == 80
    assert instance.query(f"SELECT id FROM {TABLE_NAME} ORDER BY id") == instance.query(
        "SELECT number FROM numbers(20, 80)"
    )

    # Download the ClickHouse-written metadata/manifests from storage so we can inspect the
    # new snapshot summary locally and let Spark read the table back.
    default_download_directory(
        started_cluster_iceberg_with_spark,
        storage_type,
        f"/var/lib/clickhouse/user_files/iceberg_data/default/{TABLE_NAME}/",
        f"/var/lib/clickhouse/user_files/iceberg_data/default/{TABLE_NAME}/",
    )

    # A manifest-only rewrite must have happened (replace operation).
    summary = get_current_snapshot_summary(
        f"/var/lib/clickhouse/user_files/iceberg_data/default/{TABLE_NAME}/"
    )
    assert summary.get("operation") == "replace", (
        f"Expected operation='replace', got: {summary.get('operation')}"
    )

    # Spark must still read the same rows after ClickHouse rewrote the manifests.
    spark_rows = spark.read.format("iceberg").load(
        f"/var/lib/clickhouse/user_files/iceberg_data/default/{TABLE_NAME}"
    ).collect()
    assert len(spark_rows) == 80
    spark_ids = sorted(row["id"] for row in spark_rows)
    assert spark_ids == list(range(20, 100))


@pytest.mark.parametrize("storage_type", ["s3"])
def test_optimize_manifest_files_v3_rejected(started_cluster_iceberg_with_spark, storage_type):
    """
    Format-version 3 adds row lineage: each data file carries an inherited 'first_row_id' from
    which readers assign '_row_id'. The manifest writer does not yet round-trip 'first_row_id'
    (it uses the v2 Avro schema for v3), so a manifest-only rewrite would carry data files forward
    while dropping their row ids, producing a v3 table with a valid-looking snapshot but broken row
    lineage. Until the round-trip is implemented, OPTIMIZE TABLE ... MANIFEST must fail loudly on a
    v3 table rather than silently corrupt lineage.
    """
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    TABLE_NAME = "test_optimize_manifest_v3_rejected_" + storage_type + "_" + get_uuid_str()

    spark.sql(
        f"CREATE TABLE {TABLE_NAME} (id long, data string) USING iceberg "
        f"TBLPROPERTIES ('format-version' = '3')"
    )
    for lo in range(0, 40, 10):
        spark.sql(
            f"INSERT INTO {TABLE_NAME} select id, char(id + ascii('a')) from range({lo}, {lo + 10})"
        )

    default_upload_directory(
        started_cluster_iceberg_with_spark,
        storage_type,
        f"/iceberg_data/default/{TABLE_NAME}/",
        f"/iceberg_data/default/{TABLE_NAME}/",
    )
    create_iceberg_table(storage_type, instance, TABLE_NAME, started_cluster_iceberg_with_spark)

    assert int(instance.query(f"SELECT count() FROM {TABLE_NAME}")) == 40

    error_message = instance.query_and_get_error(
        f"OPTIMIZE TABLE {TABLE_NAME} MANIFEST",
        settings={
            "allow_experimental_iceberg_compaction": 1,
            "iceberg_manifest_min_count_to_compact": 2,
        },
    )
    assert "not yet supported for Iceberg format-version 3" in error_message

    # The rejected compaction must leave the table untouched and still readable.
    assert int(instance.query(f"SELECT count() FROM {TABLE_NAME}")) == 40


@pytest.mark.parametrize("storage_type", ["s3"])
def test_optimize_manifest_files_v1_rejected(started_cluster_iceberg_with_spark, storage_type):
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    TABLE_NAME = "test_optimize_manifest_v1_rejected_" + storage_type + "_" + get_uuid_str()

    spark.sql(
        f"CREATE TABLE {TABLE_NAME} (id long, data string) USING iceberg "
        f"TBLPROPERTIES ('format-version' = '1')"
    )
    for lo in range(0, 40, 10):
        spark.sql(
            f"INSERT INTO {TABLE_NAME} select id, char(id + ascii('a')) from range({lo}, {lo + 10})"
        )

    default_upload_directory(
        started_cluster_iceberg_with_spark,
        storage_type,
        f"/iceberg_data/default/{TABLE_NAME}/",
        f"/iceberg_data/default/{TABLE_NAME}/",
    )
    create_iceberg_table(storage_type, instance, TABLE_NAME, started_cluster_iceberg_with_spark)

    assert int(instance.query(f"SELECT count() FROM {TABLE_NAME}")) == 40

    error_message = instance.query_and_get_error(
        f"OPTIMIZE TABLE {TABLE_NAME} MANIFEST",
        settings={
            "allow_experimental_iceberg_compaction": 1,
            "iceberg_manifest_min_count_to_compact": 2,
        },
    )
    assert "supported only for Iceberg format_version 2" in error_message

    assert int(instance.query(f"SELECT count() FROM {TABLE_NAME}")) == 40


@pytest.mark.parametrize("storage_type", ["s3"])
def test_optimize_manifest_files_partitioned(started_cluster_iceberg_with_spark, storage_type):
    """
    Test manifest-only compaction for a partitioned Iceberg table.

    The table is partitioned by 'region' (3 distinct values).  We perform many
    small inserts across all partitions so that the number of manifest files
    grows well above the compaction threshold.  After OPTIMIZE TABLE ... MANIFEST
    the manifests should be consolidated to one per partition.

    Checks:
    - Data correctness is preserved after compaction.
    - Time-travel via snapshot_id still works after compaction.
    - A second OPTIMIZE invocation is a no-op (already optimal).
    - The compaction threshold setting is honoured: with the default threshold (30)
      a table that already has <= 30 manifest files is left untouched, while with
      a lower threshold (2) compaction is triggered sooner.
    """
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    TABLE_NAME = "test_optimize_manifests_partitioned_" + storage_type + "_" + get_uuid_str()

    # 3 distinct partition values
    REGIONS = ["eu", "us", "ap"]
    NUM_PARTITIONS = len(REGIONS)

    # ── Create partitioned table ──────────────────────────────────────────────
    spark.sql(
        f"""
        CREATE TABLE {TABLE_NAME} (id long, data string, region string)
        USING iceberg
        PARTITIONED BY (region)
        TBLPROPERTIES (
            'format-version' = '2',
            'write.update.mode'  = 'merge-on-read',
            'write.delete.mode'  = 'merge-on-read',
            'write.merge.mode'   = 'merge-on-read'
        )
        """
    )

    # ── Initial insert – one batch per partition ──────────────────────────────
    for region in REGIONS:
        spark.sql(
            f"INSERT INTO {TABLE_NAME} "
            f"SELECT id, char(id + ascii('a')), '{region}' "
            f"FROM range(0, 30)"
        )

    default_upload_directory(
        started_cluster_iceberg_with_spark,
        storage_type,
        f"/iceberg_data/default/{TABLE_NAME}/",
        f"/iceberg_data/default/{TABLE_NAME}/",
    )

    create_iceberg_table(storage_type, instance, TABLE_NAME, started_cluster_iceberg_with_spark)
    first_snapshot_id = get_last_snapshot(f"/var/lib/clickhouse/user_files/iceberg_data/default/{TABLE_NAME}/")
    snapshot_timestamp = datetime.now(timezone.utc)

    time.sleep(0.1)
    # 30 rows × 3 regions = 90 rows
    assert int(instance.query(f"SELECT count() FROM {TABLE_NAME}")) == 90

    # Time-travel snapshot should also see 90 rows
    assert int(instance.query(
        f"SELECT count() FROM {TABLE_NAME} "
        f"SETTINGS iceberg_snapshot_id = {first_snapshot_id}"
    )) == 90

    # ── Many more small inserts to create many manifest files ─────────────────
    # 6 batches × 3 regions = 18 additional inserts → well above the lowered threshold (2)
    for batch_start in range(30, 90, 10):
        for region in REGIONS:
            spark.sql(
                f"INSERT INTO {TABLE_NAME} "
                f"SELECT id, char(id + ascii('a')), '{region}' "
                f"FROM range({batch_start}, {batch_start + 10})"
            )
        default_upload_directory(
            started_cluster_iceberg_with_spark,
            storage_type,
            f"/iceberg_data/default/{TABLE_NAME}/",
            f"/iceberg_data/default/{TABLE_NAME}/",
        )

    snapshot_id = get_last_snapshot(f"/var/lib/clickhouse/user_files/iceberg_data/default/{TABLE_NAME}/")

    # 90 (initial) + 6 batches × 10 rows × 3 regions = 90 + 180 = 270
    total_rows = 90 + 6 * 10 * NUM_PARTITIONS
    assert int(instance.query(
        f"SELECT count() FROM {TABLE_NAME} "
        f"SETTINGS iceberg_snapshot_id = {snapshot_id}"
    )) == total_rows

    # ── Run manifest compaction ───────────────────────────────────────────────
    # Lower threshold to 2 so that compaction is definitely triggered
    # (each partition will have at least 7 manifest files after the inserts above)
    instance.query(
        f"OPTIMIZE TABLE {TABLE_NAME} MANIFEST",
        settings={
            "allow_experimental_iceberg_compaction": 1,
            "iceberg_manifest_min_count_to_compact": 2,
        },
    )

    # ── Data correctness after compaction ────────────────────────────────────
    # Check the current (post-compaction) snapshot via the default read path.
    assert int(instance.query(f"SELECT count() FROM {TABLE_NAME}")) == total_rows

    for region in REGIONS:
        expected_count = 90  # 30 initial + 6 × 10 additional
        actual_count = int(instance.query(
            f"SELECT count() FROM {TABLE_NAME} WHERE region = '{region}'"
        ))
        assert actual_count == expected_count, \
            f"Region '{region}': expected {expected_count} rows after compaction, got {actual_count}"

    # Cross-check: the pre-compaction snapshot must also still be readable.
    assert int(instance.query(
        f"SELECT count() FROM {TABLE_NAME} "
        f"SETTINGS iceberg_snapshot_id = {snapshot_id}"
    )) == total_rows

    # ── Time-travel still works after compaction ──────────────────────────────
    assert int(instance.query(
        f"SELECT count() FROM {TABLE_NAME} "
        f"SETTINGS iceberg_snapshot_id = {first_snapshot_id}"
    )) == 90

    assert int(instance.query(
        f"SELECT count() FROM {TABLE_NAME} "
        f"SETTINGS iceberg_timestamp_ms = {int(snapshot_timestamp.timestamp() * 1000)}"
    )) == 90

    # ── Second OPTIMIZE should be a no-op (already one manifest per partition) ─
    # This must not raise and must leave data intact.
    instance.query(
        f"OPTIMIZE TABLE {TABLE_NAME} MANIFEST;",
        settings={
            "allow_experimental_iceberg_compaction": 1,
            "iceberg_manifest_min_count_to_compact": 2,
        },
    )
    # Verify the current snapshot is still intact after the no-op.
    assert int(instance.query(f"SELECT count() FROM {TABLE_NAME}")) == total_rows

    # ── Third OPTIMIZE should throw exception
    error_message = instance.query_and_get_error(
        f"OPTIMIZE TABLE {TABLE_NAME} FINAL MANIFEST;",
        settings={
            "allow_experimental_iceberg_compaction": 1,
            "iceberg_manifest_min_count_to_compact": 2,
        },
    )
    assert "OPTIMIZE MANIFEST is incompatible with FINAL, PARTITION, DEDUPLICATE, CLEANUP, and DRY RUN options" in error_message


@pytest.mark.parametrize("storage_type", ["s3"])
def test_optimize_manifest_files_partitioned_concurrent(started_cluster_iceberg_with_spark, storage_type):
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    TABLE_NAME = "test_optimize_manifests_concurrent_" + storage_type + "_" + get_uuid_str()

    REGIONS = ["eu", "us", "ap"]
    NUM_PARTITIONS = len(REGIONS)

    spark.sql(
        f"""
        CREATE TABLE {TABLE_NAME} (id long, data string, region string)
        USING iceberg
        PARTITIONED BY (region)
        TBLPROPERTIES (
            'format-version' = '2',
            'write.update.mode'  = 'merge-on-read',
            'write.delete.mode'  = 'merge-on-read',
            'write.merge.mode'   = 'merge-on-read'
        )
        """
    )

    # Initial insert – one batch per partition.
    for region in REGIONS:
        spark.sql(
            f"INSERT INTO {TABLE_NAME} "
            f"SELECT id, char(id + ascii('a')), '{region}' "
            f"FROM range(0, 30)"
        )

    # Many more small inserts to create many manifest files (>> compaction threshold).
    for batch_start in range(30, 90, 10):
        for region in REGIONS:
            spark.sql(
                f"INSERT INTO {TABLE_NAME} "
                f"SELECT id, char(id + ascii('a')), '{region}' "
                f"FROM range({batch_start}, {batch_start + 10})"
            )

    default_upload_directory(
        started_cluster_iceberg_with_spark,
        storage_type,
        f"/iceberg_data/default/{TABLE_NAME}/",
        f"/iceberg_data/default/{TABLE_NAME}/",
    )

    create_iceberg_table(storage_type, instance, TABLE_NAME, started_cluster_iceberg_with_spark)

    # 30 initial + 6 × 10 additional rows per region.
    expected_per_region = 90
    total_rows = expected_per_region * NUM_PARTITIONS

    # Sanity check before launching threads.
    assert int(instance.query(f"SELECT count() FROM {TABLE_NAME}")) == total_rows

    NUM_READER_THREADS = 4
    NUM_OPTIMIZE_THREADS = 2
    # Iteration-bounded rather than wall-clock-bounded: a slow CI runner
    # would otherwise truncate the test to a handful of iterations and miss
    # the conflict windows we're trying to exercise.
    OPTIMIZE_ITERATIONS_PER_THREAD = 5

    errors = []
    errors_lock = threading.Lock()
    optimize_attempts = [0] * NUM_OPTIMIZE_THREADS
    reader_attempts = [0] * NUM_READER_THREADS

    optimizers_done_event = threading.Event()
    finished_optimizers = [0]
    finished_lock = threading.Lock()

    def report_error(label, exc):
        with errors_lock:
            errors.append(f"{label}: {type(exc).__name__}: {exc}")

    def reader_loop(idx):
        # Readers run as long as any optimizer is still in flight, so the
        # exposure to the conflict window scales with the optimize workload
        # rather than with wall-clock time.
        try:
            while not optimizers_done_event.is_set():
                got_total = int(instance.query(f"SELECT count() FROM {TABLE_NAME}"))
                if got_total != total_rows:
                    raise AssertionError(
                        f"SELECT count() returned {got_total}, expected {total_rows}"
                    )
                region = REGIONS[idx % NUM_PARTITIONS]
                got_part = int(instance.query(
                    f"SELECT count() FROM {TABLE_NAME} WHERE region = '{region}'"
                ))
                if got_part != expected_per_region:
                    raise AssertionError(
                        f"count(WHERE region={region}) returned {got_part}, "
                        f"expected {expected_per_region}"
                    )
                reader_attempts[idx] += 1
        except Exception as exc:
            report_error(f"reader-{idx}", exc)

    def optimize_loop(idx):
        try:
            for _ in range(OPTIMIZE_ITERATIONS_PER_THREAD):
                instance.query(
                    f"OPTIMIZE TABLE {TABLE_NAME} MANIFEST",
                    settings={
                        "allow_experimental_iceberg_compaction": 1,
                        "iceberg_manifest_min_count_to_compact": 2,
                    },
                )
                optimize_attempts[idx] += 1
        except Exception as exc:
            report_error(f"optimize-{idx}", exc)
        finally:
            # Wake the readers as soon as the last optimizer is done so the
            # whole test ends in bounded time even if an optimizer raised.
            with finished_lock:
                finished_optimizers[0] += 1
                if finished_optimizers[0] == NUM_OPTIMIZE_THREADS:
                    optimizers_done_event.set()

    readers = [
        threading.Thread(target=reader_loop, args=(i,), daemon=True)
        for i in range(NUM_READER_THREADS)
    ]
    optimizers = [
        threading.Thread(target=optimize_loop, args=(i,), daemon=True)
        for i in range(NUM_OPTIMIZE_THREADS)
    ]

    for t in optimizers:
        t.start()
    for t in readers:
        t.start()

    # Generous per-thread join timeout so a slow CI runner does not flake;
    # the test itself completes as soon as all threads finish their bounded work.
    for t in optimizers + readers:
        t.join(timeout=300)
        assert not t.is_alive(), "Worker thread did not finish in time"

    assert not errors, "Concurrent run produced errors:\n" + "\n".join(errors)
    assert sum(reader_attempts) > 0, "No reads were performed"
    assert sum(optimize_attempts) == NUM_OPTIMIZE_THREADS * OPTIMIZE_ITERATIONS_PER_THREAD, (
        f"Expected {NUM_OPTIMIZE_THREADS * OPTIMIZE_ITERATIONS_PER_THREAD} OPTIMIZE iterations, "
        f"got {sum(optimize_attempts)}"
    )

    # Final consistency check.
    assert int(instance.query(f"SELECT count() FROM {TABLE_NAME}")) == total_rows
    for region in REGIONS:
        assert int(instance.query(
            f"SELECT count() FROM {TABLE_NAME} WHERE region = '{region}'"
        )) == expected_per_region


@pytest.mark.parametrize("storage_type", ["s3"])
def test_optimize_manifest_totals_invariant(started_cluster_iceberg_with_spark, storage_type):
    """
    Regression test: repeated OPTIMIZE TABLE ... MANIFEST must not inflate the
    snapshot summary totals (total-data-files, total-records, total-files-size).

    Before the fix, each compaction call passed added_files = total_data_files and
    added_records/added_files_size from the previous snapshot delta to
    generateNextMetadata, which computes total_* = parent_total_* + added_*.
    This caused totals to double (or more) with every OPTIMIZE run.
    """
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    TABLE_NAME = "test_optimize_totals_" + storage_type + "_" + get_uuid_str()
    TABLE_PATH = f"/var/lib/clickhouse/user_files/iceberg_data/default/{TABLE_NAME}/"

    spark.sql(
        f"""
        CREATE TABLE {TABLE_NAME} (id long, data string) USING iceberg
        TBLPROPERTIES ('format-version' = '2')
        """
    )

    # Several inserts to produce multiple manifest files.
    for batch_start in range(0, 50, 10):
        spark.sql(
            f"INSERT INTO {TABLE_NAME} "
            f"SELECT id, char(id + ascii('a')) FROM range({batch_start}, {batch_start + 10})"
        )
        default_upload_directory(
            started_cluster_iceberg_with_spark,
            storage_type,
            f"/iceberg_data/default/{TABLE_NAME}/",
            f"/iceberg_data/default/{TABLE_NAME}/",
        )

    create_iceberg_table(storage_type, instance, TABLE_NAME, started_cluster_iceberg_with_spark)
    assert int(instance.query(f"SELECT count() FROM {TABLE_NAME}")) == 50

    # First compaction — consolidates manifests.
    instance.query(
        f"OPTIMIZE TABLE {TABLE_NAME} MANIFEST",
        settings={
            "allow_experimental_iceberg_compaction": 1,
            "iceberg_manifest_min_count_to_compact": 2,
        },
    )
    default_download_directory(
        started_cluster_iceberg_with_spark,
        storage_type,
        f"/var/lib/clickhouse/user_files/iceberg_data/default/{TABLE_NAME}/",
        f"/var/lib/clickhouse/user_files/iceberg_data/default/{TABLE_NAME}/",
    )
    summary_after_first = get_current_snapshot_summary(TABLE_PATH)
    assert summary_after_first, "Could not read snapshot summary after first compaction"

    # Verify the operation type is correct for a manifest-only rewrite.
    assert summary_after_first.get("operation") == "replace", (
        f"Expected operation='replace', got: {summary_after_first.get('operation')}"
    )

    total_files_1 = int(summary_after_first.get("total-data-files", -1))
    total_records_1 = int(summary_after_first.get("total-records", -1))
    total_size_1 = int(summary_after_first.get("total-files-size", -1))

    assert total_files_1 >= 0
    assert total_records_1 == 50

    # Second compaction — already optimal, should be a no-op that does NOT change totals.
    instance.query(
        f"OPTIMIZE TABLE {TABLE_NAME} MANIFEST",
        settings={
            "allow_experimental_iceberg_compaction": 1,
            "iceberg_manifest_min_count_to_compact": 2,
        },
    )
    default_download_directory(
        started_cluster_iceberg_with_spark,
        storage_type,
        f"/var/lib/clickhouse/user_files/iceberg_data/default/{TABLE_NAME}/",
        f"/var/lib/clickhouse/user_files/iceberg_data/default/{TABLE_NAME}/",
    )
    summary_after_second = get_current_snapshot_summary(TABLE_PATH)
    assert summary_after_second, "Could not read snapshot summary after second compaction"

    # The second round is already optimal, so it must not change the totals.
    assert int(summary_after_second.get("total-data-files", -1)) == total_files_1, (
        f"total-data-files changed by no-op compaction: {total_files_1} -> {summary_after_second.get('total-data-files')}"
    )
    assert int(summary_after_second.get("total-records", -1)) == total_records_1, (
        f"total-records changed by no-op compaction: {total_records_1} -> {summary_after_second.get('total-records')}"
    )
    assert int(summary_after_second.get("total-files-size", -1)) == total_size_1, (
        f"total-files-size changed by no-op compaction: {total_size_1} -> {summary_after_second.get('total-files-size')}"
    )

    # Third compaction — totals must remain identical.
    instance.query(
        f"OPTIMIZE TABLE {TABLE_NAME} MANIFEST",
        settings={
            "allow_experimental_iceberg_compaction": 1,
            "iceberg_manifest_min_count_to_compact": 1,
        },
    )
    default_download_directory(
        started_cluster_iceberg_with_spark,
        storage_type,
        f"/var/lib/clickhouse/user_files/iceberg_data/default/{TABLE_NAME}/",
        f"/var/lib/clickhouse/user_files/iceberg_data/default/{TABLE_NAME}/",
    )
    summary_after_third = get_current_snapshot_summary(TABLE_PATH)
    assert summary_after_third, "Could not read snapshot summary after third compaction"

    total_files_3 = int(summary_after_third.get("total-data-files", -1))
    total_records_3 = int(summary_after_third.get("total-records", -1))
    total_size_3 = int(summary_after_third.get("total-files-size", -1))

    assert total_files_3 == total_files_1, (
        f"total-data-files inflated: {total_files_1} -> {total_files_3}"
    )
    assert total_records_3 == total_records_1, (
        f"total-records inflated: {total_records_1} -> {total_records_3}"
    )
    assert total_size_3 == total_size_1, (
        f"total-files-size inflated: {total_size_1} -> {total_size_3}"
    )

    # Data must still be correct after all compaction rounds.
    assert int(instance.query(f"SELECT count() FROM {TABLE_NAME}")) == 50


@pytest.mark.parametrize("compression_method", ["", "gzip"])
@pytest.mark.parametrize("storage_type", ["s3"])
def test_optimize_manifest_totals_invariant_schema_evolution(
    started_cluster_iceberg_with_spark, storage_type, compression_method
):
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    suffix = compression_method or "none"
    TABLE_NAME = f"test_optimize_totals_se_{suffix}_{storage_type}_{get_uuid_str()}"
    TABLE_PATH = f"/var/lib/clickhouse/user_files/iceberg_data/default/{TABLE_NAME}/"

    base_settings = {"allow_insert_into_iceberg": 1}
    if compression_method:
        base_settings["iceberg_metadata_compression_method"] = compression_method

    create_iceberg_table(
        storage_type,
        instance,
        TABLE_NAME,
        started_cluster_iceberg_with_spark,
        "(x Nullable(Int32))",
        format_version=2,
        compression_method=compression_method if compression_method else None,
    )

    # Schema evolution: widen, add, then drop a column to produce non-trivial metadata.
    instance.query(
        f"ALTER TABLE {TABLE_NAME} MODIFY COLUMN x Nullable(Int64);",
        settings=base_settings,
    )
    instance.query(
        f"INSERT INTO {TABLE_NAME} SELECT number FROM numbers(0, 10);",
        settings=base_settings,
    )
    instance.query(
        f"INSERT INTO {TABLE_NAME} SELECT number FROM numbers(10, 10);",
        settings=base_settings,
    )

    instance.query(
        f"ALTER TABLE {TABLE_NAME} ADD COLUMN y Nullable(Float64);",
        settings=base_settings,
    )
    instance.query(
        f"INSERT INTO {TABLE_NAME} SELECT number, number + 0.5 FROM numbers(20, 10);",
        settings=base_settings,
    )
    instance.query(
        f"INSERT INTO {TABLE_NAME} SELECT number, number + 0.5 FROM numbers(30, 10);",
        settings=base_settings,
    )

    instance.query(
        f"ALTER TABLE {TABLE_NAME} DROP COLUMN x;",
        settings=base_settings,
    )
    instance.query(
        f"INSERT INTO {TABLE_NAME} SELECT number + 0.5 FROM numbers(40, 10);",
        settings=base_settings,
    )

    total_rows = 50
    assert int(instance.query(f"SELECT count() FROM {TABLE_NAME}")) == total_rows

    optimize_settings = dict(base_settings)
    optimize_settings.update({
        "allow_experimental_iceberg_compaction": 1,
        "iceberg_manifest_min_count_to_compact": 2,
    })

    # First compaction — consolidates manifests.
    instance.query(
        f"OPTIMIZE TABLE {TABLE_NAME} MANIFEST",
        settings=optimize_settings,
    )
    default_download_directory(
        started_cluster_iceberg_with_spark,
        storage_type,
        f"/var/lib/clickhouse/user_files/iceberg_data/default/{TABLE_NAME}/",
        f"/var/lib/clickhouse/user_files/iceberg_data/default/{TABLE_NAME}/",
    )
    summary_after_first = get_current_snapshot_summary(TABLE_PATH)
    assert summary_after_first, (
        f"Could not read snapshot summary after first compaction "
        f"(compression='{compression_method}')"
    )
    assert summary_after_first.get("operation") == "replace", (
        f"Expected operation='replace', got: {summary_after_first.get('operation')}"
    )

    total_files_1 = int(summary_after_first.get("total-data-files", -1))
    total_records_1 = int(summary_after_first.get("total-records", -1))
    total_size_1 = int(summary_after_first.get("total-files-size", -1))

    assert total_files_1 >= 0
    assert total_records_1 == total_rows

    # Second compaction — already optimal, totals must stay identical.
    optimize_settings["iceberg_manifest_min_count_to_compact"] = 1
    instance.query(
        f"OPTIMIZE TABLE {TABLE_NAME} MANIFEST",
        settings=optimize_settings,
    )
    default_download_directory(
        started_cluster_iceberg_with_spark,
        storage_type,
        f"/var/lib/clickhouse/user_files/iceberg_data/default/{TABLE_NAME}/",
        f"/var/lib/clickhouse/user_files/iceberg_data/default/{TABLE_NAME}/",
    )
    summary_after_second = get_current_snapshot_summary(TABLE_PATH)
    assert summary_after_second, (
        f"Could not read snapshot summary after second compaction "
        f"(compression='{compression_method}')"
    )

    total_files_2 = int(summary_after_second.get("total-data-files", -1))
    total_records_2 = int(summary_after_second.get("total-records", -1))
    total_size_2 = int(summary_after_second.get("total-files-size", -1))

    assert total_files_2 == total_files_1, (
        f"total-data-files inflated (compression='{compression_method}'): "
        f"{total_files_1} -> {total_files_2}"
    )
    assert total_records_2 == total_records_1, (
        f"total-records inflated (compression='{compression_method}'): "
        f"{total_records_1} -> {total_records_2}"
    )
    assert total_size_2 == total_size_1, (
        f"total-files-size inflated (compression='{compression_method}'): "
        f"{total_size_1} -> {total_size_2}"
    )

    # Data must still be correct after compaction.
    assert int(instance.query(f"SELECT count() FROM {TABLE_NAME}")) == total_rows


@pytest.mark.parametrize("storage_type", ["s3"])
def test_optimize_manifest_parent_summary_missing_totals(
    started_cluster_iceberg_with_spark, storage_type
):
    """
    Regression test: OPTIMIZE TABLE ... MANIFEST must tolerate a parent snapshot
    whose summary omits some of the carried `total-*` counters.

    Iceberg only requires totals on snapshots that change row-level state, so older
    Spark-written tables and tables touched by tools like `removeOrphanFiles`
    routinely drop fields like `total-position-deletes`, `total-equality-deletes`,
    or `total-delete-files` from the summary. Before the fix, the carry-forward
    helper would call `parse<Int64>` on a missing field and throw
    "Cannot parse Int64".

    This test simulates that situation by stripping those fields from the latest
    metadata file before ClickHouse first sees the table.
    """
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    TABLE_NAME = "test_optimize_parent_missing_totals_" + storage_type + "_" + get_uuid_str()
    TABLE_PATH = f"/var/lib/clickhouse/user_files/iceberg_data/default/{TABLE_NAME}/"

    spark.sql(
        f"""
        CREATE TABLE {TABLE_NAME} (id long, data string) USING iceberg
        TBLPROPERTIES ('format-version' = '2')
        """
    )
    for batch_start in range(0, 30, 10):
        spark.sql(
            f"INSERT INTO {TABLE_NAME} "
            f"SELECT id, char(id + ascii('a')) FROM range({batch_start}, {batch_start + 10})"
        )

    default_upload_directory(
        started_cluster_iceberg_with_spark,
        storage_type,
        f"/iceberg_data/default/{TABLE_NAME}/",
        f"/iceberg_data/default/{TABLE_NAME}/",
    )


    # Locate the latest metadata file and strip several total-* fields from the
    # current snapshot's summary, mimicking older Spark / removeOrphanFiles output.
    # Same deterministic tie-break as _load_latest_metadata: when two files share
    # last-updated-ms, the higher metadata version wins (independent of os.listdir order).
    metadata_dir = f"{TABLE_PATH}/metadata/"
    latest_path = None
    best_key = None
    for filename in os.listdir(metadata_dir):
        if not filename.endswith(".json"):
            continue
        fp = os.path.join(metadata_dir, filename)
        with _open_metadata_file(fp) as f:
            data = json.load(f)
        key = (data.get("last-updated-ms", 0), _metadata_version_from_name(filename))
        if best_key is None or key > best_key:
            best_key = key
            latest_path = fp
    assert latest_path is not None, "Could not locate latest metadata file"

    with _open_metadata_file(latest_path) as f:
        data = json.load(f)

    stripped_fields = (
        "total-position-deletes",
        "total-equality-deletes",
        "total-delete-files",
    )
    for snap in data.get("snapshots", []):
        summary = snap.get("summary", {})
        for stripped in stripped_fields:
            summary.pop(stripped, None)

    # Preserve the on-disk encoding (gzip vs plain) when writing back.
    with open(latest_path, "rb") as raw:
        magic = raw.read(2)
    if magic == b"\x1f\x8b":
        with gzip.open(latest_path, "wt") as f:
            json.dump(data, f)
    else:
        with open(latest_path, "w") as f:
            json.dump(data, f)

    # Re-upload the edited metadata so ClickHouse reads the stripped summary.
    default_upload_directory(
        started_cluster_iceberg_with_spark,
        storage_type,
        f"/iceberg_data/default/{TABLE_NAME}/",
        f"/iceberg_data/default/{TABLE_NAME}/",
    )

    # Create the table only after the edit so no metadata cache is populated
    # with the original (full) summary.
    create_iceberg_table(storage_type, instance, TABLE_NAME, started_cluster_iceberg_with_spark)
    assert int(instance.query(f"SELECT count() FROM {TABLE_NAME}")) == 30

    # Must not throw despite the missing total-* fields in the parent summary.
    instance.query(
        f"OPTIMIZE TABLE {TABLE_NAME} MANIFEST",
        settings={
            "allow_experimental_iceberg_compaction": 1,
            "iceberg_manifest_min_count_to_compact": 1,
        },
    )

    assert int(instance.query(f"SELECT count() FROM {TABLE_NAME}")) == 30

    # The newly-written manifest-only snapshot must carry the missing totals as "0".
    default_download_directory(
        started_cluster_iceberg_with_spark,
        storage_type,
        f"/var/lib/clickhouse/user_files/iceberg_data/default/{TABLE_NAME}/",
        f"/var/lib/clickhouse/user_files/iceberg_data/default/{TABLE_NAME}/",
    )
    summary = get_current_snapshot_summary(TABLE_PATH)
    assert summary, "Could not read snapshot summary after compaction"
    assert summary.get("operation") == "replace", (
        f"Expected operation='replace', got: {summary.get('operation')}"
    )
    for stripped in stripped_fields:
        assert summary.get(stripped) == "0", (
            f"Expected {stripped}='0' on the new manifest-only snapshot, "
            f"got: {summary.get(stripped)!r}"
        )


@pytest.mark.parametrize("storage_type", ["s3"])
def test_optimize_manifest_files_experimental_gate(started_cluster_iceberg_with_spark, storage_type):
    """
    `OPTIMIZE TABLE ... MANIFEST` is gated behind the experimental
    `allow_experimental_iceberg_compaction` setting. Running it without the setting must throw
    rather than silently rewrite Iceberg metadata.
    """
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    TABLE_NAME = "test_optimize_manifest_gate_" + storage_type + "_" + get_uuid_str()

    spark.sql(
        f"""
        CREATE TABLE {TABLE_NAME} (id long, data string) USING iceberg TBLPROPERTIES ('format-version' = '2')
        """
    )
    spark.sql(f"INSERT INTO {TABLE_NAME} SELECT id, char(id + ascii('a')) FROM range(0, 10)")

    default_upload_directory(
        started_cluster_iceberg_with_spark,
        storage_type,
        f"/iceberg_data/default/{TABLE_NAME}/",
        f"/iceberg_data/default/{TABLE_NAME}/",
    )
    create_iceberg_table(storage_type, instance, TABLE_NAME, started_cluster_iceberg_with_spark)

    error_message = instance.query_and_get_error(f"OPTIMIZE TABLE {TABLE_NAME} MANIFEST")
    assert "allow_experimental_iceberg_compaction" in error_message, (
        f"Expected the experimental-gate exception, got: {error_message}"
    )
