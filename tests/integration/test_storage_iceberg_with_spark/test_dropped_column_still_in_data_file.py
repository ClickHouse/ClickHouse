import json
import os
import re

import pytest

from helpers.iceberg_utils import (
    default_upload_directory,
    get_creation_expression,
    get_uuid_str,
)


# Manifest merging rewrites the pre-drop file's manifest entry into a manifest whose Avro header
# records the post-drop schema id; with the pre-drop snapshot expired that header is the only
# remaining schema hint, so the reader gets the post-drop field id map for a pre-drop file.
MERGE_PROPERTIES = (
    "'format-version' = '2', "
    "'commit.manifest-merge.enabled' = 'true', "
    "'commit.manifest.min-count-to-merge' = '1'"
)
FAR_FUTURE = "2099-12-31 23:59:59"


def _metadata_version_from_name(filename):
    """Return the leading metadata version from `v<N>.metadata.json` (0 if not parseable)."""
    match = re.match(r"v?0*(\d+)", filename)
    return int(match.group(1)) if match else 0


def _snapshot_ids(table_name):
    """Snapshot ids recorded in the table's latest metadata file.

    A tie on `last-updated-ms` is broken by the metadata version, so the answer does not depend on
    os.listdir order.
    """
    metadata_dir = f"/var/lib/clickhouse/user_files/iceberg_data/default/{table_name}/metadata"
    best = None
    best_key = None
    for filename in os.listdir(metadata_dir):
        if not filename.endswith(".json"):
            continue
        with open(os.path.join(metadata_dir, filename)) as f:
            data = json.load(f)
        key = (data.get("last-updated-ms", 0), _metadata_version_from_name(filename))
        if best_key is None or key > best_key:
            best_key = key
            best = data
    return {snap.get("snapshot-id") for snap in (best or {}).get("snapshots", [])}


def _prepare(spark, table_name, readd_dropped_name):
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            id INT NOT NULL,
            legacy_col STRING
        ) using iceberg
        TBLPROPERTIES ({MERGE_PROPERTIES});
        """
    )
    spark.sql(f"INSERT INTO {table_name} VALUES (1, 'x'), (2, 'y'), (3, 'z');")
    spark.sql(f"ALTER TABLE {table_name} DROP COLUMN legacy_col;")
    if readd_dropped_name:
        # Spark assigns a NEW field id to the re-added column. The old data file still holds the
        # dropped column under its original id and the SAME name.
        spark.sql(f"ALTER TABLE {table_name} ADD COLUMN legacy_col STRING;")
        spark.sql(f"INSERT INTO {table_name} VALUES (4, 'NEW4'), (5, 'NEW5');")
    else:
        spark.sql(f"INSERT INTO {table_name} VALUES (4), (5);")
    spark.sql(f"CALL system.rewrite_manifests('{table_name}')")
    before = _snapshot_ids(table_name)
    assert len(before) >= 2, (
        f"expected at least the pre-drop and post-drop snapshots before expiry, got {before}; "
        "the fixture no longer reproduces the bug"
    )
    spark.sql(
        f"CALL system.expire_snapshots("
        f"table => '{table_name}', "
        f"older_than => TIMESTAMP '{FAR_FUTURE}', "
        f"retain_last => 1)"
    )
    after = _snapshot_ids(table_name)
    assert len(after) == 1 and after < before, (
        f"expire_snapshots did not drop the pre-drop snapshot ({before} -> {after}); the fixture "
        "no longer reproduces the bug, because the pre-drop snapshot still resolves the old "
        "file's schema"
    )


@pytest.mark.parametrize("storage_type", ["s3", "local"])
def test_read_data_file_holding_dropped_column(
    started_cluster_iceberg_with_spark, storage_type
):
    """A data file written before DROP COLUMN stays readable."""
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    table_name = "test_dropped_column_" + storage_type + "_" + get_uuid_str()

    _prepare(spark, table_name, readd_dropped_name=False)
    default_upload_directory(
        started_cluster_iceberg_with_spark,
        storage_type,
        f"/iceberg_data/default/{table_name}/",
        f"/iceberg_data/default/{table_name}/",
    )
    table_expression = get_creation_expression(
        storage_type,
        table_name,
        started_cluster_iceberg_with_spark,
        table_function=True,
    )

    # count() is answered from metadata without opening a data file, so it cannot detect this.
    assert instance.query(f"SELECT max(id) FROM {table_expression}") == "5\n"
    assert (
        instance.query(f"SELECT id FROM {table_expression} ORDER BY id")
        == "1\n2\n3\n4\n5\n"
    )


@pytest.mark.parametrize("storage_type", ["s3", "local"])
def test_dropped_column_name_reused(started_cluster_iceberg_with_spark, storage_type):
    """Re-adding the dropped column's NAME must not resurrect the dropped column's values.

    The re-added column has a different field id, so it is absent from the pre-drop data file and
    reads as NULL there. Matching the file column by its physical name instead would serve
    'x'/'y'/'z'.
    """
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    table_name = "test_dropped_column_reused_" + storage_type + "_" + get_uuid_str()

    _prepare(spark, table_name, readd_dropped_name=True)
    default_upload_directory(
        started_cluster_iceberg_with_spark,
        storage_type,
        f"/iceberg_data/default/{table_name}/",
        f"/iceberg_data/default/{table_name}/",
    )
    table_expression = get_creation_expression(
        storage_type,
        table_name,
        started_cluster_iceberg_with_spark,
        table_function=True,
    )

    assert (
        instance.query(
            f"SELECT id, ifNull(legacy_col, 'NULL') FROM {table_expression} ORDER BY id"
        )
        == "1\tNULL\n2\tNULL\n3\tNULL\n4\tNEW4\n5\tNEW5\n"
    )
