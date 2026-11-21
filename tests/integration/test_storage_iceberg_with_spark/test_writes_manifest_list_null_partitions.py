"""The copy of a parent manifest list into the new manifest list of an INSERT has to keep a null
`partitions` field null and an empty one empty: a table without partition fields has nothing to
summarize, so the field is null, while Java Iceberg writes an empty array for such a table."""

import glob

import avro.datafile
import avro.io
import pytest

from helpers.iceberg_utils import (
    create_iceberg_table,
    default_download_directory,
    get_last_snapshot,
    get_uuid_str,
)

TABLE_ROOT = "/var/lib/clickhouse/user_files/iceberg_data/default"


def _download_table(started_cluster, storage_type, table_name):
    path = f"{TABLE_ROOT}/{table_name}/"
    default_download_directory(started_cluster, storage_type, path, path)
    return path


def _manifest_list_of_last_snapshot(table_path):
    snapshot_id = get_last_snapshot(table_path)
    matches = glob.glob(f"{table_path}/metadata/snap-{snapshot_id}-*.avro")
    assert len(matches) == 1, matches
    return matches[0]


def _partition_summaries_of_last_snapshot(table_path):
    with open(_manifest_list_of_last_snapshot(table_path), "rb") as f:
        return [
            entry["partitions"]
            for entry in avro.datafile.DataFileReader(f, avro.io.DatumReader())
        ]


def _set_partitions_to_empty_array(manifest_list):
    """Rewrite the manifest list file in place so that every entry has `partitions` as a non-null
    empty array; the file-level metadata, including `format-version`, is kept."""
    with open(manifest_list, "rb") as f:
        reader = avro.datafile.DataFileReader(f, avro.io.DatumReader())
        schema = reader.datum_reader.writers_schema
        metadata = dict(reader.meta)
        records = list(reader)
        reader.close()

    for record in records:
        record["partitions"] = []

    with open(manifest_list, "wb") as f:
        writer = avro.datafile.DataFileWriter(f, avro.io.DatumWriter(), schema)
        for key, value in metadata.items():
            if not key.startswith("avro."):
                writer.set_meta(key, value)
        for record in records:
            writer.append(record)
        writer.close()


@pytest.mark.parametrize("format_version", [1, 2])
@pytest.mark.parametrize("storage_type", ["s3", "local"])
def test_writes_manifest_list_null_summary_survives_copy(
    started_cluster_iceberg_with_spark, format_version, storage_type
):
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    TABLE_NAME = (
        "test_writes_manifest_list_null_summary_survives_copy_"
        + storage_type
        + "_"
        + get_uuid_str()
    )

    create_iceberg_table(
        storage_type,
        instance,
        TABLE_NAME,
        started_cluster_iceberg_with_spark,
        "(id Int32)",
        format_version,
        order_by="id",
    )
    # The second INSERT copies the manifest-list entry of the first one: a null `partitions` has to
    # survive the copy as null.
    for values in ("(1), (2)", "(3)"):
        instance.query(
            f"INSERT INTO {TABLE_NAME} VALUES {values};",
            settings={"allow_insert_into_iceberg": 1},
        )

    summaries = _partition_summaries_of_last_snapshot(
        _download_table(started_cluster_iceberg_with_spark, storage_type, TABLE_NAME)
    )

    # A table without partition fields has nothing to summarize, and the field is left null rather
    # than written as an empty list.
    assert summaries == [None, None]


@pytest.mark.parametrize("format_version", [1, 2])
def test_writes_manifest_list_empty_summary_survives_copy(
    started_cluster_iceberg_with_spark, format_version
):
    """A `partitions` that is an empty array, as Java Iceberg writes it for an unpartitioned table, is
    not a null one: the copy of the parent manifest list keeps the two apart, whatever the reader
    gives for the column of an array that is null."""
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    storage_type = "local"
    TABLE_NAME = (
        "test_writes_manifest_list_empty_summary_survives_copy_"
        + storage_type
        + "_"
        + get_uuid_str()
    )

    create_iceberg_table(
        storage_type,
        instance,
        TABLE_NAME,
        started_cluster_iceberg_with_spark,
        "(id Int32)",
        format_version,
        order_by="id",
    )
    instance.query(
        f"INSERT INTO {TABLE_NAME} VALUES (1), (2);",
        settings={"allow_insert_into_iceberg": 1},
    )

    table_path = _download_table(started_cluster_iceberg_with_spark, storage_type, TABLE_NAME)
    manifest_list = _manifest_list_of_last_snapshot(table_path)
    assert _partition_summaries_of_last_snapshot(table_path) == [None]
    _set_partitions_to_empty_array(manifest_list)
    instance.copy_file_to_container(manifest_list, manifest_list)

    # The second INSERT copies the rewritten entry: its empty array has to stay an empty array, while
    # the entry the INSERT adds itself has none.
    instance.query(
        f"INSERT INTO {TABLE_NAME} VALUES (3);",
        settings={"allow_insert_into_iceberg": 1},
    )

    summaries = _partition_summaries_of_last_snapshot(
        _download_table(started_cluster_iceberg_with_spark, storage_type, TABLE_NAME)
    )
    assert summaries == [[], None]
