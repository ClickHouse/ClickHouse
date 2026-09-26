"""Every manifest that an INSERT or a DELETE writes holds exactly one partition tuple, so its
manifest-list entry must carry a field summary pinned to that tuple: `lower_bound == upper_bound`
per partition field. Without the summary a reader cannot skip a manifest by partition and has to
open all of them."""

import glob
import os
import struct

import avro.datafile
import avro.io
import pytest

from helpers.iceberg_utils import (
    create_iceberg_table,
    default_download_directory,
    get_last_snapshot,
    get_uuid_str,
    unescape_path,
)

TABLE_ROOT = "/var/lib/clickhouse/user_files/iceberg_data/default"


def _download_table(started_cluster, storage_type, table_name):
    path = f"{TABLE_ROOT}/{table_name}/"
    default_download_directory(started_cluster, storage_type, path, path)
    return path


def _read_avro(path):
    with open(path, "rb") as f:
        return list(avro.datafile.DataFileReader(f, avro.io.DatumReader()))


def _manifest_list_of_last_snapshot(table_path):
    snapshot_id = get_last_snapshot(table_path)
    matches = glob.glob(f"{table_path}/metadata/snap-{snapshot_id}-*.avro")
    assert len(matches) == 1, matches
    return matches[0]


def _encode_bound(value):
    """A single-value bound as Iceberg serializes it: UTF-8 for a string, little-endian two's
    complement for an int."""
    if isinstance(value, str):
        return value.encode()
    return struct.pack("<i", value)


def _entries_of_last_snapshot(table_path):
    """Every manifest-list entry of the newest snapshot, paired with the partition tuple stored
    inside the manifest it points at. Pairing by manifest path is what makes a summary attached to
    the wrong entry visible."""
    entries = []
    for entry in _read_avro(_manifest_list_of_last_snapshot(table_path)):
        manifest = os.path.join(
            table_path, "metadata", os.path.basename(unescape_path(entry["manifest_path"]))
        )
        partition = {}
        for record in _read_avro(manifest):
            partition.update(record["data_file"]["partition"])
        entries.append((entry.get("content", 0), partition, entry["partitions"]))
    return entries


def _summary_tuples(summary):
    return [
        (
            field["contains_null"],
            field["contains_nan"],
            field["lower_bound"],
            field["upper_bound"],
        )
        for field in summary
    ]


def _expected_summary(partition):
    return [
        (False, None, _encode_bound(value), _encode_bound(value))
        for value in partition.values()
    ]


@pytest.mark.parametrize("format_version", [1, 2])
@pytest.mark.parametrize("storage_type", ["s3", "local"])
def test_writes_manifest_list_partition_summary(
    started_cluster_iceberg_no_spark, format_version, storage_type
):
    instance = started_cluster_iceberg_no_spark.instances["node1"]
    TABLE_NAME = (
        "test_writes_manifest_list_partition_summary_"
        + storage_type
        + "_"
        + get_uuid_str()
    )

    # Two partition fields of different types: the summary must list them in spec order, with a
    # string bound written as UTF-8 and an int bound as little-endian bytes.
    create_iceberg_table(
        storage_type,
        instance,
        TABLE_NAME,
        started_cluster_iceberg_no_spark,
        "(region String, part Int32, id Int32)",
        format_version,
        partition_by="(region, part)",
        order_by="id",
    )
    instance.query(
        f"INSERT INTO {TABLE_NAME} VALUES ('eu', 10, 1), ('eu', 10, 2), ('us', 11, 3);",
        settings={"allow_insert_into_iceberg": 1},
    )

    entries = _entries_of_last_snapshot(
        _download_table(started_cluster_iceberg_no_spark, storage_type, TABLE_NAME)
    )

    assert len(entries) == 2
    assert sorted(partition["region"] for _, partition, _ in entries) == ["eu", "us"]
    for _, partition, summary in entries:
        assert summary is not None
        assert _summary_tuples(summary) == _expected_summary(partition)


@pytest.mark.parametrize("format_version", [1, 2])
@pytest.mark.parametrize("storage_type", ["s3", "local"])
def test_writes_manifest_list_no_summary_when_unpartitioned(
    started_cluster_iceberg_no_spark, format_version, storage_type
):
    instance = started_cluster_iceberg_no_spark.instances["node1"]
    TABLE_NAME = (
        "test_writes_manifest_list_no_summary_when_unpartitioned_"
        + storage_type
        + "_"
        + get_uuid_str()
    )

    create_iceberg_table(
        storage_type,
        instance,
        TABLE_NAME,
        started_cluster_iceberg_no_spark,
        "(id Int32)",
        format_version,
        order_by="id",
    )
    instance.query(
        f"INSERT INTO {TABLE_NAME} VALUES (1), (2);",
        settings={"allow_insert_into_iceberg": 1},
    )

    entries = _entries_of_last_snapshot(
        _download_table(started_cluster_iceberg_no_spark, storage_type, TABLE_NAME)
    )

    # A table without partition fields has nothing to summarize, and the field is left null rather
    # than written as an empty list.
    assert [summary for _, _, summary in entries] == [None]


@pytest.mark.parametrize("storage_type", ["s3", "local"])
def test_writes_manifest_list_partition_summary_after_delete(
    started_cluster_iceberg_no_spark, storage_type
):
    instance = started_cluster_iceberg_no_spark.instances["node1"]
    TABLE_NAME = (
        "test_writes_manifest_list_partition_summary_after_delete_"
        + storage_type
        + "_"
        + get_uuid_str()
    )

    create_iceberg_table(
        storage_type,
        instance,
        TABLE_NAME,
        started_cluster_iceberg_no_spark,
        "(region String, id Int32)",
        2,
        partition_by="(region)",
        order_by="id",
    )
    instance.query(
        f"INSERT INTO {TABLE_NAME} VALUES ('eu', 1), ('us', 2);",
        settings={"allow_insert_into_iceberg": 1},
    )
    instance.query(
        f"ALTER TABLE {TABLE_NAME} DELETE WHERE id = 1;",
        settings={"allow_insert_into_iceberg": 1},
    )

    entries = _entries_of_last_snapshot(
        _download_table(started_cluster_iceberg_no_spark, storage_type, TABLE_NAME)
    )

    # The DELETE writes a position-delete manifest of its own and copies the data manifests of the
    # parent snapshot: both kinds of entry must come out with bounds.
    assert sorted(content for content, _, _ in entries) == [0, 0, 1]
    for _, partition, summary in entries:
        assert summary is not None
        assert _summary_tuples(summary) == _expected_summary(partition)

    assert instance.query(f"SELECT * FROM {TABLE_NAME} ORDER BY ALL") == "us\t2\n"
