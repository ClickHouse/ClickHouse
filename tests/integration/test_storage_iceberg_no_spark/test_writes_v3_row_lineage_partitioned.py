import glob
import json
import os

import avro.datafile
import avro.io
import pytest

from helpers.iceberg_utils import (
    create_iceberg_table,
    default_download_directory,
    get_uuid_str,
)

TABLE_ROOT = "/var/lib/clickhouse/user_files/iceberg_data/default"


def _download_table(started_cluster, storage_type, table_name):
    path = f"{TABLE_ROOT}/{table_name}/"
    default_download_directory(started_cluster, storage_type, path, path)
    return path


def _read_avro(path):
    with open(path, "rb") as f:
        return list(avro.datafile.DataFileReader(f, avro.io.DatumReader()))


def _latest_metadata(table_path):
    names = glob.glob(f"{table_path}/metadata/v*.metadata.json")
    latest = max(names, key=lambda name: int(os.path.basename(name)[1:].split(".")[0]))
    with open(latest) as f:
        return json.load(f)


def _manifest_list_of(table_path, snapshot_id):
    matches = glob.glob(f"{table_path}/metadata/snap-{snapshot_id}-*.avro")
    assert len(matches) == 1, matches
    return matches[0]


@pytest.mark.parametrize("storage_type", ["s3", "local"])
def test_writes_v3_row_lineage_partitioned(started_cluster_iceberg_no_spark, storage_type):
    instance = started_cluster_iceberg_no_spark.instances["node1"]
    TABLE_NAME = (
        "test_writes_v3_row_lineage_partitioned_" + storage_type + "_" + get_uuid_str()
    )

    create_iceberg_table(
        storage_type,
        instance,
        TABLE_NAME,
        started_cluster_iceberg_no_spark,
        "(part Int32, id Int32)",
        format_version=3,
        partition_by="part",
        order_by="id",
    )

    instance.query(
        f"INSERT INTO {TABLE_NAME} VALUES (1, 0), (1, 1), (1, 2), (2, 3), (2, 4)",
        settings={"allow_insert_into_iceberg": 1},
    )
    instance.query(
        f"INSERT INTO {TABLE_NAME} VALUES (1, 5), (2, 6), (2, 7), (2, 8)",
        settings={"allow_insert_into_iceberg": 1},
    )

    table_path = _download_table(
        started_cluster_iceberg_no_spark, storage_type, TABLE_NAME
    )
    metadata = _latest_metadata(table_path)

    assert metadata["format-version"] == 3
    assert metadata["next-row-id"] == 9

    snapshots = sorted(metadata["snapshots"], key=lambda s: s["sequence-number"])
    assert [snapshot["first-row-id"] for snapshot in snapshots] == [0, 5]
    assert [snapshot["added-rows"] for snapshot in snapshots] == [5, 4]

    entries = _read_avro(_manifest_list_of(table_path, metadata["current-snapshot-id"]))
    assert len(entries) == 4

    ranges_by_snapshot = {}
    for entry in entries:
        ranges_by_snapshot.setdefault(entry["added_snapshot_id"], []).append(
            (entry["first_row_id"], entry["added_rows_count"])
        )

    for snapshot in snapshots:
        ranges = sorted(ranges_by_snapshot[snapshot["snapshot-id"]])
        assert len(ranges) == 2, ranges
        next_free = snapshot["first-row-id"]
        for first_row_id, added_rows_count in ranges:
            assert first_row_id == next_free
            next_free += added_rows_count
        assert next_free == snapshot["first-row-id"] + snapshot["added-rows"]

    assert instance.query(
        f"SELECT _row_id FROM {TABLE_NAME} ORDER BY _row_id FORMAT TSV"
    ).split() == [str(row_id) for row_id in range(9)]
