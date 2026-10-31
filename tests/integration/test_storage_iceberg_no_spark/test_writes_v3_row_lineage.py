import json

import pytest

from avro.datafile import DataFileReader
from avro.io import DatumReader

from helpers.iceberg_utils import (
    create_iceberg_table,
    get_uuid_str,
)


def _metadata_dir(table_name):
    return f"/var/lib/clickhouse/user_files/iceberg_data/default/{table_name}/metadata"


def _latest_metadata(instance, table_name):
    latest = instance.exec_in_container(
        ["bash", "-c", f"ls -v {_metadata_dir(table_name)}/v*.metadata.json | tail -1"]
    ).strip()
    return json.loads(instance.exec_in_container(["cat", latest]))


def _read_avro(instance, remote_path, local_path):
    instance.copy_file_from_container(remote_path, local_path)
    with open(local_path, "rb") as handle:
        reader = DataFileReader(handle, DatumReader())
        records = list(reader)
        schema = json.loads(reader.meta["avro.schema"].decode("utf-8"))
        reader.close()
    return records, schema


def _field_ids(schema_fields):
    return {field["name"]: field.get("field-id") for field in schema_fields}


def _sorted_snapshots(metadata):
    return sorted(metadata["snapshots"], key=lambda s: s["sequence-number"])


@pytest.mark.parametrize("storage_type", ["local"])
def test_v3_row_lineage_written(started_cluster_iceberg_no_spark, storage_type, tmp_path):
    instance = started_cluster_iceberg_no_spark.instances["node1"]
    table_name = "test_v3_row_lineage_" + storage_type + "_" + get_uuid_str()

    create_iceberg_table(
        storage_type,
        instance,
        table_name,
        started_cluster_iceberg_no_spark,
        "(id Int32, s String)",
        format_version=3,
    )

    instance.query(f"INSERT INTO {table_name} VALUES (1, 'a'), (2, 'b')")
    instance.query(f"INSERT INTO {table_name} VALUES (3, 'c'), (4, 'd'), (5, 'e')")

    assert instance.query(f"SELECT count() FROM {table_name}").strip() == "5"

    metadata = _latest_metadata(instance, table_name)
    assert metadata["format-version"] == 3

    snapshots = _sorted_snapshots(metadata)
    assert len(snapshots) == 2

    assert [s["first-row-id"] for s in snapshots] == [0, 2]
    assert [s["added-rows"] for s in snapshots] == [2, 3]
    assert metadata["next-row-id"] == 5

    for snapshot in snapshots:
        assert snapshot["added-rows"] == int(snapshot["summary"]["added-records"])

    manifest_list_path = snapshots[-1]["manifest-list"]
    manifest_list, manifest_list_schema = _read_avro(
        instance, manifest_list_path, str(tmp_path / "manifest_list.avro")
    )

    assert _field_ids(manifest_list_schema["fields"])["first_row_id"] == 520

    ranges = sorted(
        (entry["first_row_id"], entry["added_rows_count"]) for entry in manifest_list
    )
    assert ranges == [(0, 2), (2, 3)]

    next_free = 0
    for first_row_id, added_rows_count in ranges:
        assert first_row_id == next_free
        next_free += added_rows_count
    assert next_free == metadata["next-row-id"]

    for index, entry in enumerate(manifest_list):
        _, manifest_schema = _read_avro(
            instance, entry["manifest_path"], str(tmp_path / f"manifest_{index}.avro")
        )
        data_file_schema = next(
            field for field in manifest_schema["fields"] if field["name"] == "data_file"
        )
        assert _field_ids(data_file_schema["type"]["fields"])["first_row_id"] == 142
