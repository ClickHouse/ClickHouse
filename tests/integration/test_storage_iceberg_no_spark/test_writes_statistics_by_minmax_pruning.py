import glob
import os

import avro.datafile
import avro.io
import pyarrow.parquet as pq
import pytest

from helpers.iceberg_utils import (
    check_validity_and_get_prunned_files_general,
    create_iceberg_table,
    default_download_directory,
    get_last_snapshot,
    get_uuid_str,
    unescape_path,
)

TABLE_ROOT = "/var/lib/clickhouse/user_files/iceberg_data/default"


@pytest.mark.parametrize("format_version", [1, 2])
@pytest.mark.parametrize("storage_type", ["s3", "azure", "local"])
def test_writes_statistics_by_minmax_pruning(started_cluster_iceberg_no_spark, format_version, storage_type):
    instance = started_cluster_iceberg_no_spark.instances["node1"]
    TABLE_NAME = "test_writes_statistics_by_minmax_pruning_" + storage_type + "_" + get_uuid_str()

    schema = """
    (tag Int32,
    date Date32,
    ts DateTime,
    name String,
    number Int64)
    """
    create_iceberg_table(storage_type, instance, TABLE_NAME, started_cluster_iceberg_no_spark, schema, format_version)

    instance.query(
    f"""
        INSERT INTO {TABLE_NAME} VALUES
        (1, '2024-01-20',
        '2024-02-20 10:00:00',
        'vasya', 5);
    """
    )

    instance.query(
    f"""
        INSERT INTO {TABLE_NAME} VALUES
        (2, '2024-02-20',
        '2024-03-20 15:00:00',
        'vasilisa', 6);
    """
    )

    instance.query(
    f"""
        INSERT INTO {TABLE_NAME} VALUES
        (3, '2025-03-20',
        '2024-04-30 14:00:00',
        'icebreaker', 7);
    """
    )

    instance.query(
    f"""
        INSERT INTO {TABLE_NAME} VALUES
        (4, '2025-04-20',
        '2024-05-30 14:00:00',
        'iceberg', 8);
    """
    )


    def check_validity_and_get_prunned_files(select_expression):
        settings1 = {
            "use_iceberg_partition_pruning": 0,
            "input_format_parquet_bloom_filter_push_down": 0,
            "input_format_parquet_filter_push_down": 0,
        }
        settings2 = {
            "use_iceberg_partition_pruning": 1,
            "input_format_parquet_bloom_filter_push_down": 0,
            "input_format_parquet_filter_push_down": 0,
        }
        return check_validity_and_get_prunned_files_general(
            instance, TABLE_NAME, settings1, settings2, 'IcebergMinMaxIndexPrunedFiles', select_expression
        )

    assert (
        check_validity_and_get_prunned_files(
            f"SELECT * FROM {TABLE_NAME} ORDER BY ALL"
        )
        == 0
    )
    assert (
        check_validity_and_get_prunned_files(
            f"SELECT * FROM {TABLE_NAME} WHERE date <= '2024-01-25' ORDER BY ALL"
        )
        == 3
    )
    assert (
        check_validity_and_get_prunned_files(
            f"SELECT * FROM {TABLE_NAME} WHERE ts <= timestamp('2024-03-20 14:00:00.000000') ORDER BY ALL"
        )
        == 3
    )

    assert (
        check_validity_and_get_prunned_files(
            f"SELECT * FROM {TABLE_NAME} WHERE tag == 1 ORDER BY ALL"
        )
        == 3
    )

    assert (
        check_validity_and_get_prunned_files(
            f"SELECT * FROM {TABLE_NAME} WHERE tag <= 1 ORDER BY ALL"
        )
        == 3
    )

    assert (
        check_validity_and_get_prunned_files(
            f"SELECT * FROM {TABLE_NAME} WHERE name == 'vasilisa' ORDER BY ALL"
        )
        == 3
    )

    assert (
        check_validity_and_get_prunned_files(
            f"SELECT * FROM {TABLE_NAME} WHERE name < 'kek' ORDER BY ALL"
        )
        == 2
    )

    assert (
        check_validity_and_get_prunned_files(
            f"SELECT * FROM {TABLE_NAME} WHERE number == 8 ORDER BY ALL"
        )
        == 3
    )

    assert (
        check_validity_and_get_prunned_files(
            f"SELECT * FROM {TABLE_NAME} WHERE number <= 5 ORDER BY ALL"
        )
        == 3
    )


def _read_avro(path):
    with open(path, "rb") as f:
        return list(avro.datafile.DataFileReader(f, avro.io.DatumReader()))


def _data_files_of_last_snapshot(table_path):
    """The `data_file` record of every manifest entry of the newest snapshot."""
    snapshot_id = get_last_snapshot(table_path)
    manifest_lists = glob.glob(f"{table_path}/metadata/snap-{snapshot_id}-*.avro")
    assert len(manifest_lists) == 1, manifest_lists

    data_files = []
    for list_entry in _read_avro(manifest_lists[0]):
        manifest = os.path.join(
            table_path, "metadata", os.path.basename(unescape_path(list_entry["manifest_path"]))
        )
        data_files.extend(entry["data_file"] for entry in _read_avro(manifest))
    return data_files


def _column_sizes_from_parquet(path):
    """Per-column compressed size as the Parquet footer of one data file reports it, keyed by the
    Iceberg field id the column carries."""
    parquet_file = pq.ParquetFile(path)
    field_ids = [
        int(field.metadata[b"PARQUET:field_id"]) for field in parquet_file.schema_arrow
    ]

    sizes = dict.fromkeys(field_ids, 0)
    metadata = parquet_file.metadata
    for row_group_index in range(metadata.num_row_groups):
        row_group = metadata.row_group(row_group_index)
        assert row_group.num_columns == len(field_ids)
        for column_index, field_id in enumerate(field_ids):
            sizes[field_id] += row_group.column(column_index).total_compressed_size
    return sizes


@pytest.mark.parametrize("format_version", [1, 2])
@pytest.mark.parametrize("storage_type", ["s3", "local"])
def test_writes_column_sizes_are_on_disk_sizes(
    started_cluster_iceberg_no_spark, format_version, storage_type
):
    """`data_file.column_sizes` is the size the column occupies inside the data file, so every
    entry must repeat what the Parquet footer of the file it names says. An in-memory size passes
    neither check below: `s` holds 1000000 bytes of one repeated character, which compresses to a
    fraction of the file, and the sum over a file cannot exceed the file itself."""
    instance = started_cluster_iceberg_no_spark.instances["node1"]
    TABLE_NAME = (
        "test_writes_column_sizes_are_on_disk_sizes_"
        + storage_type
        + "_"
        + get_uuid_str()
    )

    create_iceberg_table(
        storage_type,
        instance,
        TABLE_NAME,
        started_cluster_iceberg_no_spark,
        "(id Int32, s String)",
        format_version,
        order_by="id",
    )
    # Blocks of 1000 rows that the insert pipeline does not squash, so the writer rolls over at
    # exactly 4000 rows and the commit holds three data files of known sizes.
    instance.query(
        f"INSERT INTO {TABLE_NAME} SELECT number, repeat('x', 100) FROM numbers(10000)",
        settings={
            "iceberg_insert_max_rows_in_data_file": 4000,
            "max_block_size": 1000,
            "max_insert_block_size": 1000,
            "min_insert_block_size_rows": 0,
            "min_insert_block_size_bytes": 0,
            "max_insert_threads": 1,
        },
    )

    table_path = f"{TABLE_ROOT}/{TABLE_NAME}/"
    default_download_directory(
        started_cluster_iceberg_no_spark, storage_type, table_path, table_path
    )

    data_files = _data_files_of_last_snapshot(table_path)
    assert len(data_files) == 3

    for data_file in data_files:
        data_path = os.path.join(
            table_path, "data", os.path.basename(unescape_path(data_file["file_path"]))
        )
        written = {pair["key"]: pair["value"] for pair in data_file["column_sizes"]}

        assert written == _column_sizes_from_parquet(data_path)
        assert sum(written.values()) <= data_file["file_size_in_bytes"]

    assert instance.query(f"SELECT count(), uniqExact(s) FROM {TABLE_NAME}") == "10000\t1\n"
