import glob
import os

import pyarrow.parquet as pq

from helpers.iceberg_utils import (
    create_iceberg_table,
    default_download_directory,
    default_upload_directory,
    get_uuid_str,
)


def _row_group_starts(path):
    metadata = pq.ParquetFile(path).metadata
    starts = []
    for i in range(metadata.num_row_groups):
        column = metadata.row_group(i).column(0)
        starts.append(column.dictionary_page_offset if column.has_dictionary_page else column.data_page_offset)
    return starts


def _spark_split_offsets(rows):
    return {os.path.basename(row.file_path): list(row.split_offsets or []) for row in rows}


def test_writes_split_offsets_read_by_spark(started_cluster_iceberg_with_spark):
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    storage_type = "local"
    TABLE_NAME = "test_writes_split_offsets_" + get_uuid_str()
    local_path = f"/var/lib/clickhouse/user_files/iceberg_data/default/{TABLE_NAME}"

    create_iceberg_table(
        storage_type,
        instance,
        TABLE_NAME,
        started_cluster_iceberg_with_spark,
        "(id Int64, s String)",
        2,
        additional_settings=["output_format_parquet_row_group_size = 1000"],
    )
    instance.query(
        f"INSERT INTO {TABLE_NAME} SELECT number, toString(number) FROM numbers(10000)",
        settings={"allow_insert_into_iceberg": 1, "max_insert_threads": 1},
    )

    default_download_directory(
        started_cluster_iceberg_with_spark,
        storage_type,
        f"{local_path}/",
        f"{local_path}/",
    )

    data_files = glob.glob(f"{local_path}/data/**/*.parquet", recursive=True)
    assert len(data_files) == 1, data_files

    split_offsets = _spark_split_offsets(
        spark.read.format("iceberg").load(f"{local_path}#files").select("file_path", "file_size_in_bytes", "split_offsets").collect()
    )
    expected = _row_group_starts(data_files[0])
    assert len(expected) > 1, expected
    assert split_offsets == {os.path.basename(data_files[0]): expected}


def test_spark_split_offsets_match_clickhouse_row_groups(started_cluster_iceberg_with_spark):
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    storage_type = "local"
    TABLE_NAME = "test_spark_split_offsets_" + get_uuid_str()

    spark.sql(
        f"""
        CREATE TABLE {TABLE_NAME} (id BIGINT, s STRING)
        USING iceberg
        TBLPROPERTIES ('format-version'='2', 'write.parquet.row-group-size-bytes'='2048')
        """
    )
    spark.range(5000).selectExpr("id", "cast(id AS STRING) AS s").coalesce(1).writeTo(TABLE_NAME).append()

    default_upload_directory(
        started_cluster_iceberg_with_spark,
        storage_type,
        f"/iceberg_data/default/{TABLE_NAME}/",
        f"/iceberg_data/default/{TABLE_NAME}/",
    )

    split_offsets = _spark_split_offsets(spark.sql(f"SELECT file_path, file_size_in_bytes, split_offsets FROM spark_catalog.default.{TABLE_NAME}.files").collect())
    assert len(split_offsets) == 1, split_offsets
    assert len(next(iter(split_offsets.values()))) > 1, split_offsets

    clickhouse_offsets = {}
    for line in instance.query(
        f"""
        SELECT _file, arrayMap(rg -> rg.file_offset, row_groups)
        FROM file('iceberg_data/default/{TABLE_NAME}/data/**.parquet', 'ParquetMetadata')
        FORMAT TSV
        """
    ).splitlines():
        file_name, offsets = line.split("\t")
        clickhouse_offsets[file_name] = [int(offset) for offset in offsets.strip("[]").split(",")]

    assert clickhouse_offsets == split_offsets
