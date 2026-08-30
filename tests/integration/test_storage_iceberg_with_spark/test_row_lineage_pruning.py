"""Pruning by the Iceberg v3 row lineage columns (`_row_id`, `_last_updated_sequence_number`).

Both columns are derived from file-level metadata, so a filter on them is answerable from the
manifest alone: a data file holds the row ids `[first_row_id, first_row_id + record_count)` and a
single `_last_updated_sequence_number` (its data sequence number). That makes the incremental "give
me everything changed after sequence number N" read - the way row lineage is meant to be consumed -
a metadata-only file skip, and a `_row_id` point lookup a single-file read.

The trap these tests guard is the copy-on-write rewrite: a file whose rows carry materialized row
ids does NOT hold the contiguous range its manifest entry advertises, so pruning it by that range
drops live rows. See `test_row_lineage.py` for how the values themselves are derived.

NOTE: row lineage is written only by `iceberg-spark-runtime` 1.10.0 and later, see the version
pinned in `ci/docker/integration/runner/Dockerfile`.
"""

import uuid

import pytest

from helpers.iceberg_utils import (
    check_validity_and_get_prunned_files_general,
    default_upload_directory,
    get_creation_expression,
    get_uuid_str,
)

# Both runs must read the same data, so only the metadata-level skip differs between them.
PRUNING_DISABLED = {
    "use_iceberg_partition_pruning": 0,
    "input_format_parquet_filter_push_down": 0,
    "input_format_parquet_bloom_filter_push_down": 0,
}
PRUNING_ENABLED = {
    "use_iceberg_partition_pruning": 1,
    "input_format_parquet_filter_push_down": 0,
    "input_format_parquet_bloom_filter_push_down": 0,
}


def _publish(started_cluster, storage_type, table_name):
    default_upload_directory(
        started_cluster,
        storage_type,
        f"/iceberg_data/default/{table_name}/",
        f"/iceberg_data/default/{table_name}/",
    )


def _table_function(started_cluster, storage_type, table_name):
    return get_creation_expression(
        storage_type, table_name, started_cluster, table_function=True
    )


def _pruned_files(instance, table_name, select_expression):
    """Number of data files skipped by the manifest-level filter, cross-checked for equal results."""
    return check_validity_and_get_prunned_files_general(
        instance,
        table_name,
        PRUNING_DISABLED,
        PRUNING_ENABLED,
        "IcebergMinMaxIndexPrunedFiles",
        select_expression,
    )


def _read_rows(instance, select_expression, settings=None):
    query_id = f"row-lineage-pruning-{uuid.uuid4()}"
    instance.query(select_expression, query_id=query_id, settings=settings)
    instance.query("SYSTEM FLUSH LOGS")
    return int(
        instance.query(
            f"SELECT read_rows FROM system.query_log WHERE query_id = '{query_id}' AND type = 'QueryFinish'"
        )
    )


def _create_table_with_five_appends(spark, table_name, file_format="parquet"):
    """Five appends of ten rows: file k holds row ids [10k, 10k + 10) and sequence number k + 1."""
    spark.sql(
        f"CREATE TABLE {table_name} (id bigint, data string) USING iceberg "
        f"TBLPROPERTIES ('format-version' = '3', 'write.format.default' = '{file_format}')"
    )
    for lo in range(0, 50, 10):
        spark.sql(
            f"INSERT INTO {table_name} select id, 'a' from range({lo}, {lo + 10})"
        )


@pytest.mark.parametrize("storage_type", ["s3"])
def test_row_id_filter_prunes_files(started_cluster_iceberg_with_spark, storage_type):
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    TABLE_NAME = "test_row_id_pruning_" + storage_type + "_" + get_uuid_str()

    _create_table_with_five_appends(spark, TABLE_NAME)
    _publish(started_cluster_iceberg_with_spark, storage_type, TABLE_NAME)
    table_expression = _table_function(
        started_cluster_iceberg_with_spark, storage_type, TABLE_NAME
    )

    assert (
        _pruned_files(instance, TABLE_NAME, f"SELECT id FROM {table_expression} ORDER BY ALL")
        == 0
    )

    # A point lookup touches the one file whose row id range contains the value.
    assert (
        _pruned_files(
            instance,
            TABLE_NAME,
            f"SELECT id FROM {table_expression} WHERE _row_id = 25 ORDER BY ALL",
        )
        == 4
    )

    # A half-open range keeps the two files above it.
    assert (
        _pruned_files(
            instance,
            TABLE_NAME,
            f"SELECT id FROM {table_expression} WHERE _row_id >= 35 ORDER BY ALL",
        )
        == 3
    )

    assert (
        _pruned_files(
            instance,
            TABLE_NAME,
            f"SELECT id FROM {table_expression} WHERE _row_id < 10 ORDER BY ALL",
        )
        == 4
    )

    # A range spanning everything prunes nothing.
    assert (
        _pruned_files(
            instance,
            TABLE_NAME,
            f"SELECT id FROM {table_expression} WHERE _row_id >= 0 ORDER BY ALL",
        )
        == 0
    )


@pytest.mark.parametrize("storage_type", ["s3"])
def test_incremental_read_by_sequence_number_prunes_files(
    started_cluster_iceberg_with_spark, storage_type
):
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    TABLE_NAME = "test_sequence_number_pruning_" + storage_type + "_" + get_uuid_str()

    _create_table_with_five_appends(spark, TABLE_NAME)
    _publish(started_cluster_iceberg_with_spark, storage_type, TABLE_NAME)
    table_expression = _table_function(
        started_cluster_iceberg_with_spark, storage_type, TABLE_NAME
    )

    # "Everything that changed after the sequence number I consumed last time" - the whole point of
    # the column - must not open the files it is going to discard.
    assert (
        _pruned_files(
            instance,
            TABLE_NAME,
            f"SELECT id FROM {table_expression} WHERE _last_updated_sequence_number > 3 ORDER BY ALL",
        )
        == 3
    )

    assert (
        _pruned_files(
            instance,
            TABLE_NAME,
            f"SELECT id FROM {table_expression} WHERE _last_updated_sequence_number = 2 ORDER BY ALL",
        )
        == 4
    )

    assert (
        _pruned_files(
            instance,
            TABLE_NAME,
            f"SELECT id FROM {table_expression} WHERE _last_updated_sequence_number > 0 ORDER BY ALL",
        )
        == 0
    )

    # An incremental consumer reads exactly the rows of the two newest files, not the whole table.
    assert (
        _read_rows(
            instance,
            f"SELECT id FROM {table_expression} WHERE _last_updated_sequence_number > 3 FORMAT Null",
            PRUNING_ENABLED,
        )
        == 20
    )


@pytest.mark.parametrize("storage_type", ["s3"])
def test_pruning_without_column_statistics(
    started_cluster_iceberg_with_spark, storage_type
):
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    TABLE_NAME = "test_row_lineage_pruning_no_stats_" + storage_type + "_" + get_uuid_str()

    # Avro data files carry no per-column statistics, so the manifest cannot tell whether a file
    # materializes row lineage - and materialized values are always carried over from an earlier
    # write, so only the upper bound of the inherited range is known to hold.
    _create_table_with_five_appends(spark, TABLE_NAME, file_format="avro")
    _publish(started_cluster_iceberg_with_spark, storage_type, TABLE_NAME)
    table_expression = _table_function(
        started_cluster_iceberg_with_spark, storage_type, TABLE_NAME
    )

    # An incremental read still skips everything written before the consumed sequence number.
    assert (
        _pruned_files(
            instance,
            TABLE_NAME,
            f"SELECT id FROM {table_expression} WHERE _last_updated_sequence_number > 3 ORDER BY ALL",
        )
        == 3
    )

    # A point lookup keeps every file that could hold an older materialized value, so only the one
    # file whose whole range is below the requested value is skipped.
    assert (
        _pruned_files(
            instance,
            TABLE_NAME,
            f"SELECT id FROM {table_expression} WHERE _last_updated_sequence_number = 2 ORDER BY ALL",
        )
        == 1
    )

    # The same for row ids: everything above the inherited block is still skipped ...
    assert (
        _pruned_files(
            instance,
            TABLE_NAME,
            f"SELECT id FROM {table_expression} WHERE _row_id >= 35 ORDER BY ALL",
        )
        == 3
    )

    # ... while nothing below it is, because an older materialized id could be there.
    assert (
        _pruned_files(
            instance,
            TABLE_NAME,
            f"SELECT id FROM {table_expression} WHERE _row_id < 10 ORDER BY ALL",
        )
        == 0
    )

    assert (
        _pruned_files(
            instance,
            TABLE_NAME,
            f"SELECT id FROM {table_expression} WHERE _row_id = 25 ORDER BY ALL",
        )
        == 2
    )


@pytest.mark.parametrize("storage_type", ["s3"])
def test_row_id_pruning_is_skipped_for_orc(
    started_cluster_iceberg_with_spark, storage_type
):
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    TABLE_NAME = "test_row_lineage_pruning_orc_" + storage_type + "_" + get_uuid_str()

    _create_table_with_five_appends(spark, TABLE_NAME, file_format="orc")
    _publish(started_cluster_iceberg_with_spark, storage_type, TABLE_NAME)
    table_expression = _table_function(
        started_cluster_iceberg_with_spark, storage_type, TABLE_NAME
    )

    # The ORC reader reports no physical row numbers, so `_row_id` is NULL for every row and the
    # inherited range describes nothing: pruning by it would drop the rows this query asks for.
    assert (
        instance.query(
            f"SELECT count() FROM {table_expression} WHERE _row_id IS NULL",
            settings=PRUNING_ENABLED,
        ).strip()
        == "50"
    )

    # The sequence number does not depend on row numbers, so it prunes as usual.
    assert (
        _pruned_files(
            instance,
            TABLE_NAME,
            f"SELECT id FROM {table_expression} WHERE _last_updated_sequence_number > 3 ORDER BY ALL",
        )
        == 3
    )


@pytest.mark.parametrize("storage_type", ["s3"])
def test_materialized_row_ids_are_not_pruned_away(
    started_cluster_iceberg_with_spark, storage_type
):
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    TABLE_NAME = "test_row_id_pruning_materialized_" + storage_type + "_" + get_uuid_str()

    spark.sql(
        f"CREATE TABLE {TABLE_NAME} (id bigint, data string) USING iceberg "
        f"TBLPROPERTIES ('format-version' = '3', 'write.update.mode' = 'copy-on-write')"
    )
    spark.sql(f"INSERT INTO {TABLE_NAME} select id, 'a' from range(0, 4)")
    spark.sql(f"UPDATE {TABLE_NAME} SET data = 'z' WHERE id = 1")

    _publish(started_cluster_iceberg_with_spark, storage_type, TABLE_NAME)
    table_expression = _table_function(
        started_cluster_iceberg_with_spark, storage_type, TABLE_NAME
    )

    # The rewritten file keeps the original row ids 0..3, while its manifest entry advertises the
    # freshly reserved block 4..7. Pruning by that block would drop every live row, so a file with
    # materialized row ids must be read no matter what the filter says.
    for row_key in range(4):
        assert (
            instance.query(
                f"SELECT id FROM {table_expression} WHERE _row_id = {row_key}",
                settings=PRUNING_ENABLED,
            ).strip()
            == str(row_key)
        )

    assert (
        instance.query(
            f"SELECT id FROM {table_expression} WHERE _row_id >= 4 ORDER BY ALL",
            settings=PRUNING_ENABLED,
        ).strip()
        == ""
    )

    # The same for the sequence number: the updated row carries a materialized value of its own.
    assert (
        instance.query(
            f"SELECT id FROM {table_expression} WHERE _last_updated_sequence_number = 1 ORDER BY ALL",
            settings=PRUNING_ENABLED,
        ).strip()
        == "0\n2\n3"
    )
