"""Pruning by the Iceberg v3 row lineage columns (`_row_id`, `_last_updated_sequence_number`).

Both columns are derived from file-level metadata: a data file inherits the row ids
`[first_row_id, first_row_id + record_count)` and a single `_last_updated_sequence_number` (its data
sequence number). That makes the incremental "give me everything changed after sequence number N"
read - the way row lineage is meant to be consumed - a metadata-only file skip.

The trap these tests guard is the copy-on-write rewrite: a file whose rows carry materialized row
ids does NOT hold the contiguous range its manifest entry advertises, so pruning it by that range
drops live rows. A file is pruned by a lineage column only when its manifest declares bounds for
that column, widened by the inherited values when some rows may store NULL; a file without such
statistics is not pruned by it. See `test_row_lineage.py` for how the values themselves are derived.

NOTE: row lineage is written only by `iceberg-spark-runtime` 1.10.0 and later, see the version
pinned in `ci/docker/integration/runner/Dockerfile`.
"""

import json
import os

import avro.datafile
import avro.io
import pytest

from helpers.iceberg_utils import (
    check_validity_and_get_prunned_files_general,
    create_iceberg_table,
    default_upload_directory,
    get_creation_expression,
    get_uuid_str,
)

# The reserved field ids of `_row_id` and `_last_updated_sequence_number`, which a manifest uses to
# key statistics about them.
ROW_ID_FIELD_ID = 2147483540
LAST_UPDATED_SEQUENCE_NUMBER_FIELD_ID = 2147483539

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
def test_row_id_filter_keeps_files_without_lineage_bounds(
    started_cluster_iceberg_with_spark, storage_type
):
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

    # Spark writes no lineage statistics for new rows, so nothing bounds the row ids a file may store
    # and no file is skipped.
    assert (
        _pruned_files(
            instance,
            TABLE_NAME,
            f"SELECT id FROM {table_expression} WHERE _row_id = 25 ORDER BY ALL",
        )
        == 0
    )

    assert (
        _pruned_files(
            instance,
            TABLE_NAME,
            f"SELECT id FROM {table_expression} WHERE _row_id >= 35 ORDER BY ALL",
        )
        == 0
    )

    assert (
        _pruned_files(
            instance,
            TABLE_NAME,
            f"SELECT id FROM {table_expression} WHERE _row_id < 10 ORDER BY ALL",
        )
        == 0
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
def test_incremental_read_by_sequence_number_keeps_files_without_lineage_bounds(
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

    # "Everything that changed after the sequence number I consumed last time": a file without
    # lineage statistics may store any sequence number, so it is read.
    assert (
        _pruned_files(
            instance,
            TABLE_NAME,
            f"SELECT id FROM {table_expression} WHERE _last_updated_sequence_number > 3 ORDER BY ALL",
        )
        == 0
    )

    assert (
        _pruned_files(
            instance,
            TABLE_NAME,
            f"SELECT id FROM {table_expression} WHERE _last_updated_sequence_number = 2 ORDER BY ALL",
        )
        == 0
    )

    assert (
        _pruned_files(
            instance,
            TABLE_NAME,
            f"SELECT id FROM {table_expression} WHERE _last_updated_sequence_number > 0 ORDER BY ALL",
        )
        == 0
    )


@pytest.mark.parametrize("storage_type", ["s3"])
def test_pruning_without_column_statistics(
    started_cluster_iceberg_with_spark, storage_type
):
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    TABLE_NAME = "test_row_lineage_pruning_no_stats_" + storage_type + "_" + get_uuid_str()

    # Avro data files carry no per-column statistics, so the manifest cannot tell whether a file
    # materializes row lineage, nor which values: no file is pruned by a lineage column.
    _create_table_with_five_appends(spark, TABLE_NAME, file_format="avro")
    _publish(started_cluster_iceberg_with_spark, storage_type, TABLE_NAME)
    table_expression = _table_function(
        started_cluster_iceberg_with_spark, storage_type, TABLE_NAME
    )

    assert (
        _pruned_files(
            instance,
            TABLE_NAME,
            f"SELECT id FROM {table_expression} WHERE _last_updated_sequence_number > 3 ORDER BY ALL",
        )
        == 0
    )

    assert (
        _pruned_files(
            instance,
            TABLE_NAME,
            f"SELECT id FROM {table_expression} WHERE _last_updated_sequence_number = 2 ORDER BY ALL",
        )
        == 0
    )

    assert (
        _pruned_files(
            instance,
            TABLE_NAME,
            f"SELECT id FROM {table_expression} WHERE _row_id >= 35 ORDER BY ALL",
        )
        == 0
    )

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
        == 0
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

    # Rows of ORC data files inherit their row ids, but ORC files are not skipped by `_row_id` bounds.
    assert (
        instance.query(
            f"SELECT id, _row_id FROM {table_expression} WHERE _row_id IN (0, 25, 49) ORDER BY id",
            settings=PRUNING_ENABLED,
        )
        == "0\t0\n25\t25\n49\t49\n"
    )
    assert (
        _pruned_files(
            instance,
            TABLE_NAME,
            f"SELECT id FROM {table_expression} WHERE _row_id = 25 ORDER BY ALL",
        )
        == 0
    )

    # The sequence number does not depend on row numbers, but without lineage statistics it does not
    # prune either.
    assert (
        _pruned_files(
            instance,
            TABLE_NAME,
            f"SELECT id FROM {table_expression} WHERE _last_updated_sequence_number > 3 ORDER BY ALL",
        )
        == 0
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

    # The manifest declares the materialized ids 0..3 for this file with no nulls, so the filter above
    # skips it rather than reading it and finding nothing: this count is what pins the declared
    # bounds as being used at all.
    assert (
        _pruned_files(
            instance,
            TABLE_NAME,
            f"SELECT id FROM {table_expression} WHERE _row_id >= 4 ORDER BY ALL",
        )
        == 1
    )

    # The same for the sequence number: the updated row carries a materialized value of its own.
    assert (
        instance.query(
            f"SELECT id FROM {table_expression} WHERE _last_updated_sequence_number = 1 ORDER BY ALL",
            settings=PRUNING_ENABLED,
        ).strip()
        == "0\n2\n3"
    )

    # The file stores the sequence number 1 for its untouched rows and NULL for the updated one, which
    # inherits 2: the declared bounds [1, 1] widened by the inherited value still skip the file here.
    assert (
        _pruned_files(
            instance,
            TABLE_NAME,
            f"SELECT id FROM {table_expression} WHERE _last_updated_sequence_number > 2 ORDER BY ALL",
        )
        == 1
    )

    # The statistics of the stored column in the Parquet file do not cover the inherited value either,
    # so the format must not skip row groups by them.
    for condition in ("= 2", "> 1"):
        assert (
            instance.query(
                f"SELECT id FROM {table_expression} WHERE _last_updated_sequence_number {condition}",
                settings={
                    "use_iceberg_partition_pruning": 1,
                    "input_format_parquet_filter_push_down": 1,
                },
            ).strip()
            == "1"
        )


def _current_manifest_list(table_name):
    """Path of the current snapshot's manifest list, on the filesystem Spark just wrote it to."""
    metadata_dir = f"/var/lib/clickhouse/user_files/iceberg_data/default/{table_name}/metadata"
    with open(os.path.join(metadata_dir, "version-hint.text")) as f:
        version = f.read().strip()
    with open(os.path.join(metadata_dir, f"v{version}.metadata.json")) as f:
        metadata = json.load(f)
    snapshot = next(
        entry
        for entry in metadata["snapshots"]
        if entry["snapshot-id"] == metadata["current-snapshot-id"]
    )
    return snapshot["manifest-list"]


def _rewrite_avro(path, patch_record):
    """Rewrite an avro file in place with `patch_record` applied to each record, preserving the
    writer schema, the codec and the Iceberg file metadata. Returns how many records it changed, so
    that a field the writer did not produce fails the test instead of silently patching nothing."""
    with open(path, "rb") as f:
        reader = avro.datafile.DataFileReader(f, avro.io.DatumReader())
        schema = reader.datum_reader.writers_schema
        file_meta = dict(reader.meta)
        codec = reader.codec
        records = list(reader)
        reader.close()

    patched = sum(1 for record in records if patch_record(record))
    if patched:
        with open(path, "wb") as f:
            writer = avro.datafile.DataFileWriter(
                f, avro.io.DatumWriter(), schema, codec=codec
            )
            for key, value in file_meta.items():
                if not key.startswith("avro."):
                    writer.set_meta(key, value)
            for record in records:
                writer.append(record)
            writer.close()
    return patched


def _manifest_holding_row_id(table_name, row_key):
    """Path of the manifest describing the data file whose row id block contains `row_key`. The
    block a writer reserved for a file is `[first_row_id, first_row_id + added_rows_count)`."""
    with open(_current_manifest_list(table_name), "rb") as f:
        reader = avro.datafile.DataFileReader(f, avro.io.DatumReader())
        entries = list(reader)
        reader.close()
    for entry in entries:
        first_row_id = entry["first_row_id"]
        if first_row_id <= row_key < first_row_id + entry["added_rows_count"]:
            return entry["manifest_path"]
    raise AssertionError(f"No manifest holds row id {row_key} in {table_name}")


@pytest.mark.parametrize("storage_type", ["s3"])
def test_inverted_declared_row_id_bounds_do_not_prune(
    started_cluster_iceberg_with_spark, storage_type
):
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    TABLE_NAME = "test_row_lineage_inverted_bounds_" + storage_type + "_" + get_uuid_str()

    _create_table_with_five_appends(spark, TABLE_NAME)

    def declare_inverted_row_id_bounds(record):
        data_file = record["data_file"]
        # Statistics that place the lower bound above the upper bound describe no value at all, so
        # nothing may be pruned on them: a writer that emits them is not to be trusted about this
        # column, and both swapping and clamping the pair invent a range it never declared.
        data_file["lower_bounds"].append(
            {"key": ROW_ID_FIELD_ID, "value": (100).to_bytes(8, "little")}
        )
        data_file["upper_bounds"].append(
            {"key": ROW_ID_FIELD_ID, "value": (50).to_bytes(8, "little")}
        )
        # With no nulls, the declared pair alone would bound the column.
        data_file["null_value_counts"].append({"key": ROW_ID_FIELD_ID, "value": 0})
        return True

    assert (
        _rewrite_avro(
            _manifest_holding_row_id(TABLE_NAME, 25), declare_inverted_row_id_bounds
        )
        == 1
    )

    _publish(started_cluster_iceberg_with_spark, storage_type, TABLE_NAME)
    table_expression = _table_function(
        started_cluster_iceberg_with_spark, storage_type, TABLE_NAME
    )

    # No file declares usable lineage bounds, so none is skipped. Swapping the pair instead would
    # exclude 25 and skip the file holding it, and pruning on the pair as declared would skip it too,
    # both losing the row the filter asks for.
    assert (
        _pruned_files(
            instance,
            TABLE_NAME,
            f"SELECT id FROM {table_expression} WHERE _row_id = 25 ORDER BY ALL",
        )
        == 0
    )

    assert (
        instance.query(
            f"SELECT id FROM {table_expression} WHERE _row_id = 25",
            settings=PRUNING_ENABLED,
        ).strip()
        == "25"
    )


@pytest.mark.parametrize("storage_type", ["s3"])
def test_unrepresentable_row_id_block_does_not_prune(
    started_cluster_iceberg_with_spark, storage_type
):
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    TABLE_NAME = "test_row_lineage_wrapped_block_" + storage_type + "_" + get_uuid_str()

    spark.sql(
        f"CREATE TABLE {TABLE_NAME} (id bigint, data string) USING iceberg "
        f"TBLPROPERTIES ('format-version' = '3', 'write.format.default' = 'parquet')"
    )
    for lo in range(0, 20, 10):
        spark.sql(
            f"INSERT INTO {TABLE_NAME} select id, 'a' from range({lo}, {lo + 10})"
        )

    def declare_row_id_bounds_with_nulls(record):
        # Declared bounds with nulls are widened by the inherited block, so they make the block
        # matter for this file.
        data_file = record["data_file"]
        data_file["lower_bounds"].append(
            {"key": ROW_ID_FIELD_ID, "value": (0).to_bytes(8, "little")}
        )
        data_file["upper_bounds"].append(
            {"key": ROW_ID_FIELD_ID, "value": (0).to_bytes(8, "little")}
        )
        data_file["null_value_counts"].append({"key": ROW_ID_FIELD_ID, "value": 1})
        return True

    assert (
        _rewrite_avro(
            _manifest_holding_row_id(TABLE_NAME, 0), declare_row_id_bounds_with_nulls
        )
        == 1
    )

    def declare_negative_first_row_id(record):
        # `first_row_id` is a signed long on the wire but a row id is unsigned, so a negative value
        # names a block starting just below the top of the range whose end no longer fits in it.
        # The rows the file really holds are then outside the block, and pruning by it drops them.
        if record["first_row_id"] != 0:
            return False
        record["first_row_id"] = -1
        return True

    assert (
        _rewrite_avro(_current_manifest_list(TABLE_NAME), declare_negative_first_row_id)
        == 1
    )

    _publish(started_cluster_iceberg_with_spark, storage_type, TABLE_NAME)
    table_expression = _table_function(
        started_cluster_iceberg_with_spark, storage_type, TABLE_NAME
    )

    # The file whose block does not fit is read whatever its declared bounds say: widened by the
    # wrapped block they would become [0, 8] and skip it, while its first row has row id 2^64 - 1.
    # The second file declares no lineage bounds and is read too.
    assert (
        _pruned_files(
            instance,
            TABLE_NAME,
            f"SELECT id FROM {table_expression} WHERE _row_id >= 20 ORDER BY ALL",
        )
        == 0
    )

    # A row id is assigned as `first_row_id` plus the row's position, so the block starting one
    # below zero shifts every id in the file down by one: row id 5 is the seventh row, `id` 6.
    # Pruning that file on its block would drop this row from a filter that matches it.
    assert (
        instance.query(
            f"SELECT id FROM {table_expression} WHERE _row_id = 5",
            settings=PRUNING_ENABLED,
        ).strip()
        == "6"
    )


@pytest.mark.parametrize("storage_type", ["s3"])
def test_materialized_row_lineage_without_statistics_is_not_pruned_away(
    started_cluster_iceberg_with_spark, storage_type
):
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    TABLE_NAME = "test_row_lineage_no_lineage_stats_" + storage_type + "_" + get_uuid_str()

    # The rewritten file of `test_materialized_row_ids_are_not_pruned_away`: it keeps the row ids 0..3
    # and the sequence number 1 of its untouched rows, below its inherited block 4..7 and number 2.
    spark.sql(
        f"CREATE TABLE {TABLE_NAME} (id bigint, data string) USING iceberg "
        f"TBLPROPERTIES ('format-version' = '3', 'write.update.mode' = 'copy-on-write')"
    )
    spark.sql(f"INSERT INTO {TABLE_NAME} select id, 'a' from range(0, 4)")
    spark.sql(f"UPDATE {TABLE_NAME} SET data = 'z' WHERE id = 1")

    lineage_field_ids = (ROW_ID_FIELD_ID, LAST_UPDATED_SEQUENCE_NUMBER_FIELD_ID)
    sizes_left_for_table_columns = []

    def drop_row_lineage_statistics(record):
        # Every metric map is optional and a missing id means "unknown", so a writer may keep the
        # statistics of the table columns and leave out those of the reserved lineage columns.
        data_file = record["data_file"]
        dropped = False
        for name in (
            "column_sizes",
            "value_counts",
            "null_value_counts",
            "nan_value_counts",
            "lower_bounds",
            "upper_bounds",
        ):
            stats = data_file.get(name)
            if stats is None:
                continue
            kept = [item for item in stats if item["key"] not in lineage_field_ids]
            dropped = dropped or len(kept) != len(stats)
            data_file[name] = kept
        if dropped:
            sizes_left_for_table_columns.append(len(data_file["column_sizes"] or []))
        return dropped

    with open(_current_manifest_list(TABLE_NAME), "rb") as f:
        reader = avro.datafile.DataFileReader(f, avro.io.DatumReader())
        data_manifests = [
            entry["manifest_path"] for entry in reader if entry["content"] == 0
        ]
        reader.close()

    # Only the rewritten file has lineage statistics to drop, and it must keep sizes for its table
    # columns: the manifest then knows about some columns of the file, but nothing about lineage.
    assert (
        sum(_rewrite_avro(path, drop_row_lineage_statistics) for path in data_manifests)
        == 1
    )
    assert sizes_left_for_table_columns[0] > 0

    _publish(started_cluster_iceberg_with_spark, storage_type, TABLE_NAME)
    table_expression = _table_function(
        started_cluster_iceberg_with_spark, storage_type, TABLE_NAME
    )

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
            f"SELECT id FROM {table_expression} WHERE _last_updated_sequence_number = 1 ORDER BY ALL",
            settings=PRUNING_ENABLED,
        ).strip()
        == "0\n2\n3"
    )

    assert (
        _pruned_files(
            instance,
            TABLE_NAME,
            f"SELECT id FROM {table_expression} WHERE _row_id = 1 ORDER BY ALL",
        )
        == 0
    )

    # Without lineage statistics nothing bounds the stored values, so a filter above the inherited
    # block does not prune the file either.
    assert (
        _pruned_files(
            instance,
            TABLE_NAME,
            f"SELECT id FROM {table_expression} WHERE _row_id >= 8 ORDER BY ALL",
        )
        == 0
    )

    assert (
        _pruned_files(
            instance,
            TABLE_NAME,
            f"SELECT id FROM {table_expression} WHERE _last_updated_sequence_number > 2 ORDER BY ALL",
        )
        == 0
    )


# The tests above read what Spark wrote. The ones below replay the same filters over metadata
# ClickHouse wrote itself, which declares no lineage statistics either, so no file is skipped.
INSERT_SETTINGS = {"allow_insert_into_iceberg": 1}


def _clickhouse_table_with_five_appends(
    started_cluster, storage_type, table_name, schema="(id Int32, s String)", payload="'a'"
):
    """Five inserts of ten rows: file k holds row ids [10k, 10k + 10) and sequence number k + 1."""
    instance = started_cluster.instances["node1"]
    create_iceberg_table(
        storage_type,
        instance,
        table_name,
        started_cluster,
        schema,
        format_version=3,
    )
    for lo in range(0, 50, 10):
        instance.query(
            f"INSERT INTO {table_name} SELECT number, {payload} FROM numbers({lo}, 10)",
            settings=INSERT_SETTINGS,
        )


@pytest.mark.parametrize("storage_type", ["s3"])
def test_row_id_filter_keeps_files_without_lineage_bounds_clickhouse(
    started_cluster_iceberg_with_spark, storage_type
):
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    TABLE_NAME = "test_row_id_pruning_clickhouse_" + storage_type + "_" + get_uuid_str()

    _clickhouse_table_with_five_appends(
        started_cluster_iceberg_with_spark, storage_type, TABLE_NAME
    )

    assert _pruned_files(instance, TABLE_NAME, f"SELECT id FROM {TABLE_NAME} ORDER BY ALL") == 0

    # No lineage statistics are written for new rows, so no file is skipped.
    assert (
        _pruned_files(
            instance, TABLE_NAME, f"SELECT id FROM {TABLE_NAME} WHERE _row_id = 25 ORDER BY ALL"
        )
        == 0
    )

    assert (
        _pruned_files(
            instance, TABLE_NAME, f"SELECT id FROM {TABLE_NAME} WHERE _row_id >= 35 ORDER BY ALL"
        )
        == 0
    )

    assert (
        _pruned_files(
            instance, TABLE_NAME, f"SELECT id FROM {TABLE_NAME} WHERE _row_id < 10 ORDER BY ALL"
        )
        == 0
    )

    # A range spanning everything prunes nothing.
    assert (
        _pruned_files(
            instance, TABLE_NAME, f"SELECT id FROM {TABLE_NAME} WHERE _row_id >= 0 ORDER BY ALL"
        )
        == 0
    )


@pytest.mark.parametrize("storage_type", ["s3"])
def test_incremental_read_by_sequence_number_keeps_files_without_lineage_bounds_clickhouse(
    started_cluster_iceberg_with_spark, storage_type
):
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    TABLE_NAME = "test_sequence_number_pruning_clickhouse_" + storage_type + "_" + get_uuid_str()

    _clickhouse_table_with_five_appends(
        started_cluster_iceberg_with_spark, storage_type, TABLE_NAME
    )

    assert (
        _pruned_files(
            instance,
            TABLE_NAME,
            f"SELECT id FROM {TABLE_NAME} WHERE _last_updated_sequence_number > 3 ORDER BY ALL",
        )
        == 0
    )

    assert (
        _pruned_files(
            instance,
            TABLE_NAME,
            f"SELECT id FROM {TABLE_NAME} WHERE _last_updated_sequence_number = 2 ORDER BY ALL",
        )
        == 0
    )

    assert (
        _pruned_files(
            instance,
            TABLE_NAME,
            f"SELECT id FROM {TABLE_NAME} WHERE _last_updated_sequence_number > 0 ORDER BY ALL",
        )
        == 0
    )


@pytest.mark.parametrize("storage_type", ["s3"])
def test_row_id_pruning_is_skipped_for_orc_clickhouse(
    started_cluster_iceberg_with_spark, storage_type
):
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    TABLE_NAME = "test_row_lineage_pruning_orc_clickhouse_" + storage_type + "_" + get_uuid_str()

    create_iceberg_table(
        storage_type,
        instance,
        TABLE_NAME,
        started_cluster_iceberg_with_spark,
        "(id Int32, s String)",
        format_version=3,
        format="ORC",
    )
    for lo in range(0, 50, 10):
        instance.query(
            f"INSERT INTO {TABLE_NAME} SELECT number, 'a' FROM numbers({lo}, 10)",
            settings=INSERT_SETTINGS,
        )

    # Rows of ORC data files inherit their row ids, but ORC files are not skipped by `_row_id` bounds.
    assert (
        instance.query(
            f"SELECT id, _row_id FROM {TABLE_NAME} WHERE _row_id IN (0, 25, 49) ORDER BY id",
            settings=PRUNING_ENABLED,
        )
        == "0\t0\n25\t25\n49\t49\n"
    )
    assert (
        _pruned_files(
            instance,
            TABLE_NAME,
            f"SELECT id FROM {TABLE_NAME} WHERE _row_id = 25 ORDER BY ALL",
        )
        == 0
    )

    # The sequence number does not depend on row numbers, but without lineage statistics it does not
    # prune either.
    assert (
        _pruned_files(
            instance,
            TABLE_NAME,
            f"SELECT id FROM {TABLE_NAME} WHERE _last_updated_sequence_number > 3 ORDER BY ALL",
        )
        == 0
    )
