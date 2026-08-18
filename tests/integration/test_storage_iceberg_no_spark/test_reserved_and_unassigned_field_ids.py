"""Parquet field ids a data file carries but the Iceberg schema does not project.

Iceberg reserves field ids above 2147483447 for writer-internal columns such as `_row_id`, and
readers must ignore reserved ids they do not recognize instead of failing
(https://iceberg.apache.org/spec/#reserved-field-ids). An id the table has actually assigned but
that the current schema no longer lists is likewise not projected
(https://iceberg.apache.org/spec/#column-projection). Every other unmapped id is a genuine
mismatch between the file and the metadata and must still be rejected.

Reproduces https://github.com/ClickHouse/ClickHouse/issues/107343 and pins the accepted window of
https://github.com/ClickHouse/ClickHouse/issues/113324.
"""

import os
import shutil
import tempfile

import pyarrow as pa
import pyarrow.parquet as pq

from helpers.iceberg_utils import (
    create_iceberg_table,
    get_creation_expression,
    get_uuid_str,
)

# Integer.MAX_VALUE - 200. Ids strictly above this are reserved; this one is still assignable.
ICEBERG_MAX_USER_FIELD_ID = 2147483447
# _row_id, one of the row-lineage columns a spec-compliant v3 writer materializes.
ROW_LINEAGE_ROW_ID_FIELD_ID = 2147483540

NOCACHE = {
    "use_iceberg_metadata_files_cache": False,
    "use_parquet_metadata_cache": False,
}


def _table_path(table_name):
    return f"/var/lib/clickhouse/user_files/iceberg_data/default/{table_name}"


def _sole_data_file(instance, table_name):
    """The single Parquet data file of a freshly written one-insert table."""
    data_dir = f"{_table_path(table_name)}/data"
    parquet_files = [
        path
        for path in instance.get_files_list_in_container(data_dir)
        if path.endswith(".parquet")
    ]
    assert len(parquet_files) == 1, f"expected one data file in {data_dir}, got {parquet_files}"
    return parquet_files[0]


def _add_physical_column(instance, remote_path, extra_field_id, rows):
    """Rewrite the data file so it also holds `extra` under `extra_field_id`.

    Column `x` keeps field id 1, so it still maps to the table column; `extra` is present in the
    file only, exactly as a writer that emits a column the read schema does not project.
    """
    temp_dir = tempfile.mkdtemp()
    try:
        local_path = os.path.join(temp_dir, os.path.basename(remote_path))
        instance.copy_file_from_container(remote_path, local_path)

        x = pa.field(
            "x", pa.int32(), nullable=False, metadata={b"PARQUET:field_id": b"1"}
        )
        extra = pa.field(
            "extra",
            pa.int64(),
            nullable=True,
            metadata={b"PARQUET:field_id": str(extra_field_id).encode()},
        )
        table = pa.table(
            {
                "x": pa.array(rows, pa.int32()),
                "extra": pa.array(list(range(len(rows))), pa.int64()),
            },
            schema=pa.schema([x, extra]),
        )
        pq.write_table(table, local_path)

        instance.copy_file_to_container(local_path, remote_path)
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)


def _prepare(cluster, instance, table_name, extra_field_id, rows):
    create_iceberg_table(
        "local",
        instance,
        table_name,
        cluster,
        "(x Int32)",
        settings={"allow_insert_into_iceberg": 1, **NOCACHE},
    )
    values = ", ".join(f"({row})" for row in rows)
    instance.query(
        f"INSERT INTO {table_name} VALUES {values}",
        settings={"allow_insert_into_iceberg": 1, **NOCACHE},
    )
    assert instance.query(
        f"SELECT x FROM {table_name} ORDER BY x", settings=NOCACHE
    ) == "".join(f"{row}\n" for row in rows)

    _add_physical_column(
        instance, _sole_data_file(instance, table_name), extra_field_id, rows
    )
    # The table function re-reads the metadata from disk, so the rewritten file is what it sees.
    return get_creation_expression("local", table_name, cluster, table_function=True)


def test_reserved_row_lineage_field_id_is_ignored(started_cluster_iceberg_no_spark):
    """A reserved row-lineage id in the file must not stop the projected column being read."""
    instance = started_cluster_iceberg_no_spark.instances["node1"]
    table_name = "test_v3_row_lineage_" + get_uuid_str()

    table_expression = _prepare(
        started_cluster_iceberg_no_spark,
        instance,
        table_name,
        ROW_LINEAGE_ROW_ID_FIELD_ID,
        [1, 2, 3],
    )

    assert (
        instance.query(
            f"SELECT x FROM {table_expression} ORDER BY x", settings=NOCACHE
        )
        == "1\n2\n3\n"
    )


def test_field_id_at_reserved_boundary_is_rejected(started_cluster_iceberg_no_spark):
    """2147483447 is the highest assignable id, so it is NOT reserved.

    This table never assigned it, so an unmapped column carrying it is a mismatch. Pins the
    reserved-range comparison as strictly greater-than.
    """
    instance = started_cluster_iceberg_no_spark.instances["node1"]
    table_name = "test_v3_unmapped_" + get_uuid_str()

    table_expression = _prepare(
        started_cluster_iceberg_no_spark,
        instance,
        table_name,
        ICEBERG_MAX_USER_FIELD_ID,
        [1],
    )

    error = instance.query_and_get_error(
        f"SELECT x FROM {table_expression} ORDER BY x", settings=NOCACHE
    )
    assert "ICEBERG_SPECIFICATION_VIOLATION" in error, error


def test_field_id_below_assigned_range_is_rejected(started_cluster_iceberg_no_spark):
    """Id 0 is below the table's last assigned field id, yet Iceberg assigns ids from 1 up.

    So no table ever assigned it and it cannot be a column dropped from the schema. Pins the lower
    bound of the accepted window.
    """
    instance = started_cluster_iceberg_no_spark.instances["node1"]
    table_name = "test_v3_zero_" + get_uuid_str()

    table_expression = _prepare(
        started_cluster_iceberg_no_spark, instance, table_name, 0, [1]
    )

    error = instance.query_and_get_error(
        f"SELECT x FROM {table_expression} ORDER BY x", settings=NOCACHE
    )
    assert "ICEBERG_SPECIFICATION_VIOLATION" in error, error
