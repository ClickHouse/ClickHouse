"""
A `Buffer` over an Iceberg table must write blocks typed for the Iceberg schema: Iceberg stores
`UInt8` as `int` and `Date` as `date`, so the destination's columns change on its first metadata refresh.
"""

import pytest

from helpers.iceberg_utils import get_uuid_str


@pytest.mark.parametrize("mode, rows", [("direct", 84), ("optimize", 5)])
def test_buffer_flush_into_iceberg_converts_to_iceberg_schema(
    started_cluster_iceberg_no_spark, mode, rows
):
    node = started_cluster_iceberg_no_spark.instances["node1"]
    dst = f"buffer_dst_{mode}_{get_uuid_str()}"
    buf = f"buffer_src_{mode}_{get_uuid_str()}"
    path = f"/var/lib/clickhouse/user_files/iceberg_data/default/{dst}/"

    node.query(
        f"CREATE TABLE {dst} (a Date32, b Nullable(UInt8), c Date) ENGINE = IcebergLocal('{path}', 'Parquet')"
    )
    # max_rows = 10: 84 rows are written directly, 5 rows stay buffered until OPTIMIZE.
    node.query(
        f"CREATE TABLE {buf} ENGINE = Buffer(default, {dst}, 1, 100000, 100000, 1000000, 10, 1000000000, 1000000000)"
    )
    node.query(
        f"INSERT INTO {buf} (b, a, c) SELECT 75, '2020-01-01'::Date32, '2020-01-02'::Date FROM numbers({rows})"
    )
    if mode == "optimize":
        node.query(f"OPTIMIZE TABLE {buf}")

    assert (
        node.query(
            f"SELECT count(), sum(b), toTypeName(any(b)), any(c), toTypeName(any(c)) FROM {dst}"
        )
        == f"{rows}\t{75 * rows}\tNullable(Int32)\t2020-01-02\tDate32\n"
    )
