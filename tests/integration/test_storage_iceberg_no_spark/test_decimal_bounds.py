#!/usr/bin/env python3

# Regression test for https://github.com/ClickHouse/ClickHouse/issues/114929
#
# Iceberg serializes a decimal column's min/max bounds as the unscaled value in two's complement
# big endian form, using the minimum number of bytes. Reading a manifest decodes the bounds of
# every column of the entry, so any query carrying a filter reaches the decimal decoder even when
# the filter does not touch the decimal column. The decoder used to accumulate into `int64_t`,
# where a bound wider than 64 bits and the `10^scale` scaler both overflow: a `decimal(38, 30)`
# bound is 14 bytes wide and its scaler is `10^30`. That is undefined behaviour, and a build with
# the undefined behaviour sanitizer aborts on it.
#
# ClickHouse cannot write a decimal column to Iceberg (`getIcebergType` has no decimal case), so
# the tables are built with PyIceberg, which covers every decimal width the spec allows: `Decimal32`
# (precision up to 9), `Decimal64` (up to 18) and `Decimal128` (up to 38, the spec maximum).

import uuid
from decimal import Decimal

import pyarrow as pa
from pyiceberg.catalog import load_catalog
from pyiceberg.partitioning import PartitionSpec
from pyiceberg.schema import NestedField, Schema
from pyiceberg.types import DecimalType, LongType

from helpers.config_cluster import minio_access_key, minio_secret_key
from helpers.iceberg_utils import (
    check_validity_and_get_prunned_files_general,
    get_uuid_str,
)

BASE_URL = "http://rest:8181/v1"
CATALOG_NAME = "demo"


def load_catalog_impl(started_cluster):
    return load_catalog(
        CATALOG_NAME,
        **{
            "uri": f"http://localhost:{started_cluster.iceberg_rest_catalog_port}",
            "type": "rest",
            "s3.endpoint": f"http://{started_cluster.minio_ip}:{started_cluster.minio_port}",
            "s3.access-key-id": minio_access_key,
            "s3.secret-access-key": minio_secret_key,
        },
    )


def create_clickhouse_iceberg_database(started_cluster, node, name):
    node.query(
        f"""
DROP DATABASE IF EXISTS {name};
SET allow_database_iceberg=true;
CREATE DATABASE {name} ENGINE = DataLakeCatalog('{BASE_URL}', 'minio', '{minio_secret_key}')
SETTINGS catalog_type='rest', warehouse='demo', storage_endpoint='http://minio1:9001/warehouse-rest'
        """
    )


# (column name, precision, scale). The precisions pick one column per ClickHouse decimal width.
DECIMAL_COLUMNS = [
    ("d32", 9, 2),
    ("d64", 18, 4),
    ("d128", 38, 30),
]


def test_decimal_bounds(started_cluster_iceberg_no_spark):
    instance = started_cluster_iceberg_no_spark.instances["node1"]
    catalog = load_catalog_impl(started_cluster_iceberg_no_spark)
    namespace = f"clickhouse_{uuid.uuid4()}"
    table_name = "decimal_bounds_" + get_uuid_str()

    schema = Schema(
        NestedField(field_id=1, name="id", field_type=LongType(), required=False),
        *[
            NestedField(
                field_id=2 + i,
                name=name,
                field_type=DecimalType(precision, scale),
                required=False,
            )
            for i, (name, precision, scale) in enumerate(DECIMAL_COLUMNS)
        ],
    )

    catalog.create_namespace(namespace)
    table = catalog.create_table(
        identifier=f"{namespace}.{table_name}",
        schema=schema,
        location="s3://warehouse-rest/data",
        partition_spec=PartitionSpec(),
    )

    arrow_schema = pa.schema(
        [pa.field("id", pa.int64(), nullable=True)]
        + [
            pa.field(name, pa.decimal128(precision, scale), nullable=True)
            for name, precision, scale in DECIMAL_COLUMNS
        ]
    )

    # One append per row, so each row lands in its own data file and the min/max bounds of every
    # file are used for pruning. Negative values cover the sign extension of the decoder.
    rows = [
        {"id": 1, "d32": Decimal("1.23"), "d64": Decimal("-2.5000"), "d128": Decimal("42.42")},
        {"id": 2, "d32": Decimal("-99.99"), "d64": Decimal("1234.5678"), "d128": Decimal("-7.5")},
        {"id": 3, "d32": Decimal("50.00"), "d64": Decimal("0.0001"), "d128": Decimal("0.000000000000000000000000000001")},
    ]
    for row in rows:
        table.append(pa.Table.from_pylist([row], schema=arrow_schema))

    create_clickhouse_iceberg_database(started_cluster_iceberg_no_spark, instance, CATALOG_NAME)
    table_expression = f"{CATALOG_NAME}.`{namespace}.{table_name}`"

    # A filter is what makes the manifest bounds be decoded; it does not have to touch a decimal
    # column. Before the fix this aborted an undefined behaviour sanitizer build on the
    # `decimal(38, 30)` bound.
    assert instance.query(f"SELECT count() FROM {table_expression} WHERE id > 0").strip() == "3"

    # Every value survives the round trip, so decoding the bounds does not disturb the data.
    assert instance.query(
        f"SELECT id, toString(d32), toString(d64), toString(d128) FROM {table_expression} ORDER BY id"
    ).strip() == "\n".join(
        [
            "1\t1.23\t-2.5\t42.42",
            "2\t-99.99\t1234.5678\t-7.5",
            "3\t50\t0.0001\t0.000000000000000000000000000001",
        ]
    )

    # Filters on each decimal width return the right rows, which requires the decoded bounds to be
    # correct rather than merely non-crashing: a wrong bound would prune a file that must be read.
    for column, expected in [
        ("d32 > 0", "1,3"),
        ("d64 < 0", "1"),
        ("d128 > 1", "1"),
        ("d128 < 0", "2"),
    ]:
        assert (
            instance.query(
                f"SELECT groupArray(id) FROM (SELECT id FROM {table_expression} WHERE {column} ORDER BY id)"
            ).strip()
            == "[" + expected + "]"
        ), f"wrong rows for filter {column}"

    # The bounds are not just decoded, they are used: a predicate outside the range of every file
    # prunes all three data files.
    common_settings = {
        "input_format_parquet_bloom_filter_push_down": 0,
        "input_format_parquet_filter_push_down": 0,
    }
    settings_without = {"use_iceberg_partition_pruning": 0, **common_settings}
    settings_with = {"use_iceberg_partition_pruning": 1, **common_settings}
    assert (
        check_validity_and_get_prunned_files_general(
            instance,
            table_name,
            settings_without,
            settings_with,
            "IcebergMinMaxIndexPrunedFiles",
            f"SELECT * FROM {table_expression} WHERE d128 > 1000 ORDER BY ALL",
        )
        == 3
    )
