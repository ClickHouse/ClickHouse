"""An Iceberg `uuid` manifest bound must not drive min/max pruning. The spec serializes the value as
16 big-endian bytes and a conforming writer orders the bound pair by unsigned byte comparison, while
ClickHouse orders `UUID` by its second half. Byte-wise A < C < B below, so a file holding all three
is described by the pair (A, B); in ClickHouse order those sort A < B < C, so C is outside the pair
and stays outside it after a byte swap. Pruning on such a pair drops a file that holds matching rows.
The fallback is per column: `id` in the same manifest entry must keep pruning."""

import uuid as uuidlib

import pyarrow as pa
from pyiceberg.catalog import load_catalog
from pyiceberg.partitioning import PartitionSpec
from pyiceberg.schema import NestedField, Schema
from pyiceberg.types import LongType, UUIDType

from helpers.config_cluster import minio_access_key, minio_secret_key
from helpers.iceberg_utils import (
    check_validity_and_get_prunned_files_general,
    get_uuid_str,
)

# The Parquet statistics push-downs are all off. They prune on the `UUID` statistics of the data file
# itself, which is the separate defect of #118371 on a path with no Iceberg involvement; with the row
# group one left on, the reads below stay empty however the manifest bound is read.
SETTINGS = {
    "use_iceberg_metadata_files_cache": False,
    "use_parquet_metadata_cache": False,
    "input_format_parquet_filter_push_down": 0,
    "input_format_parquet_bloom_filter_push_down": 0,
    "input_format_parquet_page_filter_push_down": 0,
}

A = "00000000-0000-0000-0000-000000000001"
C = "00000000-0000-0000-ffff-ffffffffffff"
B = "00000001-0000-0000-0000-000000000002"
D = "00000002-0000-0000-0000-000000000003"


def test_iceberg_uuid_manifest_bounds(started_cluster_iceberg_no_spark):
    """pyiceberg writes the table, because the ClickHouse writer emits no `uuid` bounds at all."""
    cluster = started_cluster_iceberg_no_spark
    instance = cluster.instances["node1"]
    table_name = "test_iceberg_uuid_manifest_bounds_" + get_uuid_str()
    key_prefix = f"var/lib/clickhouse/user_files/iceberg_data/default/{table_name}"

    catalog = load_catalog(
        "demo",
        **{
            "uri": f"http://localhost:{cluster.iceberg_rest_catalog_port}",
            "type": "rest",
            "s3.endpoint": f"http://{cluster.minio_ip}:{cluster.minio_port}",
            "s3.access-key-id": minio_access_key,
            "s3.secret-access-key": minio_secret_key,
        },
    )
    namespace = f"clickhouse_{get_uuid_str()}"
    catalog.create_namespace(namespace)
    table = catalog.create_table(
        f"{namespace}.{table_name}",
        schema=Schema(
            NestedField(1, "id", LongType(), required=False),
            NestedField(2, "u", UUIDType(), required=False),
        ),
        location=f"s3://{cluster.minio_bucket}/{key_prefix}",
        partition_spec=PartitionSpec(),
    )

    # The field ids must reach the Parquet files for the Iceberg reader to match the columns.
    arrow_schema = pa.schema(
        [
            pa.field("id", pa.int64(), True, metadata={b"PARQUET:field_id": b"1"}),
            pa.field("u", pa.uuid(), True, metadata={b"PARQUET:field_id": b"2"}),
        ]
    )
    # One data file per append. The first holds three values, so its declared pair spans an interior
    # value that ClickHouse orders outside the pair. The second holds one value, where lower equals
    # upper and only the byte order differs.
    for ids, values in (([1, 2, 3], [A, C, B]), ([11], [D])):
        table.append(
            pa.Table.from_pydict(
                {"id": ids, "u": [uuidlib.UUID(value).bytes for value in values]},
                schema=arrow_schema,
            )
        )

    source = (
        f"icebergS3(s3, filename = '{key_prefix}/', format=Parquet, "
        f"url = 'http://minio1:9001/{cluster.minio_bucket}/')"
    )
    assert instance.query(f"SELECT count() FROM {source}", settings=SETTINGS).strip() == "4"

    def rows(where):
        return instance.query(
            f"SELECT id FROM {source} WHERE {where} ORDER BY ALL", settings=SETTINGS
        ).strip()

    def pruned_files(where):
        return check_validity_and_get_prunned_files_general(
            instance,
            table_name,
            {"use_iceberg_partition_pruning": 0, **SETTINGS},
            {"use_iceberg_partition_pruning": 1, **SETTINGS},
            "IcebergMinMaxIndexPrunedFiles",
            f"SELECT id, u FROM {source} WHERE {where} ORDER BY ALL",
        )

    # Control: the `id` bounds are well formed and still prune the one non-matching file, so the
    # fallback below is per column rather than a loss of min/max pruning for the whole entry.
    assert pruned_files("id < 5") == 1

    # `toUUID` is explicit because a bare string literal can leave the column without a usable
    # min/max condition, and then nothing prunes whatever the bounds say. The row assertions pin the
    # result, since two empty results also satisfy the equality check inside the pruning helper.
    assert rows(f"u = toUUID('{C}')") == "2"
    assert pruned_files(f"u = toUUID('{C}')") == 0
    assert rows(f"u = toUUID('{D}')") == "11"
    assert pruned_files(f"u = toUUID('{D}')") == 0
