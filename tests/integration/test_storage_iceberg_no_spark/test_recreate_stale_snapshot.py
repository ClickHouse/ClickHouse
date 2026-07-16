#!/usr/bin/env python3

# Regression test for https://github.com/ClickHouse/ClickHouse/issues/87574
#
# After an Iceberg table is dropped and recreated at the same location, the
# catalog-less `iceberg()` table function used to return the OLD rows: it
# resolved the current metadata file by the highest version number in the file
# name, but version numbers reset on recreate, so the dropped table's leftover
# metadata (higher version) shadowed the freshly recreated one. Selecting the
# metadata file by the monotonic `last-updated-ms` field (now the default) fixes
# it. The DataLakeCatalog engine always resolved this correctly via the catalog
# metadata pointer; both must now agree.

import uuid

import pyarrow as pa
from pyiceberg.schema import Schema, NestedField
from pyiceberg.partitioning import PartitionSpec
from pyiceberg.table.sorting import SortOrder
from pyiceberg.types import LongType, StringType

from test_read_in_order_with_pyiceberg import (
    CATALOG_NAME,
    create_clickhouse_iceberg_database,
    load_catalog_impl,
)

_SCHEMA = Schema(
    NestedField(field_id=1, name="id", field_type=LongType(), required=False),
    NestedField(field_id=2, name="name", field_type=StringType(), required=False),
)


def test_recreate_stale_snapshot(started_cluster_iceberg_no_spark):
    instance = started_cluster_iceberg_no_spark.instances["node1"]
    catalog = load_catalog_impl(started_cluster_iceberg_no_spark)
    namespace = f"clickhouse_{uuid.uuid4().hex}"
    table_short = "recreate_test"
    identifier = f"{namespace}.{table_short}"

    # Distinct s3 location per run so the leftover metadata of previous runs
    # cannot interfere with this one.
    table_dir = f"recreate_{uuid.uuid4().hex}"
    location = f"s3://warehouse-rest/{table_dir}"

    def make_table():
        return catalog.create_table(
            identifier=identifier,
            schema=_SCHEMA,
            location=location,
            partition_spec=PartitionSpec(),
            sort_order=SortOrder(),
        )

    # 1. Create + insert 3 rows.
    table = make_table()
    table.append(
        pa.table(
            {
                "id": pa.array([1, 2, 3], type=pa.int64()),
                "name": pa.array(["a", "b", "c"], type=pa.string()),
            }
        )
    )

    create_clickhouse_iceberg_database(
        started_cluster_iceberg_no_spark, instance, CATALOG_NAME
    )

    # The s3 path the catalog-less table function reads (matches `location`).
    table_function = (
        f"icebergS3(s3, filename = '{table_dir}/', "
        "url = 'http://minio1:9001/warehouse-rest/')"
    )

    def tf_count():
        return int(instance.query(f"SELECT count() FROM {table_function}").strip())

    def catalog_count():
        return int(
            instance.query(
                f"SELECT count() FROM {CATALOG_NAME}.`{identifier}`"
            ).strip()
        )

    assert tf_count() == 3
    assert catalog_count() == 3

    # 2. Drop (pyiceberg does not purge files) and recreate empty at the same
    #    location. The old metadata (3 rows, higher version number) is left
    #    behind on s3.
    catalog.drop_table(identifier)
    make_table()

    # Reload the catalog database so it picks up the recreated table.
    create_clickhouse_iceberg_database(
        started_cluster_iceberg_no_spark, instance, CATALOG_NAME
    )

    # Both readers must now see the recreated (empty) table, not the stale 3
    # rows of the dropped one.
    assert catalog_count() == 0
    assert tf_count() == 0

    # Old highest-version selection is still available on demand.
    stale = int(
        instance.query(
            f"SELECT count() FROM {table_function} "
            "SETTINGS iceberg_recent_metadata_file_by_last_updated_ms_field = 0"
        ).strip()
    )
    assert stale == 3
