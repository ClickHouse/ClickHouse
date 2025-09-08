
import pytest

from helpers.iceberg_utils import (
    get_uuid_str,
)


def test_writes_create_table_bugs(started_cluster_iceberg_no_spark):
    instance = started_cluster_iceberg_no_spark.instances["node1"]
    TABLE_NAME = "test_writes_create_table_bugs_" + get_uuid_str()
    TABLE_NAME_1 = "test_writes_create_table_bugs_" + get_uuid_str()
    instance.query(
        f"CREATE TABLE {TABLE_NAME} (c0 Int) ENGINE = IcebergLocal('/iceberg_data/default/{TABLE_NAME}/', 'CSV') AS (SELECT 1 OFFSET 1 ROW);",
        settings={"allow_experimental_insert_into_iceberg": 1}
    )

    instance.query(
        f"CREATE TABLE {TABLE_NAME_1} (c0 Int) ENGINE = IcebergLocal('/iceberg_data/default/{TABLE_NAME_1}/', 'CSV') PARTITION BY (icebergTruncate(c0));",
        settings={"allow_experimental_insert_into_iceberg": 1}
    )