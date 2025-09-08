
import pytest

from helpers.iceberg_utils import (
    create_iceberg_table,
    get_uuid_str,
    default_download_directory
)


def test_writes_create_table_bugs(started_cluster):
    instance = started_cluster.instances["node1"]
    TABLE_NAME = "test_relevant_iceberg_schema_chosen_" + get_uuid_str()
    TABLE_NAME_1 = "test_relevant_iceberg_schema_chosen_" + get_uuid_str()
    instance.query(
        f"CREATE TABLE {TABLE_NAME} (c0 Int) ENGINE = IcebergLocal('/iceberg_data/default/{TABLE_NAME}/', 'CSV') AS (SELECT 1 OFFSET 1 ROW);",
        settings={"allow_experimental_insert_into_iceberg": 1}
    )

    instance.query(
        f"CREATE TABLE {TABLE_NAME_1} (c0 Int) ENGINE = IcebergLocal('/iceberg_data/default/{TABLE_NAME_1}/', 'CSV') PARTITION BY (icebergTruncate(c0));",
        settings={"allow_experimental_insert_into_iceberg": 1}
    )