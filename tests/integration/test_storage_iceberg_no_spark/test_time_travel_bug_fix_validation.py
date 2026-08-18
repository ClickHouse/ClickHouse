import pytest
import os

from helpers.iceberg_utils import (
    create_iceberg_table,
    default_download_directory,
    get_last_snapshot,
    get_uuid_str
)

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))

def test_time_travel_bug_fix_validation(started_cluster_iceberg_no_spark):
    instance = started_cluster_iceberg_no_spark.instances["node1"]
    TABLE_NAME = "test_time_travel_bug_fix_validation_" + get_uuid_str()

    create_iceberg_table("local", instance, TABLE_NAME, started_cluster_iceberg_no_spark, "(x String, y Int64)")

    instance.query(f"INSERT INTO {TABLE_NAME} VALUES ('123', 1);", settings={"allow_insert_into_iceberg": 1, "write_full_path_in_iceberg_metadata": True})

    default_download_directory(
        started_cluster_iceberg_no_spark,
        "local",
        f"/var/lib/clickhouse/user_files/iceberg_data/default/{TABLE_NAME}/",
        f"/var/lib/clickhouse/user_files/iceberg_data/default/{TABLE_NAME}/",
    )

    first_snapshot = get_last_snapshot(f"/var/lib/clickhouse/user_files/iceberg_data/default/{TABLE_NAME}/")

    instance.query(f"INSERT INTO {TABLE_NAME} VALUES ('123', 1);", settings={"allow_insert_into_iceberg": 1, "write_full_path_in_iceberg_metadata": True})

    instance.query(f"SELECT count() FROM {TABLE_NAME}", settings={"iceberg_snapshot_id": first_snapshot})


    assert int((instance.query(f"SELECT count() FROM {TABLE_NAME}")).strip()) == 2
