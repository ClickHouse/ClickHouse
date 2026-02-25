import time
import pytest

from helpers.iceberg_utils import (
    create_iceberg_table,
    get_uuid_str,
    default_download_directory,
)


@pytest.mark.parametrize("storage_type", ["s3", "local"])
def test_expire_snapshots_basic(started_cluster_iceberg_with_spark, storage_type):
    format_version = 2
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    TABLE_NAME = "test_expire_snapshots_basic_" + storage_type + "_" + get_uuid_str()

    create_iceberg_table(
        storage_type, instance, TABLE_NAME, started_cluster_iceberg_with_spark, "(x Int)", format_version
    )

    instance.query(
        f"INSERT INTO {TABLE_NAME} VALUES (1);",
        settings={"allow_insert_into_iceberg": 1},
    )
    instance.query(
        f"INSERT INTO {TABLE_NAME} VALUES (2);",
        settings={"allow_insert_into_iceberg": 1},
    )
    instance.query(
        f"INSERT INTO {TABLE_NAME} VALUES (3);",
        settings={"allow_insert_into_iceberg": 1},
    )

    assert instance.query(f"SELECT count() FROM {TABLE_NAME}") == "3\n"

    history = instance.query(
        f"SELECT count() FROM system.iceberg_history WHERE database = 'default' AND table = '{TABLE_NAME}'"
    )
    assert int(history.strip()) >= 3

    time.sleep(2)
    expire_timestamp = time.strftime("%Y-%m-%d %H:%M:%S")

    instance.query(
        f"INSERT INTO {TABLE_NAME} VALUES (4);",
        settings={"allow_insert_into_iceberg": 1},
    )

    instance.query(
        f"ALTER TABLE {TABLE_NAME} EXECUTE expire_snapshots('{expire_timestamp}');",
        settings={"allow_insert_into_iceberg": 1},
    )

    assert instance.query(f"SELECT * FROM {TABLE_NAME} ORDER BY x") == "1\n2\n3\n4\n"


@pytest.mark.parametrize("storage_type", ["s3"])
def test_expire_snapshots_no_expirable(started_cluster_iceberg_with_spark, storage_type):
    """Test that expire_snapshots is a no-op when no snapshots match the criteria."""
    format_version = 2
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    TABLE_NAME = "test_expire_no_expirable_" + storage_type + "_" + get_uuid_str()

    create_iceberg_table(
        storage_type, instance, TABLE_NAME, started_cluster_iceberg_with_spark, "(x Int)", format_version
    )

    instance.query(
        f"INSERT INTO {TABLE_NAME} VALUES (1);",
        settings={"allow_insert_into_iceberg": 1},
    )

    instance.query(
        f"ALTER TABLE {TABLE_NAME} EXECUTE expire_snapshots('2020-01-01 00:00:00');",
        settings={"allow_insert_into_iceberg": 1},
    )

    assert instance.query(f"SELECT * FROM {TABLE_NAME} ORDER BY x") == "1\n"


@pytest.mark.parametrize("storage_type", ["s3"])
def test_expire_snapshots_files_cleaned(started_cluster_iceberg_with_spark, storage_type):
    """Test that expired snapshot metadata files are physically removed."""
    format_version = 2
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    TABLE_NAME = "test_expire_files_cleaned_" + storage_type + "_" + get_uuid_str()

    create_iceberg_table(
        storage_type, instance, TABLE_NAME, started_cluster_iceberg_with_spark, "(x Int)", format_version
    )

    instance.query(
        f"INSERT INTO {TABLE_NAME} VALUES (1);",
        settings={"allow_insert_into_iceberg": 1},
    )
    instance.query(
        f"INSERT INTO {TABLE_NAME} VALUES (2);",
        settings={"allow_insert_into_iceberg": 1},
    )

    time.sleep(2)
    expire_timestamp = time.strftime("%Y-%m-%d %H:%M:%S")

    instance.query(
        f"INSERT INTO {TABLE_NAME} VALUES (3);",
        settings={"allow_insert_into_iceberg": 1},
    )

    files_before = default_download_directory(
        started_cluster_iceberg_with_spark,
        storage_type,
        f"/var/lib/clickhouse/user_files/iceberg_data/default/{TABLE_NAME}/",
        f"/var/lib/clickhouse/user_files/iceberg_data/default/{TABLE_NAME}/",
    )

    instance.query(
        f"ALTER TABLE {TABLE_NAME} EXECUTE expire_snapshots('{expire_timestamp}');",
        settings={"allow_insert_into_iceberg": 1},
    )

    files_after = default_download_directory(
        started_cluster_iceberg_with_spark,
        storage_type,
        f"/var/lib/clickhouse/user_files/iceberg_data/default/{TABLE_NAME}/",
        f"/var/lib/clickhouse/user_files/iceberg_data/default/{TABLE_NAME}/",
    )

    assert len(files_after) < len(files_before), \
        f"Expected fewer files after expire_snapshots: {len(files_after)} >= {len(files_before)}"

    assert instance.query(f"SELECT * FROM {TABLE_NAME} ORDER BY x") == "1\n2\n3\n"


@pytest.mark.parametrize("storage_type", ["s3"])
def test_expire_snapshots_format_v1_error(started_cluster_iceberg_with_spark, storage_type):
    """Test that expire_snapshots fails for format version 1."""
    format_version = 1
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    TABLE_NAME = "test_expire_v1_error_" + storage_type + "_" + get_uuid_str()

    create_iceberg_table(
        storage_type, instance, TABLE_NAME, started_cluster_iceberg_with_spark, "(x Int)", format_version
    )

    instance.query(
        f"INSERT INTO {TABLE_NAME} VALUES (1);",
        settings={"allow_insert_into_iceberg": 1},
    )

    with pytest.raises(Exception):
        instance.query(
            f"ALTER TABLE {TABLE_NAME} EXECUTE expire_snapshots('2020-01-01 00:00:00');",
            settings={"allow_insert_into_iceberg": 1},
        )


@pytest.mark.parametrize("storage_type", ["s3"])
def test_expire_snapshots_preserves_current(started_cluster_iceberg_with_spark, storage_type):
    """Test that the current snapshot is never expired, even with a future timestamp."""
    format_version = 2
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    TABLE_NAME = "test_expire_preserves_current_" + storage_type + "_" + get_uuid_str()

    create_iceberg_table(
        storage_type, instance, TABLE_NAME, started_cluster_iceberg_with_spark, "(x Int)", format_version
    )

    instance.query(
        f"INSERT INTO {TABLE_NAME} VALUES (1);",
        settings={"allow_insert_into_iceberg": 1},
    )
    instance.query(
        f"INSERT INTO {TABLE_NAME} VALUES (2);",
        settings={"allow_insert_into_iceberg": 1},
    )

    instance.query(
        f"ALTER TABLE {TABLE_NAME} EXECUTE expire_snapshots('2099-12-31 23:59:59');",
        settings={"allow_insert_into_iceberg": 1},
    )

    assert instance.query(f"SELECT * FROM {TABLE_NAME} ORDER BY x") == "1\n2\n"
