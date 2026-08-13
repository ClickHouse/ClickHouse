import pytest

from helpers.iceberg_utils import (
    get_uuid_str,
    default_upload_directory,
    create_iceberg_table
)


def test_metadata_file_path_security(started_cluster_iceberg_with_spark):
    """
    Test security fixes for absolute path and null byte injection in iceberg_metadata_file_path.
    Related to: https://github.com/ClickHouse/clickhouse-private/issues/46354
    """
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    TABLE_NAME = (
        "test_metadata_security_"
        + get_uuid_str()
    )

    # Create a simple Iceberg table with a few versions
    spark.sql(
        f"CREATE TABLE {TABLE_NAME} (id bigint, data string) USING iceberg "
        f"TBLPROPERTIES ('format-version' = '2')"
    )

    # Insert data to create metadata versions
    for i in range(5):
        spark.sql(
            f"INSERT INTO {TABLE_NAME} select id, char(id + ascii('a')) from range(10)"
        )

    # Upload to ClickHouse accessible storage
    default_upload_directory(
        started_cluster_iceberg_with_spark,
        "local",
        f"/iceberg_data/default/{TABLE_NAME}/",
        f"/var/lib/clickhouse/user_files/iceberg_data/default/{TABLE_NAME}/",
    )

    # Test 1: Absolute path outside table directory should be rejected
    with pytest.raises(Exception, match = "PATH_ACCESS_DENIED"):
        create_iceberg_table(
            "local",
            instance,
            TABLE_NAME,
            started_cluster_iceberg_with_spark,
            explicit_metadata_path="/etc/passwd"
        )

    # Test 2: Null byte injection should be rejected
    with pytest.raises(Exception, match = "ICEBERG_SPECIFICATION_VIOLATION"):
        create_iceberg_table(
            "local",
            instance,
            TABLE_NAME,
            started_cluster_iceberg_with_spark,
            explicit_metadata_path=chr(0) + chr(1)
        )

    # Test 3: Null byte in middle of path should be rejected
    with pytest.raises(Exception, match = "PATH_ACCESS_DENIED"):
        create_iceberg_table(
            "local",
            instance,
            TABLE_NAME,
            started_cluster_iceberg_with_spark,
            explicit_metadata_path="/etc/passwd\x00/v1.metadata.json"
        )

    # Test 4: Path traversal with ../ should be rejected
    with pytest.raises(Exception, match = "BAD_ARGUMENTS"):
        create_iceberg_table(
            "local",
            instance,
            TABLE_NAME,
            started_cluster_iceberg_with_spark,
            explicit_metadata_path="../../../etc/passwd"
        )

    # Test 5: Path traversal in middle should be rejected
    with pytest.raises(Exception, match = "PATH_ACCESS_DENIED"):
        create_iceberg_table(
            "local",
            instance,
            TABLE_NAME,
            started_cluster_iceberg_with_spark,
            explicit_metadata_path="metadata/../../etc/passwd"
        )

    # Test 6: Relative path starting with . should be rejected
    with pytest.raises(Exception, match = "BAD_ARGUMENTS"):
        create_iceberg_table(
            "local",
            instance,
            TABLE_NAME,
            started_cluster_iceberg_with_spark,
            explicit_metadata_path="./secret"
        )

    # Test 7: Relative path starting with .. should be rejected
    with pytest.raises(Exception, match = "BAD_ARGUMENTS"):
        create_iceberg_table(
            "local",
            instance,
            TABLE_NAME,
            started_cluster_iceberg_with_spark,
            explicit_metadata_path=".."
        )

    # Test 8: Just a dot should be rejected
    with pytest.raises(Exception, match = "BAD_ARGUMENTS"):
        create_iceberg_table(
            "local",
            instance,
            TABLE_NAME,
            started_cluster_iceberg_with_spark,
            explicit_metadata_path="."
        )

    # Test 9: Absolute path with /tmp should be rejected
    with pytest.raises(Exception, match = "PATH_ACCESS_DENIED"):
        create_iceberg_table(
            "local",
            instance,
            TABLE_NAME,
            started_cluster_iceberg_with_spark,
            explicit_metadata_path="/tmp/malicious.json"
        )

    # # Test 10: Valid relative path should work (or at least pass path validation)
    # # This should succeed in creating the table
    create_iceberg_table(
        "local",
        instance,
        TABLE_NAME,
        started_cluster_iceberg_with_spark,
        explicit_metadata_path="metadata/v4.metadata.json"
    )

    # Verify the table works with valid path
    result = int(instance.query(f"SELECT count() FROM {TABLE_NAME}"))
    assert result == 30  # 3 inserts of 10 rows each (v3.metadata.json is after 3rd insert)

    # Test 11: Another valid path with nested directory
    instance.query(f"DROP TABLE IF EXISTS {TABLE_NAME}")
    create_iceberg_table(
        "local",
        instance,
        TABLE_NAME,
        started_cluster_iceberg_with_spark,
        explicit_metadata_path=f"/var/lib/clickhouse/user_files/iceberg_data/default/{TABLE_NAME}/metadata/v6.metadata.json"
    )

    result = int(instance.query(f"SELECT count() FROM {TABLE_NAME}"))
    assert result == 50  # All 5 inserts
