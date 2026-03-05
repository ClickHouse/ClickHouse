import json
import time
import pytest

from helpers.iceberg_utils import (
    create_iceberg_table,
    get_uuid_str,
    default_download_directory,
)


def set_iceberg_table_properties(instance, table_name, properties):
    """Write a new metadata version with updated properties.

    Creates v(N+1).metadata.json so ClickHouse reads from disk (the
    process-level metadata cache is keyed by file path, so a new path
    avoids stale cache entries).
    """
    import re

    metadata_dir = f"/var/lib/clickhouse/user_files/iceberg_data/default/{table_name}/metadata"
    latest = instance.exec_in_container(
        ["bash", "-c", f"ls -t {metadata_dir}/*.metadata.json | head -1"]
    ).strip()

    raw = instance.exec_in_container(["cat", latest])
    meta = json.loads(raw)
    if "properties" not in meta:
        meta["properties"] = {}
    meta["properties"].update(properties)
    meta["last-updated-ms"] = int(time.time() * 1000)

    version_match = re.search(r"/v(\d+)[^/]*\.metadata\.json$", latest)
    new_version = int(version_match.group(1)) + 1
    new_path = f"{metadata_dir}/v{new_version}.metadata.json"

    new_content = json.dumps(meta, indent=4)
    instance.exec_in_container(
        ["bash", "-c", f"cat > {new_path} << 'JSONEOF'\n{new_content}\nJSONEOF"]
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


@pytest.mark.parametrize("storage_type", ["local"])
def test_expire_snapshots_retention_min_keep(started_cluster_iceberg_with_spark, storage_type):
    """Retention policy: min-snapshots-to-keep prevents expiring the N most recent ancestors."""
    format_version = 2
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    TABLE_NAME = "test_expire_min_keep_" + storage_type + "_" + get_uuid_str()

    create_iceberg_table(
        storage_type, instance, TABLE_NAME, started_cluster_iceberg_with_spark, "(x Int)", format_version
    )

    for val in range(1, 6):
        instance.query(
            f"INSERT INTO {TABLE_NAME} VALUES ({val});",
            settings={"allow_insert_into_iceberg": 1},
        )

    history_before = int(instance.query(
        f"SELECT count() FROM system.iceberg_history WHERE database = 'default' AND table = '{TABLE_NAME}'"
    ).strip())
    assert history_before == 5

    set_iceberg_table_properties(instance, TABLE_NAME, {
        "history.expire.min-snapshots-to-keep": "3",
    })

    instance.query(
        f"ALTER TABLE {TABLE_NAME} EXECUTE expire_snapshots('2099-12-31 23:59:59');",
        settings={"allow_insert_into_iceberg": 1},
    )

    history_after = int(instance.query(
        f"SELECT count() FROM system.iceberg_history WHERE database = 'default' AND table = '{TABLE_NAME}'"
    ).strip())
    assert history_after == 3, f"Expected 3 retained snapshots, got {history_after}"

    assert instance.query(f"SELECT * FROM {TABLE_NAME} ORDER BY x") == "1\n2\n3\n4\n5\n"


@pytest.mark.parametrize("storage_type", ["local"])
def test_expire_snapshots_retention_max_age(started_cluster_iceberg_with_spark, storage_type):
    """Retention policy: max-snapshot-age-ms preserves recent snapshots regardless of timestamp cutoff."""
    format_version = 2
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    TABLE_NAME = "test_expire_max_age_" + storage_type + "_" + get_uuid_str()

    create_iceberg_table(
        storage_type, instance, TABLE_NAME, started_cluster_iceberg_with_spark, "(x Int)", format_version
    )

    for val in range(1, 6):
        instance.query(
            f"INSERT INTO {TABLE_NAME} VALUES ({val});",
            settings={"allow_insert_into_iceberg": 1},
        )

    set_iceberg_table_properties(instance, TABLE_NAME, {
        "history.expire.min-snapshots-to-keep": "1",
        "history.expire.max-snapshot-age-ms": str(3600 * 1000),
    })

    instance.query(
        f"ALTER TABLE {TABLE_NAME} EXECUTE expire_snapshots('2099-12-31 23:59:59');",
        settings={"allow_insert_into_iceberg": 1},
    )

    history_after = int(instance.query(
        f"SELECT count() FROM system.iceberg_history WHERE database = 'default' AND table = '{TABLE_NAME}'"
    ).strip())
    assert history_after == 5, (
        f"All 5 snapshots are within 1 hour age, so all should be retained; got {history_after}"
    )

    assert instance.query(f"SELECT * FROM {TABLE_NAME} ORDER BY x") == "1\n2\n3\n4\n5\n"


@pytest.mark.parametrize("storage_type", ["local"])
def test_expire_snapshots_no_args_default_retention(started_cluster_iceberg_with_spark, storage_type):
    """No-arg expire_snapshots uses default max-snapshot-age-ms (5 days); all recent snapshots are retained."""
    format_version = 2
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    TABLE_NAME = "test_expire_no_args_default_" + storage_type + "_" + get_uuid_str()

    create_iceberg_table(
        storage_type, instance, TABLE_NAME, started_cluster_iceberg_with_spark, "(x Int)", format_version
    )

    for val in range(1, 4):
        instance.query(
            f"INSERT INTO {TABLE_NAME} VALUES ({val});",
            settings={"allow_insert_into_iceberg": 1},
        )

    history_before = int(instance.query(
        f"SELECT count() FROM system.iceberg_history WHERE database = 'default' AND table = '{TABLE_NAME}'"
    ).strip())
    assert history_before == 3

    instance.query(
        f"ALTER TABLE {TABLE_NAME} EXECUTE expire_snapshots();",
        settings={"allow_insert_into_iceberg": 1},
    )

    history_after = int(instance.query(
        f"SELECT count() FROM system.iceberg_history WHERE database = 'default' AND table = '{TABLE_NAME}'"
    ).strip())
    assert history_after == 3, (
        f"All snapshots are less than 5 days old (default), none should be expired; got {history_after}"
    )

    assert instance.query(f"SELECT * FROM {TABLE_NAME} ORDER BY x") == "1\n2\n3\n"


@pytest.mark.parametrize("storage_type", ["local"])
def test_expire_snapshots_no_args_with_short_max_age(started_cluster_iceberg_with_spark, storage_type):
    """No-arg expire_snapshots with a very short max-snapshot-age-ms expires old snapshots."""
    format_version = 2
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    TABLE_NAME = "test_expire_no_args_short_age_" + storage_type + "_" + get_uuid_str()

    create_iceberg_table(
        storage_type, instance, TABLE_NAME, started_cluster_iceberg_with_spark, "(x Int)", format_version
    )

    for val in range(1, 6):
        instance.query(
            f"INSERT INTO {TABLE_NAME} VALUES ({val});",
            settings={"allow_insert_into_iceberg": 1},
        )

    time.sleep(2)

    set_iceberg_table_properties(instance, TABLE_NAME, {
        "history.expire.min-snapshots-to-keep": "1",
        "history.expire.max-snapshot-age-ms": "1",
    })

    instance.query(
        f"ALTER TABLE {TABLE_NAME} EXECUTE expire_snapshots();",
        settings={"allow_insert_into_iceberg": 1},
    )

    history_after = int(instance.query(
        f"SELECT count() FROM system.iceberg_history WHERE database = 'default' AND table = '{TABLE_NAME}'"
    ).strip())
    assert history_after == 1, (
        f"With 1ms max age and min-keep=1, only current snapshot should remain; got {history_after}"
    )

    assert instance.query(f"SELECT * FROM {TABLE_NAME} ORDER BY x") == "1\n2\n3\n4\n5\n"
