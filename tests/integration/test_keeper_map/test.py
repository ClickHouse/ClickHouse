import time

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.network import PartitionManager

test_recover_staled_replica_run = 1

cluster = ClickHouseCluster(__file__)

node = cluster.add_instance(
    "node",
    main_configs=["configs/enable_keeper_map.xml"],
    user_configs=[
        "configs/keeper_retries.xml",
        "configs/sync_insert.xml",],
    with_zookeeper=True,
    stay_alive=True,
    with_remote_database_disk=False,  # `test_keeper_map_without_zk` stops the Keeper connection, which might not work with the remote DB disk
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


def get_genuine_zk():
    return cluster.get_kazoo_client("zoo1")


def remove_children(client, path):
    children = client.get_children(path)

    for child in children:
        child_path = f"{path}/{child}"
        remove_children(client, child_path)
        client.delete(child_path)


def assert_keeper_exception_after_partition(query):
    with PartitionManager() as pm:
        pm.drop_instance_zk_connections(node)
        try:
            error = node.query_and_get_error_with_retry(
                query,
                sleep_time=1,
            )
            assert "Coordination::Exception" in error
        except:
            raise


def run_query(query):
    try:
        result = node.query_with_retry(query, sleep_time=1)
        return result
    except:
        raise


def test_keeper_map_without_zk(started_cluster):
    run_query("DROP TABLE IF EXISTS test_keeper_map_without_zk SYNC")
    assert_keeper_exception_after_partition(
        "CREATE TABLE test_keeper_map_without_zk (key UInt64, value UInt64) ENGINE = KeeperMap('/test_keeper_map_without_zk') PRIMARY KEY(key);"
    )

    run_query(
        "CREATE TABLE test_keeper_map_without_zk (key UInt64, value UInt64) ENGINE = KeeperMap('/test_keeper_map_without_zk') PRIMARY KEY(key);"
    )

    assert_keeper_exception_after_partition(
        "INSERT INTO test_keeper_map_without_zk VALUES (1, 11)"
    )
    run_query("INSERT INTO test_keeper_map_without_zk VALUES (1, 11)")

    assert_keeper_exception_after_partition("SELECT * FROM test_keeper_map_without_zk")
    assert run_query("SELECT * FROM test_keeper_map_without_zk") == "1\t11\n"

    with PartitionManager() as pm:
        pm.drop_instance_zk_connections(node)
        node.restart_clickhouse(60)
        try:
            error = node.query_and_get_error_with_retry(
                "SELECT * FROM test_keeper_map_without_zk",
                sleep_time=1,
            )
            assert "Failed to activate table because of connection issues" in error
        except:
            raise

    run_query("SELECT * FROM test_keeper_map_without_zk")

    client = get_genuine_zk()
    remove_children(client, "/test_keeper_map/test_keeper_map_without_zk")
    node.restart_clickhouse(60)
    error = node.query_and_get_error_with_retry(
        "SELECT * FROM test_keeper_map_without_zk"
    )
    assert "Failed to activate table because of invalid metadata in ZooKeeper" in error

    client.stop()


def test_keeper_map_with_failed_drop(started_cluster):
    run_query("DROP TABLE IF EXISTS test_keeper_map_with_failed_drop SYNC")
    run_query("DROP TABLE IF EXISTS test_keeper_map_with_failed_drop_another SYNC")
    run_query(
        "CREATE TABLE test_keeper_map_with_failed_drop (key UInt64, value UInt64) ENGINE = KeeperMap('/test_keeper_map_with_failed_drop') PRIMARY KEY(key);"
    )

    run_query("INSERT INTO test_keeper_map_with_failed_drop VALUES (1, 11)")
    run_query("SYSTEM ENABLE FAILPOINT keepermap_fail_drop_data")
    node.query("DROP TABLE test_keeper_map_with_failed_drop SYNC")

    zk_client = get_genuine_zk()
    assert (
        zk_client.get("/test_keeper_map/test_keeper_map_with_failed_drop/data")
        is not None
    )

    run_query("SYSTEM DISABLE FAILPOINT keepermap_fail_drop_data")
    run_query(
        "CREATE TABLE test_keeper_map_with_failed_drop_another (key UInt64, value UInt64) ENGINE = KeeperMap('/test_keeper_map_with_failed_drop') PRIMARY KEY(key);"
    )

def test_keeper_drop_after_update(started_cluster):
    run_query("DROP TABLE IF EXISTS test_keeper_drop_after_update SYNC")
    run_query(
        "CREATE TABLE test_keeper_drop_after_update (key UInt64, value UInt64) ENGINE = KeeperMap('/test_keeper_drop_after_update') PRIMARY KEY(key);"
    )

    zk_client = get_genuine_zk()
    assert (
        zk_client.delete("/test_keeper_map/test_keeper_drop_after_update/metadata/drop_lock_version")
        is not None
    )

    run_query("DROP TABLE test_keeper_drop_after_update SYNC")

    # The data might not be immediately visible as removed by an external client
    # connected to a different Keeper node due to replication lag in the 3-node cluster.
    for _ in range(10):
        if zk_client.exists("/test_keeper_map/test_keeper_drop_after_update/data") is None:
            break
        time.sleep(0.5)

    assert (
        zk_client.exists("/test_keeper_map/test_keeper_drop_after_update/data")
        is None
    )


def test_keeper_map_create_without_drop_lock_version(started_cluster):
    """Test that CREATE TABLE succeeds when leftover ZK nodes from a failed drop
    are missing the drop_lock_version node (simulates pre-25.1 upgrade scenario).
    Regression test for https://github.com/ClickHouse/ClickHouse/issues/101581"""

    table_name = "test_keeper_map_create_without_drop_lock_version"
    zk_path = f"/test_keeper_map/{table_name}"

    run_query(f"DROP TABLE IF EXISTS {table_name} SYNC")
    run_query(
        f"CREATE TABLE {table_name} (key UInt64, value UInt64) ENGINE = KeeperMap('/{table_name}') PRIMARY KEY(key);"
    )
    run_query(f"INSERT INTO {table_name} VALUES (1, 11)")

    # Simulate a failed drop that leaves ZK nodes behind
    run_query("SYSTEM ENABLE FAILPOINT keepermap_fail_drop_data")
    node.query(f"DROP TABLE {table_name} SYNC")
    run_query("SYSTEM DISABLE FAILPOINT keepermap_fail_drop_data")

    # Verify leftover state: dropped marker and drop_lock_version exist
    zk_client = get_genuine_zk()
    assert zk_client.exists(f"{zk_path}/metadata/dropped") is not None
    assert zk_client.exists(f"{zk_path}/metadata/drop_lock_version") is not None

    # Delete drop_lock_version to simulate pre-25.1 leftover state
    zk_client.delete(f"{zk_path}/metadata/drop_lock_version")
    assert zk_client.exists(f"{zk_path}/metadata/drop_lock_version") is None

    # CREATE TABLE on the same path should succeed
    run_query(
        f"CREATE TABLE {table_name} (key UInt64, value UInt64) ENGINE = KeeperMap('/{table_name}') PRIMARY KEY(key);"
    )

    # Verify the table is usable
    run_query(f"INSERT INTO {table_name} VALUES (2, 22)")
    assert run_query(f"SELECT key, value FROM {table_name} ORDER BY key").strip() == "2\t22"

    run_query(f"DROP TABLE {table_name} SYNC")


def test_parenthesized_primary_key_metadata_compatibility(started_cluster):
    table_name = "test_keeper_map_parenthesized_primary_key"
    second_table_name = f"{table_name}_second"
    writer_table_name = f"{table_name}_writer"
    delimiter_table_name = f"{table_name}_delimiter"
    delimiter_second_table_name = f"{delimiter_table_name}_second"
    bad_table_name = f"{table_name}_bad"
    malformed_table_name = f"{table_name}_malformed"
    zk_path = f"/test_keeper_map/{table_name}/metadata"

    table_names = (
        malformed_table_name,
        bad_table_name,
        second_table_name,
        table_name,
        writer_table_name,
        delimiter_second_table_name,
        delimiter_table_name,
    )
    for name in table_names:
        run_query(f"DROP TABLE IF EXISTS {name} SYNC")

    zk_client = None
    legacy_metadata = None
    try:
        run_query(
            f"CREATE TABLE {table_name} (key UInt64, value String) "
            f"ENGINE = KeeperMap('/{table_name}') PRIMARY KEY key"
        )

        zk_client = get_genuine_zk()
        metadata = zk_client.get(zk_path)[0]
        assert b"primary key: key\n" in metadata

        legacy_metadata = metadata.replace(
            b"primary key: key\n", b"primary key: (key)\n"
        )
        assert legacy_metadata != metadata
        zk_client.set(zk_path, legacy_metadata)

        run_query(
            f"CREATE TABLE {second_table_name} (key UInt64, value String) "
            f"ENGINE = KeeperMap('/{table_name}') PRIMARY KEY key"
        )

        run_query(f"INSERT INTO {table_name} VALUES (1, 'value')")
        assert run_query(f"SELECT * FROM {second_table_name}") == "1\tvalue\n"

        error = node.query_and_get_error(
            f"CREATE TABLE {bad_table_name} (key UInt64, value String) "
            f"ENGINE = KeeperMap('/{table_name}') PRIMARY KEY value"
        )
        assert "stored primary key definition doesn't match" in error

        malformed_metadata = legacy_metadata.replace(
            b"primary key: (key)\n", b"primary key: (key\n"
        )
        zk_client.set(zk_path, malformed_metadata)
        error = node.query_and_get_error(
            f"CREATE TABLE {malformed_table_name} (key UInt64, value String) "
            f"ENGINE = KeeperMap('/{table_name}') PRIMARY KEY key"
        )
        assert "stored primary key definition doesn't match" in error

        zk_client.set(zk_path, legacy_metadata)
        run_query(
            f"CREATE TABLE {writer_table_name} (key UInt64, value String) "
            f"ENGINE = KeeperMap('/{writer_table_name}') PRIMARY KEY(key)"
        )
        writer_metadata = zk_client.get(
            f"/test_keeper_map/{writer_table_name}/metadata"
        )[0]
        assert b"primary key: key\n" in writer_metadata

        primary_key_with_header_text = (
            "sipHash64(concat(toString(key), 'primary key: '))"
        )
        run_query(
            f"CREATE TABLE {delimiter_table_name} (key UInt64, value String) "
            f"ENGINE = KeeperMap('/{delimiter_table_name}') "
            f"PRIMARY KEY {primary_key_with_header_text}"
        )
        run_query(
            f"CREATE TABLE {delimiter_second_table_name} "
            f"(key UInt64 COMMENT 'comment added later', value String) "
            f"ENGINE = KeeperMap('/{delimiter_table_name}') "
            f"PRIMARY KEY {primary_key_with_header_text}"
        )
    finally:
        if zk_client is not None:
            if legacy_metadata is not None and zk_client.exists(zk_path) is not None:
                zk_client.set(zk_path, legacy_metadata)
            zk_client.stop()
        for name in table_names:
            node.query(f"DROP TABLE IF EXISTS {name} SYNC")
