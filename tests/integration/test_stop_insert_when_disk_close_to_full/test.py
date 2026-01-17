import pytest
import uuid

from helpers.client import QueryRuntimeException
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node = cluster.add_instance(
    "node",
    main_configs=["configs/config.d/storage_configuration.xml"],
    tmpfs=["/disk1:size=7M"],
)

node_query_log = cluster.add_instance(
    "node_query_log",
    main_configs=["configs/config.d/storage_configuration_query_log.xml"],
    tmpfs=["/disk1:size=7M"],
)


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        node.query("SET database_atomic_wait_for_drop_and_detach_synchronously = 1")
        yield cluster
    finally:
        cluster.shutdown()


def test_min_free_disk_settings(start_cluster):
    # min_free_disk_bytes_to_perform_insert (default 0)
    # min_free_disk_ratio_to_perform_insert (default 0.0)

    node.query("DROP TABLE IF EXISTS test_table SYNC")

    node.query(
        f"""
        CREATE TABLE test_table (
            id UInt32,
            data String
        ) ENGINE = MergeTree()
        ORDER BY id
        SETTINGS storage_policy = 'only_disk1'
    """
    )

    node.query("INSERT INTO test_table (id, data) values (1, 'a')")

    free_bytes = int(node.query(
        "SELECT total_space FROM system.disks WHERE name = 'disk1'"
    ).strip())
    node.query(f"SET min_free_disk_bytes_to_perform_insert = {free_bytes}")

    try:
        node.query("INSERT INTO test_table (id, data) values (1, 'a')")
    except QueryRuntimeException as e:
        assert "NOT_ENOUGH_SPACE" in str(e)

    node.query("SET min_free_disk_bytes_to_perform_insert = 0")
    node.query("INSERT INTO test_table (id, data) values (1, 'a')")

    free_ratio = 1.0
    node.query(f"SET min_free_disk_ratio_to_perform_insert = {free_ratio}")

    try:
        node.query("INSERT INTO test_table (id, data) values (1, 'a')")
    except QueryRuntimeException as e:
        assert "NOT_ENOUGH_SPACE" in str(e)

    node.query("DROP TABLE test_table SYNC")

    # server setting for min_free_disk_ratio_to_perform_insert is 1 but we can overwrite at table level
    node.query(
        f"""
        CREATE TABLE test_table (
            id UInt32,
            data String
        ) ENGINE = MergeTree()
        ORDER BY id
        SETTINGS storage_policy = 'only_disk1', min_free_disk_ratio_to_perform_insert = 0.0
    """
    )

    node.query("INSERT INTO test_table (id, data) values (1, 'a')")

    node.query("DROP TABLE test_table SYNC")
    node.query("SET min_free_disk_ratio_to_perform_insert = 0.0")


def test_insert_stops_when_disk_full(start_cluster):
    node.query("DROP TABLE IF EXISTS test_table SYNC")

    min_free_bytes = 3 * 1024 * 1024  # 3 MiB

    node.query(
        f"""
        CREATE TABLE test_table (
            id UInt32,
            data String
        ) ENGINE = MergeTree()
        ORDER BY id
        SETTINGS storage_policy = 'only_disk1', min_free_disk_bytes_to_perform_insert = {min_free_bytes}
    """
    )
    node.query("SYSTEM STOP MERGES test_table")

    count = 0

    # Insert data to fill up disk
    try:
        for _ in range(100000):
            node.query(
                "INSERT INTO test_table SELECT number, repeat('a', 1000 * 1000) FROM numbers(1)"
            )
            count += 1
    except QueryRuntimeException as e:
        msg = str(e)
        assert any(s in msg for s in ("Could not perform insert", "Cannot reserve", "NOT_ENOUGH_SPACE"))
        assert "The amount of free space" in msg or "not enough space" in msg.lower()

    free_space = int(
        node.query("SELECT free_space FROM system.disks WHERE name = 'disk1'").strip()
    )
    assert (
        free_space <= (min_free_bytes + 1 * 1024 * 1024) # need to account for 1 MiB reservation made by the insert before it's rejected
    ), f"Free space ({free_space}) is less than min_free_bytes ({min_free_bytes})"

    rows = int(node.query("SELECT count() from test_table").strip())
    assert rows == count

    node.query("DROP TABLE test_table SYNC")


def test_system_query_log(start_cluster):
    # writing to system tables (e.g. system.query_log) should not be affected by min_free_disk_*_to_perform_insert settings
    query_id = str(uuid.uuid4())
    node_query_log.query("SELECT 1", query_id=query_id)
    node_query_log.query("SYSTEM FLUSH LOGS")
    assert (
        int(
            node_query_log.query(
                f"SELECT count() FROM system.query_log WHERE query_id = '{query_id}' AND type = 'QueryFinish'"
            )
        )
        == 1
    )
