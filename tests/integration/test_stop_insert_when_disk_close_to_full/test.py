import pytest
import uuid

from helpers.client import QueryRuntimeException
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node = cluster.add_instance(
    "node",
    main_configs=["configs/config.d/storage_configuration.xml"],
    tmpfs=["/test_stop_insert_when_disk1:size=7M"],
)

node_query_log = cluster.add_instance(
    "node_query_log",
    main_configs=["configs/config.d/storage_configuration_query_log.xml"],
    tmpfs=["/test_stop_insert_when_disk1:size=7M"],
)

node_jbod = cluster.add_instance(
    "node_jbod",
    main_configs=["configs/config.d/storage_configuration.xml"],
    tmpfs=["/jbod_disk1:size=5M", "/jbod_disk2:size=10M"],
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

def test_jbod_disk_failover(start_cluster):
    """
    Test that with JBOD volumes, if the first disk doesn't meet the free space threshold,
    the insert will succeed by using the second disk that has enough space.
    """
    node_jbod.query("DROP TABLE IF EXISTS test_jbod SYNC")

    # Set threshold to 3 MiB - this will block jbod_disk1 (5M) but allow jbod_disk2 (10M)
    min_free_bytes = 3 * 1024 * 1024

    node_jbod.query(
        f"""
        CREATE TABLE test_jbod (
            id UInt32,
            data String
        ) ENGINE = MergeTree()
        ORDER BY id
        SETTINGS storage_policy = 'jbod_policy', min_free_disk_bytes_to_perform_insert = {min_free_bytes}
    """
    )

    node_jbod.query("SYSTEM STOP MERGES test_jbod")

    try:
        for _ in range(100000):
            node_jbod.query(
                "INSERT INTO test_jbod SELECT number, repeat('a', 10000) FROM numbers(1)"
            )
    except QueryRuntimeException:
        # Expected at some point jbod_disk1 will be full
        pass

    # Check that jbod_disk1 has less free space than threshold
    jbod_disk1_free = int(
        node_jbod.query("SELECT free_space FROM system.disks WHERE name = 'jbod_disk1'").strip()
    )

    # Now insert a larger part - it should fail on jbod_disk1 but succeed on jbod_disk2
    node_jbod.query(
        "INSERT INTO test_jbod SELECT number, repeat('b', 100000) FROM numbers(10)"
    )

    rows = int(node_jbod.query("SELECT count() FROM test_jbod WHERE data LIKE 'b%'").strip())
    assert rows == 10, f"Expected 10 rows with 'b' data, got {rows}"

    # Verify the new data went to jbod_disk2 (not jbod_disk1)
    # Check which disk has the latest part
    parts_info = node_jbod.query(
        "SELECT disk_name, rows FROM system.parts WHERE table = 'test_jbod' AND active ORDER BY modification_time DESC LIMIT 1"
    ).strip()

    assert "jbod_disk2" in parts_info, f"Expected latest part on jbod_disk2, got: {parts_info}"

    jbod_disk1_free_after = int(
        node_jbod.query("SELECT free_space FROM system.disks WHERE name = 'jbod_disk1'").strip()
    )
    assert jbod_disk1_free_after < min_free_bytes, \
        f"jbod_disk1 should still be below threshold ({jbod_disk1_free_after} < {min_free_bytes})"

    node_jbod.query("DROP TABLE test_jbod SYNC")
