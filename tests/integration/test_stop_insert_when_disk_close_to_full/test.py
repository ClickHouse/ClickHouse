import pytest

from helpers.client import QueryRuntimeException
from helpers.cluster import ClickHouseCluster, ClickHouseInstance

cluster = ClickHouseCluster(__file__)

node = cluster.add_instance(
    "node",
    main_configs=["configs/config.d/storage_configuration.xml"],
    tmpfs=["/disk1:size=7M"],
    macros={"shard": 0, "replica": 1},
)


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_min_free_disk_settings(start_cluster):
    # min_free_disk_bytes_to_perform_insert (default 0)
    # min_free_disk_ratio_to_perform_insert (default 0.0)

    node.query("DROP TABLE IF EXISTS test_table")

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

    free_bytes = 7 * 1024 * 1024  # 7MB -- size of disk
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

    node.query("DROP TABLE test_table")

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

    node.query("DROP TABLE test_table")
    node.query("SET min_free_disk_ratio_to_perform_insert = 0.0")


def test_insert_stops_when_disk_full(start_cluster):
    node.query("DROP TABLE IF EXISTS test_table")

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

    count = 0

    # Insert data to fill up disk
    try:
        for _ in range(100000):
            node.query(
                "INSERT INTO test_table SELECT number, repeat('a', 1000 * 1000) FROM numbers(1)"
            )
            count += 1
    except QueryRuntimeException as e:
        assert "Could not perform insert" in str(e)
        assert "free bytes left in the disk space" in str(e)

    free_space = int(
        node.query("SELECT free_space FROM system.disks WHERE name = 'disk1'").strip()
    )
    assert (
        free_space <= min_free_bytes
    ), f"Free space ({free_space}) is less than min_free_bytes ({min_free_bytes})"

    rows = int(node.query("SELECT count() from test_table").strip())
    assert rows == count

    node.query("DROP TABLE test_table")
