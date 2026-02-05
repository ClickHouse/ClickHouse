import pytest
import time

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry

cluster = ClickHouseCluster(__file__)

node = cluster.add_instance(
    "node",
    main_configs=["configs/config.d/storage_config.xml"],
    tmpfs=["/test_volatile:size=100M", "/test_persistent:size=100M"],
    stay_alive=True,
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def get_disk_for_part(node, table, part_name):
    return node.query(
        f"""
        SELECT disk_name FROM system.parts
        WHERE table = '{table}' AND name = '{part_name}' AND active = 1
        """
    ).strip()


def get_parts_on_disk(node, table, disk):
    return node.query(
        f"""
        SELECT name FROM system.parts
        WHERE table = '{table}' AND disk_name = '{disk}' AND active = 1
        ORDER BY name
        """
    ).strip().split("\n") if node.query(
        f"""
        SELECT count() FROM system.parts
        WHERE table = '{table}' AND disk_name = '{disk}' AND active = 1
        """
    ).strip() != "0" else []


def test_move_on_shutdown(started_cluster):
    """Test that parts are moved from volatile to persistent volume on shutdown."""
    node.query(
        """
        CREATE TABLE test_move (
            id UInt64,
            data String
        ) ENGINE = MergeTree()
        ORDER BY id
        SETTINGS storage_policy = 'move_on_shutdown_policy'
        """
    )

    try:
        # Insert data to create parts on volatile disk
        for i in range(3):
            node.query(f"INSERT INTO test_move VALUES ({i}, 'data{i}')")

        # Verify parts are on volatile disk
        volatile_parts = get_parts_on_disk(node, "test_move", "volatile")
        assert len(volatile_parts) == 3, f"Expected 3 parts on volatile, got {volatile_parts}"

        # Restart node (triggers shutdown move)
        node.restart_clickhouse()

        # After restart, parts should be on persistent disk
        persistent_parts = get_parts_on_disk(node, "test_move", "persistent")
        volatile_parts_after = get_parts_on_disk(node, "test_move", "volatile")

        assert len(persistent_parts) == 3, f"Expected 3 parts on persistent after restart, got {persistent_parts}"
        assert len(volatile_parts_after) == 0, f"Expected 0 parts on volatile after restart, got {volatile_parts_after}"

        # Verify data integrity
        result = node.query("SELECT count() FROM test_move").strip()
        assert result == "3", f"Expected 3 rows, got {result}"

    finally:
        node.query("DROP TABLE IF EXISTS test_move")


def test_move_on_shutdown_single_part(started_cluster):
    """Test that a single part is moved from volatile to persistent volume on shutdown."""
    node.query(
        """
        CREATE TABLE test_single (
            id UInt64,
            data String
        ) ENGINE = MergeTree()
        ORDER BY id
        SETTINGS storage_policy = 'move_on_shutdown_policy'
        """
    )

    try:
        # Insert data to create a single part
        node.query("INSERT INTO test_single VALUES (1, 'data1')")

        # Verify part is on volatile disk
        volatile_parts = get_parts_on_disk(node, "test_single", "volatile")
        assert len(volatile_parts) == 1

        # Restart node (triggers shutdown move)
        node.restart_clickhouse()

        # Part should be moved to persistent disk
        persistent_parts = get_parts_on_disk(node, "test_single", "persistent")
        volatile_parts_after = get_parts_on_disk(node, "test_single", "volatile")

        assert len(persistent_parts) == 1, f"Expected 1 part on persistent after restart, got {persistent_parts}"
        assert len(volatile_parts_after) == 0, f"Expected 0 parts on volatile after restart, got {volatile_parts_after}"

    finally:
        node.query("DROP TABLE IF EXISTS test_single")
