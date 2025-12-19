"""
Test that the cleanup thread continues to work after ZooKeeper session expiry.
"""

import time
import pytest

from helpers.cluster import ClickHouseCluster
from helpers.network import PartitionManager
from helpers.test_tools import assert_eq_with_retry, get_retry_number

cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance(
    "node1",
    main_configs=["configs/remote_servers.xml"],
    with_zookeeper=True,
)


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster
    except Exception as ex:
        print(ex)
    finally:
        cluster.shutdown()


def get_parts_never_seen_by_cleanup(node, table_name):
    """Get count of outdated parts that were never seen by the cleanup thread."""
    result = node.query(
        f"""
        SELECT count()
        FROM system.parts
        WHERE table = '{table_name}'
          AND NOT active
          AND last_removal_attempt_time = toDateTime(0)
        """
    ).strip()
    return int(result) if result else 0


def test_cleanup_thread_continues_after_zk_reconnect(start_cluster, request):
    """
    Test that the cleanup thread continues to process parts after ZK session
    is briefly interrupted and then reconnected.
    """
    retry_number = get_retry_number(request)
    table_name = f"test_cleanup_zk_expiry_{retry_number}"

    node1.query(
        f"""
        CREATE TABLE {table_name} (id UInt32)
        ENGINE = ReplicatedMergeTree('/clickhouse/tables/test/{table_name}', 'node1')
        ORDER BY id
        SETTINGS
            old_parts_lifetime = 1,
            cleanup_delay_period = 1,
            cleanup_delay_period_random_add = 0,
            cleanup_thread_preferred_points_per_iteration = 0,
            max_cleanup_delay_period = 3
        """
    )

    try:
        # Insert data to create parts
        node1.query(f"INSERT INTO {table_name} VALUES (1)")
        node1.query(f"INSERT INTO {table_name} VALUES (2)")

        # Merge to create outdated parts
        node1.query(f"OPTIMIZE TABLE {table_name} FINAL")

        # Wait for outdated parts to appear
        assert_eq_with_retry(
            node1,
            f"SELECT count() > 0 FROM system.parts WHERE table = '{table_name}' AND NOT active",
            "1",
            retry_count=30,
            sleep_time=0.5,
        )

        # Briefly drop ZK connections to trigger ZSESSIONEXPIRED
        with PartitionManager() as pm:
            pm.drop_instance_zk_connections(node1)
            time.sleep(3)
            pm.restore_instance_zk_connections(node1)

        # Wait for ZK to reconnect
        time.sleep(2)

        # Insert more data after reconnection
        node1.query(f"INSERT INTO {table_name} VALUES (3)")
        node1.query(f"INSERT INTO {table_name} VALUES (4)")

        # Merge to create more outdated parts
        node1.query(f"OPTIMIZE TABLE {table_name} FINAL")

        # Wait for new outdated parts to be created
        time.sleep(1)

        # Wait for cleanup thread to process the new parts
        max_wait_time = 20
        start_time = time.time()

        while time.time() - start_time < max_wait_time:
            never_seen = get_parts_never_seen_by_cleanup(node1, table_name)
            if never_seen == 0:
                break
            time.sleep(1)

        # Verify that all outdated parts have been seen by the cleanup thread
        never_seen_final = get_parts_never_seen_by_cleanup(node1, table_name)
        assert never_seen_final == 0, (
            f"Cleanup thread stopped processing parts after ZK session expiry! "
            f"{never_seen_final} parts were never seen by cleanup thread."
        )

        # Verify cleanup completes by waiting for parts to be removed
        assert_eq_with_retry(
            node1,
            f"SELECT count() FROM system.parts WHERE table = '{table_name}' AND active",
            "1",
            retry_count=60,
            sleep_time=1,
        )

    finally:
        node1.query(f"DROP TABLE IF EXISTS {table_name} SYNC")


def test_cleanup_thread_recovers_after_prolonged_zk_outage(start_cluster, request):
    """
    Test that the cleanup thread recovers after a prolonged ZK outage.
    """
    retry_number = get_retry_number(request)
    table_name = f"test_cleanup_prolonged_zk_outage_{retry_number}"

    node1.query(
        f"""
        CREATE TABLE {table_name} (id UInt32)
        ENGINE = ReplicatedMergeTree('/clickhouse/tables/test/{table_name}', 'node1')
        ORDER BY id
        SETTINGS
            old_parts_lifetime = 1,
            cleanup_delay_period = 1,
            cleanup_delay_period_random_add = 0,
            cleanup_thread_preferred_points_per_iteration = 0,
            max_cleanup_delay_period = 3
        """
    )

    try:
        # Create initial parts
        node1.query(f"INSERT INTO {table_name} VALUES (1)")
        node1.query(f"INSERT INTO {table_name} VALUES (2)")
        node1.query(f"OPTIMIZE TABLE {table_name} FINAL")

        # Wait for cleanup to run once
        time.sleep(3)

        # Prolonged ZK outage
        with PartitionManager() as pm:
            pm.drop_instance_zk_connections(node1)
            time.sleep(10)
            pm.restore_instance_zk_connections(node1)

        # Wait for recovery
        time.sleep(3)

        # Create more parts after recovery
        node1.query(f"INSERT INTO {table_name} VALUES (10)")
        node1.query(f"INSERT INTO {table_name} VALUES (20)")
        node1.query(f"OPTIMIZE TABLE {table_name} FINAL")

        # Verify cleanup thread is working by checking parts are eventually cleaned
        assert_eq_with_retry(
            node1,
            f"SELECT count() FROM system.parts WHERE table = '{table_name}' AND active",
            "1",
            retry_count=90,
            sleep_time=1,
        )

    finally:
        node1.query(f"DROP TABLE IF EXISTS {table_name} SYNC")
