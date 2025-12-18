"""
Test that the cleanup thread continues to work after ZooKeeper session expiry.

This test verifies the fix for a race condition where the cleanup thread could
stop permanently if:
1. A ZK operation throws ZSESSIONEXPIRED during cleanup
2. The cleanup thread returns early without rescheduling
3. ZK reconnects before the restarting thread runs
4. The restarting thread sees the table is active and returns early
5. The cleanup thread is never restarted

The fix ensures the cleanup thread always reschedules itself even on ZSESSIONEXPIRED.
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


def get_cleanup_thread_last_run_time(node, table_name):
    """Get the last cleanup attempt time for outdated parts of a table."""
    result = node.query(
        f"""
        SELECT max(last_removal_attempt_time)
        FROM system.parts
        WHERE table = '{table_name}' AND NOT active
        """
    ).strip()
    return result


def get_outdated_parts_count(node, table_name):
    """Get the count of outdated parts for a table."""
    result = node.query(
        f"""
        SELECT count()
        FROM system.parts
        WHERE table = '{table_name}' AND NOT active
        """
    ).strip()
    return int(result) if result else 0


def get_parts_never_seen_by_cleanup(node, table_name):
    """Get count of parts that were never seen by the cleanup thread."""
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

    This tests the fix for the race condition where cleanup thread could stop
    permanently after ZSESSIONEXPIRED if ZK reconnects quickly.
    """
    retry_number = get_retry_number(request)
    table_name = f"test_cleanup_zk_expiry_{retry_number}"

    # Create table with aggressive cleanup settings
    node1.query(
        f"""
        CREATE TABLE {table_name} (date Date, id UInt32)
        ENGINE = ReplicatedMergeTree('/clickhouse/tables/test/{table_name}', 'node1')
        ORDER BY id
        PARTITION BY toYYYYMM(date)
        SETTINGS
            old_parts_lifetime = 2,
            cleanup_delay_period = 1,
            cleanup_delay_period_random_add = 0,
            cleanup_thread_preferred_points_per_iteration = 0,
            max_cleanup_delay_period = 5
        """
    )

    try:
        # Insert initial data to create parts
        node1.query(
            f"INSERT INTO {table_name} VALUES ('2024-01-01', 1), ('2024-01-02', 2)"
        )
        node1.query(
            f"INSERT INTO {table_name} VALUES ('2024-01-01', 3), ('2024-01-02', 4)"
        )

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

        # Wait for cleanup to process the outdated parts at least once
        time.sleep(3)

        # Record the state before ZK interruption
        outdated_before = get_outdated_parts_count(node1, table_name)
        print(f"Outdated parts before ZK interruption: {outdated_before}")

        # Briefly drop ZK connections to trigger ZSESSIONEXPIRED in cleanup thread
        # Then quickly restore to simulate the race condition
        with PartitionManager() as pm:
            pm.drop_instance_zk_connections(node1)
            # Wait just long enough for session to expire but not too long
            time.sleep(3)
            pm.restore_instance_zk_connections(node1)

        # Wait for ZK to reconnect
        time.sleep(2)

        # Now insert more data to create new outdated parts
        # If the cleanup thread stopped, these parts won't be cleaned up
        for i in range(3):
            node1.query(
                f"INSERT INTO {table_name} VALUES ('2024-02-01', {100 + i})"
            )

        # Merge to create more outdated parts
        node1.query(f"OPTIMIZE TABLE {table_name} FINAL")

        # Wait for new outdated parts to be created
        time.sleep(2)

        # Check that new outdated parts were created
        outdated_after_insert = get_outdated_parts_count(node1, table_name)
        print(f"Outdated parts after new inserts: {outdated_after_insert}")

        # The key test: wait for cleanup thread to process the new parts
        # If the bug exists, the cleanup thread won't run and parts won't be cleaned
        # If the fix works, the cleanup thread will continue and clean up parts

        # Wait for cleanup to run (with margin for the max_cleanup_delay_period)
        max_wait_time = 15  # seconds
        start_time = time.time()

        while time.time() - start_time < max_wait_time:
            # Check if new parts have been seen by cleanup thread
            never_seen = get_parts_never_seen_by_cleanup(node1, table_name)
            if never_seen == 0:
                print(f"All parts have been seen by cleanup thread after {time.time() - start_time:.1f}s")
                break
            time.sleep(1)

        # Verify that the cleanup thread is still working
        # All outdated parts should have been seen by the cleanup thread
        never_seen_final = get_parts_never_seen_by_cleanup(node1, table_name)
        print(f"Parts never seen by cleanup thread: {never_seen_final}")

        # If the bug exists, never_seen_final would be > 0 (the new parts after ZK reconnect)
        # If the fix works, never_seen_final should be 0 (all parts processed)
        assert never_seen_final == 0, (
            f"Cleanup thread stopped processing parts after ZK session expiry! "
            f"{never_seen_final} parts were never seen by cleanup thread."
        )

        # Additional check: wait for parts to actually be removed
        # This confirms cleanup is fully functional, not just marking parts as seen
        assert_eq_with_retry(
            node1,
            f"SELECT count() FROM system.parts WHERE table = '{table_name}' AND active",
            "1",
            retry_count=30,
            sleep_time=1,
        )

        print("Test passed: Cleanup thread continues to work after ZK session expiry")

    finally:
        node1.query(f"DROP TABLE IF EXISTS {table_name} SYNC")


def test_cleanup_thread_recovers_after_prolonged_zk_outage(start_cluster, request):
    """
    Test that the cleanup thread recovers after a prolonged ZK outage.

    This is a more aggressive test where ZK is down for longer,
    ensuring the cleanup thread properly reschedules and recovers.
    """
    retry_number = get_retry_number(request)
    table_name = f"test_cleanup_prolonged_zk_outage_{retry_number}"

    node1.query(
        f"""
        CREATE TABLE {table_name} (date Date, id UInt32)
        ENGINE = ReplicatedMergeTree('/clickhouse/tables/test/{table_name}', 'node1')
        ORDER BY id
        PARTITION BY toYYYYMM(date)
        SETTINGS
            old_parts_lifetime = 2,
            cleanup_delay_period = 1,
            cleanup_delay_period_random_add = 0,
            cleanup_thread_preferred_points_per_iteration = 0,
            max_cleanup_delay_period = 5
        """
    )

    try:
        # Create initial parts
        node1.query(f"INSERT INTO {table_name} VALUES ('2024-01-01', 1)")
        node1.query(f"INSERT INTO {table_name} VALUES ('2024-01-01', 2)")
        node1.query(f"OPTIMIZE TABLE {table_name} FINAL")

        # Wait for outdated parts and cleanup to run once
        time.sleep(4)

        # Prolonged ZK outage
        with PartitionManager() as pm:
            pm.drop_instance_zk_connections(node1)
            # Longer outage
            time.sleep(10)
            pm.restore_instance_zk_connections(node1)

        # Wait for recovery
        time.sleep(3)

        # Create more parts after recovery
        node1.query(f"INSERT INTO {table_name} VALUES ('2024-02-01', 10)")
        node1.query(f"INSERT INTO {table_name} VALUES ('2024-02-01', 20)")
        node1.query(f"OPTIMIZE TABLE {table_name} FINAL")

        # Verify cleanup thread is working by checking parts are eventually cleaned
        assert_eq_with_retry(
            node1,
            f"SELECT count() FROM system.parts WHERE table = '{table_name}' AND active",
            "1",
            retry_count=60,
            sleep_time=1,
        )

        print("Test passed: Cleanup thread recovers after prolonged ZK outage")

    finally:
        node1.query(f"DROP TABLE IF EXISTS {table_name} SYNC")
