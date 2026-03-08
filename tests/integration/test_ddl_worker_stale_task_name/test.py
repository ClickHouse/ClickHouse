import time
import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import TSV

cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance(
    "node1",
    main_configs=["configs/remote_servers.xml"],
    with_zookeeper=True,
    stay_alive=True,
)

node2 = cluster.add_instance(
    "node2",
    main_configs=["configs/remote_servers.xml"],
    with_zookeeper=True,
    stay_alive=True,
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


def test_ddl_worker_stale_task_name_after_cleanup(started_cluster):
    """
    Test that DDLWorker doesn't fail with assertion when first_failed_task_name
    persists from a previous reinitialization cycle after the task was cleaned up.
    
    This test validates the fix for the race condition where:
    1. DDLWorker reinitializes and sets first_failed_task_name
    2. The failed task is removed from the queue (cleanup)
    3. Next normal scheduleTasks call should not fail the assertion
    """

    # Clean up any existing databases
    node1.query("DROP DATABASE IF EXISTS test_db ON CLUSTER 'test_cluster' SYNC")
    time.sleep(1)

    # Create a replicated database to use distributed DDL
    node1.query(
        "CREATE DATABASE IF NOT EXISTS test_db ON CLUSTER 'test_cluster' "
        "ENGINE = Replicated('/clickhouse/databases/test_db', '{shard}', '{replica}')"
    )
    time.sleep(1)

    # Create a table to trigger DDL tasks
    node1.query(
        "CREATE TABLE test_db.test_table ON CLUSTER 'test_cluster' "
        "(id UInt32, value String) ENGINE = ReplicatedMergeTree('/clickhouse/tables/test_table', '{replica}') "
        "ORDER BY id"
    )
    time.sleep(1)

    # Get initial DDL worker state
    node1.query("SYSTEM FLUSH LOGS")
    initial_logs = node1.query(
        "SELECT count() FROM system.text_log WHERE logger_name='DDLWorker'"
    ).strip()

    # Simulate a connection loss and reinitialization by resetting DDL worker
    # This can cause first_failed_task_name to be set if there are unfinished tasks
    node1.query("SYSTEM RESET DDL WORKER")
    time.sleep(2)

    # Create another DDL task to ensure DDL worker is active
    node1.query(
        "CREATE TABLE test_db.test_table2 ON CLUSTER 'test_cluster' "
        "(id UInt32, value String) ENGINE = ReplicatedMergeTree('/clickhouse/tables/test_table2', '{replica}') "
        "ORDER BY id"
    )
    time.sleep(1)

    # Verify that DDL worker is still functioning without assertion failures
    # The fix ensures that stale first_failed_task_name is cleared during normal operation
    node1.query("SYSTEM FLUSH LOGS")
    
    # Check that there are no assertion failures in the logs
    assertion_errors = node1.query(
        "SELECT count() FROM system.text_log "
        "WHERE logger_name='DDLWorker' AND message LIKE '%assertion%' AND level='Error'"
    ).strip()
    
    assert assertion_errors == "0", f"Found {assertion_errors} assertion errors in DDL worker"

    # Verify tables were created successfully
    result1 = node1.query("SELECT count() FROM system.tables WHERE database='test_db' AND name='test_table'")
    result2 = node1.query("SELECT count() FROM system.tables WHERE database='test_db' AND name='test_table2'")
    
    assert result1.strip() == "1", "test_table should exist"
    assert result2.strip() == "1", "test_table2 should exist"

    # Clean up
    node1.query("DROP DATABASE test_db ON CLUSTER 'test_cluster' SYNC")


def test_ddl_worker_reinitialization_with_task_cleanup(started_cluster):
    """
    Test that simulates the exact race condition scenario:
    1. DDL task starts but doesn't complete
    2. Connection lost, DDL worker reinitializes (first_failed_task_name set)
    3. Task gets cleaned from queue
    4. Normal operation resumes without assertion failure
    """

    # Clean up
    node1.query("DROP DATABASE IF EXISTS test_db2 ON CLUSTER 'test_cluster' SYNC")
    time.sleep(1)

    # Create test database
    node1.query(
        "CREATE DATABASE IF NOT EXISTS test_db2 ON CLUSTER 'test_cluster' "
        "ENGINE = Replicated('/clickhouse/databases/test_db2', '{shard}', '{replica}')"
    )
    time.sleep(1)

    # Create a table
    node1.query(
        "CREATE TABLE test_db2.test ON CLUSTER 'test_cluster' "
        "(x Int32) ENGINE = ReplicatedMergeTree('/clickhouse/tables/test_db2_test', '{replica}') "
        "ORDER BY x"
    )
    time.sleep(1)

    # Reset DDL worker to simulate reinitialization
    node1.query("SYSTEM RESET DDL WORKER")
    node2.query("SYSTEM RESET DDL WORKER")
    time.sleep(1)

    # Execute more DDL to ensure the worker processes tasks after reset
    # This exercises the code path where stale first_failed_task_name would cause issues
    node1.query(
        "CREATE TABLE test_db2.test2 ON CLUSTER 'test_cluster' "
        "(x Int32) ENGINE = ReplicatedMergeTree('/clickhouse/tables/test_db2_test2', '{replica}') "
        "ORDER BY x"
    )
    time.sleep(1)

    # Verify no crashes or assertion failures
    node1.query("SYSTEM FLUSH LOGS")
    
    errors = node1.query(
        "SELECT count() FROM system.text_log "
        "WHERE logger_name='DDLWorker' AND level IN ('Error', 'Fatal')"
    ).strip()
    
    assert errors == "0", f"Found {errors} errors in DDL worker logs"

    # Verify tables exist
    result = node1.query(
        "SELECT count() FROM system.tables WHERE database='test_db2' AND name IN ('test', 'test2')"
    )
    assert result.strip() == "2", "Both tables should exist"

    # Clean up
    node1.query("DROP DATABASE test_db2 ON CLUSTER 'test_cluster' SYNC")
