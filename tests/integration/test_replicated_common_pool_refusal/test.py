import time

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=["configs/background_pools.xml"],
    with_zookeeper=True,
)

COMMON_POOL_TASKS = (
    "SELECT value FROM system.metrics WHERE metric = 'BackgroundCommonPoolTask'"
)
VICTIM_QUEUE_SIZE = "SELECT count() FROM system.replication_queue WHERE database = currentDatabase() AND table = 'victim'"


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def wait_for(query, expected, timeout):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if node.query(query).strip() == expected:
            return True
        time.sleep(0.1)
    return False


def test_refused_drop_range_is_retried(started_cluster):
    for table in ["filler", "victim"]:
        node.query(f"DROP TABLE IF EXISTS {table} SYNC")
        node.query(
            f"CREATE TABLE {table} (x UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{table}', 'r1') ORDER BY x"
        )
        node.query(f"INSERT INTO {table} VALUES (1)")
        node.query(f"SYSTEM SYNC REPLICA {table}")

    # Every DROP_RANGE sleeps 10 s inside its common pool task.
    node.query("SYSTEM ENABLE FAILPOINT rmt_delay_execute_drop_range")
    try:
        node.query("TRUNCATE TABLE filler", settings={"alter_sync": 0})
        assert wait_for(COMMON_POOL_TASKS, "1", 10)

        node.query("TRUNCATE TABLE victim", settings={"alter_sync": 0})
        assert wait_for(VICTIM_QUEUE_SIZE, "1", 5)
        # The victim's DROP_RANGE became ready while the only slot was taken, so it was refused.
        assert node.query(COMMON_POOL_TASKS).strip() == "1"

        if not wait_for(VICTIM_QUEUE_SIZE, "0", 120):
            row = node.query(
                "SELECT type, num_tries, is_currently_executing, postpone_reason, last_exception "
                "FROM system.replication_queue WHERE database = currentDatabase() AND table = 'victim'"
            )
            pytest.fail(f"The victim's DROP_RANGE is still queued: {row}")
        assert node.query("SELECT count() FROM victim").strip() == "0"
    finally:
        node.query("SYSTEM DISABLE FAILPOINT rmt_delay_execute_drop_range")
