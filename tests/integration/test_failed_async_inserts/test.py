
import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
# This test relies only on the per-query `max_memory_usage`, which is unaffected
# by the server-wide `additional_memory_tracking_per_thread` reservation (it is
# charged to the total tracker via `CurrentMemoryTracker::allocGlobal`, never the
# query tracker chain). Keep the setting at its default so the async-insert flush
# path (`AsynchronousInsertQueue` → `CompletedPipelineExecutor::execute`) also
# exercises the feature and confirms it leaves query-level accounting unaffected.
node = cluster.add_instance(
    "node",
    with_zookeeper=True,
    with_remote_database_disk=False,
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_failed_async_inserts(started_cluster):
    node = started_cluster.instances["node"]
    select_query = (
        "SELECT value FROM system.events WHERE event == 'FailedAsyncInsertQuery'"
    )
    failed_count = node.query(select_query).strip()
    original_failed_count = int(failed_count) if failed_count else 0

    node.query(
        "CREATE TABLE async_insert_30_10_2022 (id UInt32, s String) ENGINE = Memory"
    )
    node.query(
        "INSERT INTO async_insert_30_10_2022 SETTINGS async_insert = 1, max_memory_usage = 1000 VALUES ()",
        ignore_error=True,
    )
    node.query(
        "INSERT INTO async_insert_30_10_2022 SETTINGS async_insert = 1, max_memory_usage = 1000 VALUES ([1,2,3], 1)",
        ignore_error=True,
    )
    node.query(
        'INSERT INTO async_insert_30_10_2022 SETTINGS async_insert = 1, max_memory_usage = 1000 FORMAT JSONEachRow {"id" : 1} {"x"}',
        ignore_error=True,
    )
    node.query(
        "INSERT INTO async_insert_30_10_2022 SETTINGS async_insert = 1, max_memory_usage = 1000 VALUES (throwIf(4),'')",
        ignore_error=True,
    )

    failed_count = node.query(select_query).strip()
    current_failed_count = int(failed_count) if failed_count else 0
    assert current_failed_count - original_failed_count == 4

    node.query("DROP TABLE IF EXISTS async_insert_30_10_2022 SYNC")
