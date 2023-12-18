import time

import pytest
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance(
    "node1",
    with_zookeeper=True,
    main_configs=["configs/asynchronous_metrics_update_period_s.xml"],
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


def test_total_pk_bytes_in_memory_fields(started_cluster):
    try:
        cluster.start()
        node1.query("SET log_queries = 1;")
        node1.query("CREATE DATABASE replica;")
        query_create = """CREATE TABLE replica.test
        (
           id Int64,
           event_time DateTime
        )
        Engine=MergeTree()
        PARTITION BY toYYYYMMDD(event_time)
        ORDER BY id;"""
        time.sleep(2)
        node1.query(query_create)
        node1.query("""INSERT INTO replica.test VALUES (1, now())""")
        node1.query("SYSTEM FLUSH LOGS;")

        # query system.asynchronous_metrics
        test_query = "SELECT count() > 0 ? 'ok' : 'fail' FROM system.asynchronous_metrics WHERE metric='TotalPrimaryKeyBytesInMemory';"
        assert "ok\n" in node1.query(test_query)

        test_query = "SELECT count() > 0 ? 'ok' : 'fail' FROM system.asynchronous_metrics WHERE metric='TotalPrimaryKeyBytesInMemoryAllocated';"
        assert "ok\n" in node1.query(test_query)

        # query system.asynchronous_metric_log
        test_query = "SELECT count() > 0 ? 'ok' : 'fail' FROM system.asynchronous_metric_log WHERE metric='TotalPrimaryKeyBytesInMemory';"
        assert "ok\n" in node1.query(test_query)

        test_query = "SELECT count() > 0 ? 'ok' : 'fail' FROM system.asynchronous_metric_log WHERE metric='TotalPrimaryKeyBytesInMemoryAllocated';"
        assert "ok\n" in node1.query(test_query)

    finally:
        cluster.shutdown()
