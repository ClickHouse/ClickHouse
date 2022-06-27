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


# Tests that the system.asynchronous_metric_log table gets populated.
# asynchronous metrics are updated once every 60s by default. To make the test run faster, the setting
# asynchronous_metric_update_period_s is being set to 2s so that the metrics are populated faster and
# are available for querying during the test.
def test_event_time_microseconds_field(started_cluster):
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

        test_query = (
            "SELECT count() > 0 ? 'ok' : 'fail' FROM system.asynchronous_metric_log"
        )
        assert "ok\n" in node1.query(test_query)
    finally:
        cluster.shutdown()
