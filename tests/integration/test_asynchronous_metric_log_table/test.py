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


# Tests that the event_time_microseconds field in system.asynchronous_metric_log table gets populated.
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
        # query assumes that the event_time field is accurate
        equals_query = """WITH (
                            (
                                SELECT event_time_microseconds
                                FROM system.asynchronous_metric_log
                                ORDER BY event_time DESC
                                LIMIT 1
                            ) AS time_with_microseconds,
                            (
                                SELECT event_time
                                FROM system.asynchronous_metric_log
                                ORDER BY event_time DESC
                                LIMIT 1
                            ) AS time)
                        SELECT if(dateDiff('second', toDateTime(time_with_microseconds), toDateTime(time)) = 0, 'ok', 'fail')"""
        assert "ok\n" in node1.query(equals_query)
    finally:
        cluster.shutdown()
