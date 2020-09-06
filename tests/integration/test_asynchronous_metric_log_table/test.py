import time

from helpers.cluster import ClickHouseCluster

# Tests that the event_time_microseconds field in system.asynchronous_metric_log table gets populated.
# asynchronous metrics are updated once every 60s by default. To make the test run faster, the setting
# asynchronous_metric_update_period_s is being set to 2s so that the metrics are populated faster and
# are available for querying during the test.
def test_asynchronous_metric_log():
    cluster = ClickHouseCluster(__file__)
    node1 = cluster.add_instance('node1', with_zookeeper=True, main_configs=['configs/asynchronous_metrics_update_period_s.xml'])
    try:
        cluster.start()
        node1.query("SET log_queries = 1;")
        node1.query("CREATE DATABASE replica;")
        query_create = '''CREATE TABLE replica.test
        (
           id Int64,
           event_time DateTime
        )
        Engine=MergeTree()
        PARTITION BY toYYYYMMDD(event_time)
        ORDER BY id;'''
        time.sleep(2)
        node1.query(query_create)
        node1.query('''INSERT INTO replica.test VALUES (1, now())''')
        node1.query("SYSTEM FLUSH LOGS;")
        node1.query("SELECT * FROM system.asynchronous_metrics LIMIT 10")
        assert "1\n" in node1.query('''SELECT count() from replica.test FORMAT TSV''')
        assert "ok\n" in node1.query("SELECT If((select count(event_time_microseconds)  from system.asynchronous_metric_log) > 0, 'ok', 'fail');")
    finally:
        cluster.shutdown()
