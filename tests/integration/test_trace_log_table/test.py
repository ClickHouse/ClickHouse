# pylint: disable=unused-argument
# pylint: disable=redefined-outer-name
import time

import pytest
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node = cluster.add_instance('node', with_zookeeper=True, main_configs=["configs/trace_log.xml"])


@pytest.fixture(scope='module')
def start_cluster():
    try:
        cluster.start()

        yield cluster
    finally:
        cluster.shutdown()


# Tests that the event_time_microseconds field in the system.trace_log table gets populated.
# To make the tests work better, the default flush_interval_milliseconds is being overridden
# to 1000 ms. Also the query_profiler_real_time_period_ns and the query_profiler_cpu_time_period_ns
# are set to suitable values so that traces are properly populated. Test compares the event_time and
# event_time_microseconds fields and asserts that they are exactly equal upto their seconds parts. Also
# one additional test to ensure that the count(event_time_microseconds) is > 0;
@pytest.mark.skip(reason="TODO: system.trace_log not populated in time on CI but works fine on dev")
def test_field_event_time_microseconds(start_cluster):
    node.query('SET query_profiler_real_time_period_ns = 0;')
    node.query('SET query_profiler_cpu_time_period_ns = 1000000;')
    node.query('SET log_queries = 1;')
    node.query("CREATE DATABASE replica;")
    query_create = '''CREATE TABLE replica.test
        (
           id Int64,
           event_time DateTime
        )
        Engine=MergeTree()
        PARTITION BY toYYYYMMDD(event_time)
        ORDER BY id;'''
    node.query(query_create)
    node.query('''INSERT INTO replica.test VALUES (1, now())''')
    node.query("SYSTEM FLUSH LOGS;")
    # TODO: is sleep necessary ?
    time.sleep(1)
    # query assumes that the event_time field is already accurate
    equals_query = '''WITH (
                        (
                            SELECT event_time_microseconds
                            FROM system.trace_log
                            ORDER BY event_time DESC
                            LIMIT 1
                        ) AS time_with_microseconds, 
                        (
                            SELECT event_time
                            FROM system.trace_log
                            ORDER BY event_time DESC
                            LIMIT 1
                        ) AS t)
                    SELECT if(dateDiff('second', toDateTime(time_with_microseconds), toDateTime(t)) = 0, 'ok', 'fail') 
    '''
    assert 'ok\n' in node.query(equals_query)
    assert 'ok\n' in node.query(
        '''SELECT if((SELECT COUNT(event_time_microseconds) FROM system.trace_log) > 0, 'ok', 'fail')''')
