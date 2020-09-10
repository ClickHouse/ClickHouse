# pylint: disable=unused-argument
# pylint: disable=redefined-outer-name

import pytest
from helpers.client import QueryRuntimeException
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node = cluster.add_instance('node', main_configs=["configs/config.d/text_log.xml"])


@pytest.fixture(scope='module')
def start_cluster():
    try:
        cluster.start()

        yield cluster
    finally:
        cluster.shutdown()


def test_basic(start_cluster):
    with pytest.raises(QueryRuntimeException):
        # generate log with "Error" level
        node.query('SELECT * FROM no_such_table')

    node.query('SYSTEM FLUSH LOGS')

    assert int(node.query("SELECT count() FROM system.text_log WHERE level = 'Trace'")) == 0
    assert int(node.query("SELECT count() FROM system.text_log WHERE level = 'Debug'")) == 0
    assert int(node.query("SELECT count() FROM system.text_log WHERE level = 'Information'")) >= 1
    assert int(node.query("SELECT count() FROM system.text_log WHERE level = 'Error'")) >= 1

# compare the event_time and event_time_microseconds and test
# that they are exactly equal upto their seconds parts.
def test_field_event_time_microseconds(start_cluster):
    with pytest.raises(QueryRuntimeException):
        # generate log with "Error" level
        node.query('SELECT * FROM no_such_table')
    node.query('SYSTEM FLUSH LOGS')
    equals_query = '''WITH (
                        (
                            SELECT event_time_microseconds
                            FROM system.text_log
                            ORDER BY event_time DESC
                            LIMIT 1
                        ) AS time_with_microseconds, 
                        (
                            SELECT event_time
                            FROM system.text_log
                            ORDER BY event_time DESC
                            LIMIT 1
                        ) AS time)
                    SELECT if(dateDiff('second', toDateTime(time_with_microseconds), toDateTime(time)) = 0, 'ok', 'fail') 
    '''
    assert 'ok\n' in node.query(equals_query)
