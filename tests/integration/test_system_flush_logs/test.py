# pylint: disable=line-too-long
# pylint: disable=unused-argument
# pylint: disable=redefined-outer-name

import pytest
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance('node_default')

system_logs = [
    # disabled by default
    ('system.part_log', 0),
    ('system.text_log', 0),

    # enabled by default
    ('system.query_log', 1),
    ('system.query_thread_log', 1),
    ('system.trace_log', 1),
    ('system.metric_log', 1),
]


@pytest.fixture(scope='module', autouse=True)
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


@pytest.fixture(scope='function')
def flush_logs():
    node.query('SYSTEM FLUSH LOGS')


@pytest.mark.parametrize('table,exists', system_logs)
def test_system_logs(flush_logs, table, exists):
    q = 'SELECT * FROM {}'.format(table)
    if exists:
        node.query(q)
    else:
        assert "Table {} doesn't exist".format(table) in node.query_and_get_error(q)


# Logic is tricky, let's check that there is no hang in case of message queue
# is not empty (this is another code path in the code).
def test_system_logs_non_empty_queue():
    node.query('SELECT 1', settings={
        # right now defaults are the same,
        # this set explicitly to avoid depends from defaults.
        'log_queries': 1,
        'log_queries_min_type': 'QUERY_START',
    })
    node.query('SYSTEM FLUSH LOGS')
