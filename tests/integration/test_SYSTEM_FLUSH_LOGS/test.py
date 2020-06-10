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

@pytest.fixture(scope='module')
def start_cluster():
    try:
        cluster.start()
        node.query('SYSTEM FLUSH LOGS')
        yield cluster
    finally:
        cluster.shutdown()

@pytest.mark.parametrize('table,exists', system_logs)
def test_system_logs(start_cluster, table, exists):
    q = 'SELECT * FROM {}'.format(table)
    if exists:
        node.query(q)
    else:
        assert "Table {} doesn't exist".format(table) in node.query_and_get_error(q)
