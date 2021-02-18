# pylint: disable=unused-argument
# pylint: disable=redefined-outer-name

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.client import QueryRuntimeException

cluster = ClickHouseCluster(__file__)

node = cluster.add_instance('node', config_dir='configs')

@pytest.fixture(scope='module')
def start_cluster():
    try:
        cluster.start()

        yield cluster
    finally:
        cluster.shutdown()

def test_basic(start_cluster):
    with pytest.raises(QueryRuntimeException):
        # generates log with "Error" level
        node.query('SELECT * FROM no_such_table')

    node.query('SYSTEM FLUSH LOGS')

    assert int(node.query("SELECT count() FROM system.text_log WHERE level = 'Trace'")) == 0
    assert int(node.query("SELECT count() FROM system.text_log WHERE level = 'Debug'")) == 0
    assert int(node.query("SELECT count() FROM system.text_log WHERE level = 'Information'")) >= 1
    assert int(node.query("SELECT count() FROM system.text_log WHERE level = 'Error'")) >= 1
