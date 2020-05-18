import pytest
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node_default = cluster.add_instance('node_default')
# main_configs is mandatory ,since system_tables_lazy_load will be read earlier then parsing of config_lazy.xml
node_lazy = cluster.add_instance('node_lazy', config_dir='configs', main_configs=['configs/config_lazy.xml'])

system_logs = [
    # disabled by default
    # ('system.part_log'),
    # ('system.text_log'),

    # enabled by default
    ('system.query_log'),
    ('system.query_thread_log'),
    ('system.trace_log'),
    ('system.metric_log'),
]

@pytest.fixture(scope='module')
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()

@pytest.mark.parametrize('table', system_logs)
def test_system_table(start_cluster, table):
    node_default.query('SELECT * FROM {}'.format(table))
    assert "Table {} doesn't exist".format(table) in node_lazy.query_and_get_error('SELECT * FROM {}'.format(table))
