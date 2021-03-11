import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance('node', main_configs=['configs/config_information.xml'])

@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()

def test_check_client_logs_level(start_cluster):
    logs = node.query_and_get_answer_with_error("SELECT 1", settings={"send_logs_level": 'trace'})[1]
    assert logs.count('Trace') != 0
