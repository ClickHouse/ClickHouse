import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance('node', main_configs=['configs/config.xml'])

@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()

def test_check_timezone_config(start_cluster):
    assert node.query("SELECT toDateTime(1111111111)") == "2005-03-17 17:58:31\n"
