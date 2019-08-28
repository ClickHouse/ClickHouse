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

def test_check_client_logs_level(start_cluster):
    assert TSV(instance.query("SELECT toDateTime(1111111111)")) == TSV("2005-03-17 17:58:31\n")
