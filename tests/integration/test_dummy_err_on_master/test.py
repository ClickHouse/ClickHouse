import pytest
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance('node')

@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_drop_memory_database(start_cluster):
    assert node.query("SELECT value not like '22%' FROM system.build_options WHERE name = 'GIT_BRANCH'").strip() == "1"

