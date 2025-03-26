import pytest
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance('node', main_configs=['configs/config.d/tcp_connection_limits.xml'])

@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()

def test_query_count_limit(started_cluster):
    # First 3 queries should succeed
    for i in range(1, 4):
        assert node.query(f"SELECT {i}") == f"{i}\n"

    # Fourth query should fail with connection closed
    with pytest.raises(Exception, match=r".*Connection closed by peer.*"):
        node.query("SELECT 4")

def test_time_limit(started_cluster):
    # First query should succeed
    assert node.query("SELECT 1") == "1\n"

    # Sleep for 3 seconds (longer than the 2-second limit)
    node.query("SELECT sleep(3)")

    # Next query should fail with connection closed
    with pytest.raises(Exception, match=r".*Connection closed by peer.*"):
        node.query("SELECT 2")
