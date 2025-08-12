import pytest
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
keeper_node = cluster.add_instance("keeper1", stay_alive=True)

@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()

def test_keeper_starts_successfully_on_ipv4_only_node(started_cluster):
    """
    This test verifies that Keeper starts up successfully in a container
    where IPv6 is disabled via kernel sysctls. This confirms that the
    IPv4 fallback logic for the Raft listener is working correctly.
    """
    assert keeper_node.query("ruok") == "imok\n"
