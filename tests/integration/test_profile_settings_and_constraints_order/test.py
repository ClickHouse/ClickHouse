import pytest

from helpers.client import QueryRuntimeException
from helpers.cluster import ClickHouseCluster
from helpers.test_tools import TSV

cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance("node1", user_configs=["configs/constraints_first.xml"])
node2 = cluster.add_instance("node2", user_configs=["configs/constraints_last.xml"])


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        yield cluster

    finally:
        cluster.shutdown()


def test_profile_settings_and_constraints_order(started_cluster):
    query = "SELECT name, readonly FROM system.settings WHERE name == 'log_queries'"
    expected = """\
log_queries	1"""

    settings = node1.query(
        query,
        user="test_profile_settings_and_constraints_order",
    )

    assert TSV(settings) == TSV(expected)

    settings = node2.query(
        query,
        user="test_profile_settings_and_constraints_order",
    )

    assert TSV(settings) == TSV(expected)
