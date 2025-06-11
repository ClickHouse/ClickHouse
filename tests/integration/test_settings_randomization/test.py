import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance("node1", user_configs=["config/users.xml"])


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


def test_settings_randomization(started_cluster):
    """
    See tests/integration/helpers/random_settings.py
    """

    def q(field, name):
        return int(
            node.query(
                f"SELECT {field} FROM system.settings WHERE name = '{name}'"
            ).strip()
        )

    # setting set in test config is not overriden
    assert q("value", "max_block_size") == 59999

    # some setting is randomized
    assert q("changed", "max_joined_block_size_rows") == 1
    assert 8000 <= q("value", "max_joined_block_size_rows") <= 100000
