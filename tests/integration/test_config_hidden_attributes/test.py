import pytest
import os
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance("node", main_configs=["configs/config.xml"])


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


def test_hidden(started_cluster):
    assert (
        node.query(
            "select value from system.server_settings where name ='max_table_size_to_drop'"
        )
        == "60000000000\n"
    )
