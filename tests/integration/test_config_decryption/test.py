import os

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance("node1", main_configs=["configs/config.xml"])
node2 = cluster.add_instance("node2", main_configs=["configs/config.yaml"])


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


def check_node(started_cluster, node):
    assert (
        node.query(
            "select value from system.server_settings where name ='max_table_size_to_drop'"
        )
        == "60000000000\n"
    )
    assert (
        node.query(
            "select value from system.server_settings where name ='max_partition_size_to_drop'"
        )
        == "40000000000\n"
    )


def test_successful_decryption_xml(started_cluster):
    check_node(started_cluster, node1)


def test_successful_decryption_yaml(started_cluster):
    check_node(started_cluster, node2)
