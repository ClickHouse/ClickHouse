import time
import pytest
import logging
from helpers.cluster import ClickHouseCluster
from tests.integration.helpers.test_tools import TSV

cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance(
    "node1",
    with_zookeeper=True,
    main_configs=[
        "configs_keeper_server/remote_servers.xml",
        "configs_keeper_server/enable_keeper1.xml",
        "configs_keeper_server/use_keeper.xml",
    ],
    macros={"replica": "node1"},
)

node2 = cluster.add_instance(
    "node2",
    with_zookeeper=True,
    main_configs=[
        "configs_keeper_server/remote_servers.xml",
        "configs_keeper_server/enable_keeper2.xml",
    ],
    macros={"replica": "node2"},
)


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


def test_create_insert(started_cluster):
    node1.query("DROP TABLE IF EXISTS tbl ON CLUSTER 'test_cluster' NO DELAY")
    node1.query(
        """
        CREATE TABLE tbl ON CLUSTER 'test_cluster' (
            id Int64,
            str String
        ) ENGINE=ReplicatedMergeTree('/clickhouse/tables/tbl/', '{replica}')
        ORDER BY id
        """
    )

    node1.query("INSERT INTO tbl VALUES (1, 'str1')")
    node2.query("INSERT INTO tbl VALUES (1, 'str1')")  # Test deduplication
    node2.query("INSERT INTO tbl VALUES (2, 'str2')")

    expected = [[1, "str1"], [2, "str2"]]
    assert node1.query("SELECT * FROM tbl ORDER BY id") == TSV(expected)
    assert node2.query("SELECT * FROM tbl ORDER BY id") == TSV(expected)
    assert node1.query("CHECK TABLE tbl") == "1\n"
    assert node2.query("CHECK TABLE tbl") == "1\n"
