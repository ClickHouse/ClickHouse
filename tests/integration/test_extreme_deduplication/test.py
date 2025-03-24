import time

import pytest

from helpers.client import CommandRequest, QueryTimeoutExceedException
from helpers.cluster import ClickHouseCluster
from helpers.test_tools import TSV

cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance(
    "node1",
    main_configs=["configs/conf.d/merge_tree.xml", "configs/conf.d/remote_servers.xml"],
    with_zookeeper=True,
    macros={"layer": 0, "shard": 0, "replica": 1},
)
node2 = cluster.add_instance(
    "node2",
    main_configs=["configs/conf.d/merge_tree.xml", "configs/conf.d/remote_servers.xml"],
    with_zookeeper=True,
    macros={"layer": 0, "shard": 0, "replica": 2},
)
nodes = [node1, node2]


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        pass
        cluster.shutdown()


def test_deduplication_window_in_seconds(started_cluster):
    node = node1

    node1.query(
        """
        CREATE TABLE simple ON CLUSTER test_cluster (date Date, id UInt32)
        ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/simple', '{replica}') PARTITION BY toYYYYMM(date) ORDER BY id"""
    )

    node.query("INSERT INTO simple VALUES (0, 0)")
    time.sleep(1)
    node.query("INSERT INTO simple VALUES (0, 0)")  # deduplication works here
    node.query("INSERT INTO simple VALUES (0, 1)")
    assert TSV(node.query("SELECT count() FROM simple")) == TSV("2\n")

    # Wait for the cleanup thread.
    for i in range(100):
        time.sleep(5)

        if (
            TSV.toMat(
                node.query(
                    "SELECT count() FROM system.zookeeper WHERE path = '/clickhouse/tables/0/simple/blocks'"
                )
            )[0][0]
            <= "1"
        ):
            break
    else:
        raise Exception("The blocks from Keeper were not removed in time")

    node.query(
        "INSERT INTO simple VALUES (0, 0)"
    )  # Deduplication doesn't work here as the first hash node was deleted
    assert TSV.toMat(node.query("SELECT count() FROM simple"))[0][0] == "3"

    node1.query("""DROP TABLE simple ON CLUSTER test_cluster""")
