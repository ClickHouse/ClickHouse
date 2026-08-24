import logging

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance("node", main_configs=["configs/clusters.xml"])


@pytest.fixture(scope="module")
def start_cluster():
    try:
        logging.info("Starting cluster...")
        cluster.start()
        logging.info("Cluster started")

        yield cluster
    finally:
        cluster.shutdown()


@pytest.mark.parametrize("prefer_localhost_replica", [0, 1])
def test_distributed_table_with_alias(start_cluster, prefer_localhost_replica):
    node.query(
        """
        DROP TABLE IF EXISTS local;
        DROP TABLE IF EXISTS dist;
        CREATE TABLE local(`dummy` UInt8) ENGINE = MergeTree ORDER BY tuple();
        CREATE TABLE dist AS local ENGINE = Distributed(localhost_cluster, currentDatabase(), local);
    """
    )

    node.query(
        "WITH 'Hello' AS `alias` SELECT `alias` FROM dist GROUP BY `alias`;",
        settings={"prefer_localhost_replica": prefer_localhost_replica},
    )
