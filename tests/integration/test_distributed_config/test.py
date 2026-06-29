import logging

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node", main_configs=["configs/overrides.xml", "configs/clusters.xml"]
)


@pytest.fixture(scope="module")
def start_cluster():
    try:
        logging.info("Starting cluster...")
        cluster.start()
        logging.info("Cluster started")

        yield cluster
    finally:
        cluster.shutdown()


def test_distibuted_settings(start_cluster):
    node.query("")
    node.query(
        """
        CREATE TABLE data_1 (key Int) ENGINE Memory();
        CREATE TABLE dist_1 as data_1 ENGINE Distributed(localhost_cluster, default, data_1) SETTINGS flush_on_detach = true;
        SYSTEM STOP DISTRIBUTED SENDS dist_1;
        INSERT INTO dist_1 SETTINGS prefer_localhost_replica=0 VALUES (1);
        DETACH TABLE dist_1;
    """
    )
    assert "flush_on_detach = true" in node.query("SHOW CREATE dist_1")
    # flush_on_detach=true, so data_1 should have 1 row
    assert int(node.query("SELECT count() FROM data_1")) == 1

    node.query(
        """
        CREATE TABLE data_2 (key Int) ENGINE Memory();
        CREATE TABLE dist_2 as data_2 ENGINE Distributed(localhost_cluster, default, data_2);
        SYSTEM STOP DISTRIBUTED SENDS dist_2;
        INSERT INTO dist_2 SETTINGS prefer_localhost_replica=0 VALUES (2);
        DETACH TABLE dist_2;
    """
    )
    ## Settings are not added to CREATE (only specific one, like index_granularity for MergeTree)
    # assert "flush_on_detach = 0" in node.query("SHOW CREATE dist_2")

    # But settins are applied (flush_on_detach=false in config, so data_2 should not have any rows)
    assert int(node.query("SELECT count() FROM data_2")) == 0
