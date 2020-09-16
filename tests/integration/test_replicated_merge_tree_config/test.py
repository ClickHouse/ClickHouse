import pytest
from helpers.cluster import ClickHouseCluster
import logging

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance("node", main_configs=["configs/config.xml"], with_zookeeper=True)


@pytest.fixture(scope="module")
def start_cluster():
    try:
        logging.info("Starting cluster...")
        cluster.start()
        logging.info("Cluster started")

        yield cluster
    finally:
        cluster.shutdown()


@pytest.fixture(autouse=True)
def drop_table(start_cluster):
    yield
    for node in cluster.instances.values():
        node.query("DROP TABLE IF EXISTS test1")
        node.query("DROP TABLE IF EXISTS test2")


def test_replicated_merge_tree_settings(start_cluster):
    node.query("CREATE TABLE test1 (id Int64) ENGINE MergeTree ORDER BY id")
    node.query(
        "CREATE TABLE test2 (id Int64) ENGINE ReplicatedMergeTree('/clickhouse/test', 'test') ORDER BY id"
    )

    assert node.query("SHOW CREATE test1").strip().endswith("100")
    assert node.query("SHOW CREATE test2").strip().endswith("200")
