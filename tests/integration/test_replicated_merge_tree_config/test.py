import pytest
from helpers.cluster import ClickHouseCluster


@pytest.fixture(scope="module")
def cluster():
    try:
        cluster = ClickHouseCluster(__file__)
        cluster.add_instance(
            "node", config_dir="configs", with_zookeeper=True,
        )
        logging.info("Starting cluster...")
        cluster.start()
        logging.info("Cluster started")

        yield cluster
    finally:
        cluster.shutdown()


@pytest.fixture(autouse=True)
def drop_table(cluster):
    yield
    for node in cluster.instances.values():
        node.query("DROP TABLE IF EXISTS test1")
        node.query("DROP TABLE IF EXISTS test2")


def test_replicated_merge_tree_settings(cluster):
    node = cluster.instances["node"]
    node.query("CREATE TABLE test1 (id Int64) ENGINE MergeTree ORDER BY id")
    node.query(
        "CREATE TABLE test2 (id Int64) ENGINE ReplicatedMergeTree('/clickhouse/test', 'test') ORDER BY id"
    )

    assert node.query("SHOW CREATE test1").endswith("100")
    assert node.query("SHOW CREATE test2").endswith("200")
