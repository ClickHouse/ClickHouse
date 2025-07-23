import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import TSV

cluster = ClickHouseCluster(__file__)
configs = ["configs/config_on_cluster.xml"]
user_configs=["configs/settings_on_cluster.xml"]

node_ignore_on_cluster = cluster.add_instance(
    name="node1",
    main_configs=configs,
    user_configs=user_configs,
    with_zookeeper=True,
    macros={"replica": "replica1", "shard": "shard1"},
)
basic_node = cluster.add_instance(
    name="node2",
    main_configs=configs,
    macros={"replica": "replica2", "shard": "shard1"},
    with_zookeeper=True,
)
cluster_nodes = [node_ignore_on_cluster, basic_node]

@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster

    except Exception as ex:
        print(ex)

    finally:
        cluster.shutdown()


def perform_query(query: str):
    assert "ON CLUSTER is not allowed for Replicated database" in basic_node.query_and_get_error(
        query
    )
    node_ignore_on_cluster.query(query)

    basic_node.query("SYSTEM SYNC DATABASE REPLICA repl_db")


def test_turn_off_on_cluster(
    start_cluster,
):
    query_count = "SELECT count(*) FROM system.tables WHERE database='repl_db'"
    
    node_ignore_on_cluster.query(
        "CREATE DATABASE repl_db ON CLUSTER 'test_cluster' ENGINE = Replicated('/test/repl_db', '{shard}', '{replica}');"
    )

    query = "CREATE TABLE repl_db.test_table ON CLUSTER 'test_cluster' (n Int64) ENGINE = MergeTree ORDER BY n;"
    perform_query(query=query)
    assert TSV([1]) == node_ignore_on_cluster.query(query_count)
    assert TSV([1]) == basic_node.query(query_count)

    query = "DETACH TABLE repl_db.test_table ON CLUSTER 'test_cluster' PERMANENTLY"
    perform_query(query=query)
    assert TSV([0]) == node_ignore_on_cluster.query(query_count)
    assert TSV([0]) == basic_node.query(query_count)

    query = "ATTACH TABLE repl_db.test_table ON CLUSTER 'test_cluster'"
    perform_query(query=query)
    assert TSV([1]) == node_ignore_on_cluster.query(query_count)
    assert TSV([1]) == basic_node.query(query_count)

    query = "DROP TABLE repl_db.test_table ON CLUSTER 'test_cluster'"
    perform_query(query=query)
    assert TSV([0]) == node_ignore_on_cluster.query(query_count)
    assert TSV([0]) == basic_node.query(query_count)
