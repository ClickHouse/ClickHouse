import pytest

from helpers.cluster import ClickHouseCluster
# from helpers.test_tools import TSV

cluster = ClickHouseCluster(__file__)
configs = ["configs/config_on_cluster.xml"]
user_configs=["configs/settings_on_cluster.xml"]

node_1 = cluster.add_instance(
    name="node1",
    main_configs=configs,
    user_configs=user_configs,
    with_zookeeper=True,
    macros={"replica": "replica1", "shard": "shard1"},
    # stay_alive=True,
)
node_2 = cluster.add_instance(
    name="node2",
    main_configs=configs,
    user_configs=user_configs,
    macros={"replica": "replica2", "shard": "shard1"},
    with_zookeeper=True,
)
cluster_nodes = [node_1, node_2]

@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster

    except Exception as ex:
        print(ex)

    finally:
        cluster.shutdown()


def test_turn_off_on_cluster(
    start_cluster,
):
    node_1.query(
        "CREATE DATABASE repl_db ON CLUSTER 'test_cluster' ENGINE = Replicated('/test/repl_db', '{shard}', '{replica}');"
    )

    node_1.query(
        "CREATE TABLE repl_db.test_table ON CLUSTER 'test_cluster' (n Int64) ENGINE = MergeTree ORDER BY n;"
    )
