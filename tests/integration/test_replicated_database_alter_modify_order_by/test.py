import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry

cluster = ClickHouseCluster(__file__)

shard1_node = cluster.add_instance(
    "shard1_node",
    main_configs=["configs/config.xml"],
    user_configs=["configs/settings.xml"],
    with_zookeeper=True,
    stay_alive=True,
    macros={"shard": 1, "replica": 1},
)

shard2_node = cluster.add_instance(
    "shard2_node",
    main_configs=["configs/config.xml"],
    user_configs=["configs/settings.xml"],
    with_zookeeper=True,
    stay_alive=True,
    macros={"shard": 2, "replica": 1},
)


all_nodes = [
    shard1_node,
    shard2_node,
]


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


def test_alter_modify_order_by(started_cluster):
    shard1_node.query("DROP DATABASE IF EXISTS alter_modify_order_by SYNC;")
    shard2_node.query("DROP DATABASE IF EXISTS alter_modify_order_by SYNC;")

    shard1_node.query(
        "CREATE DATABASE alter_modify_order_by ENGINE = Replicated('/test/database/alter_modify_order_by', '{shard}', '{replica}');"
    )
    shard1_node.query(
        "CREATE TABLE alter_modify_order_by.t1 (id Int64, score Int64) ENGINE = ReplicatedMergeTree('/test/tables/{uuid}/{shard}', '{replica}') ORDER BY (id);"
    )
    shard1_node.query("ALTER TABLE alter_modify_order_by.t1 modify order by (id);")
    shard2_node.query(
        "CREATE DATABASE alter_modify_order_by ENGINE = Replicated('/test/database/alter_modify_order_by', '{shard}', '{replica}');"
    )

    query = (
        "select count() from system.tables where database = 'alter_modify_order_by';"
    )
    expected = shard1_node.query(query)
    assert_eq_with_retry(shard2_node, query, expected)

    query = "show create table alter_modify_order_by.t1;"
    assert shard1_node.query(query) == shard2_node.query(query)

    shard1_node.query("DROP DATABASE IF EXISTS alter_modify_order_by SYNC;")
    shard2_node.query("DROP DATABASE IF EXISTS alter_modify_order_by SYNC;")
