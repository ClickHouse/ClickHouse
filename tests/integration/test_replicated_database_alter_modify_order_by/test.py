import re
import pytest


from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry, assert_logs_contain


cluster = ClickHouseCluster(__file__)

main_node = cluster.add_instance(
    "main_node",
    main_configs=["configs/config.xml"],
    user_configs=["configs/settings.xml"],
    with_zookeeper=True,
    stay_alive=True,
    macros={"shard": 1, "replica": 1},
)

snapshotting_node = cluster.add_instance(
    "snapshotting_node",
    main_configs=["configs/config.xml"],
    user_configs=["configs/settings.xml"],
    with_zookeeper=True,
    macros={"shard": 2, "replica": 1},
)


all_nodes = [
    main_node,
    snapshotting_node,
]


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


def test_alter_modify_order_by(started_cluster):
    main_node.query("DROP DATABASE IF EXISTS alter_modify_order_by SYNC;")
    snapshotting_node.query("DROP DATABASE IF EXISTS alter_modify_order_by SYNC;")

    main_node.query(
        "CREATE DATABASE alter_modify_order_by ENGINE = Replicated('/test/database/alter_modify_order_by', '{shard}', '{replica}');"
    )
    main_node.query(
        "CREATE TABLE alter_modify_order_by.t1 (id Int64, score Int64) ENGINE = ReplicatedMergeTree('/test/tables/{uuid}/{shard}', '{replica}') ORDER BY (id);"
    )
    main_node.query("ALTER TABLE alter_modify_order_by.t1 modify order by (id);")
    snapshotting_node.query(
        "CREATE DATABASE alter_modify_order_by ENGINE = Replicated('/test/database/alter_modify_order_by', '{shard}', '{replica}');"
    )

    query = (
        "select count() from system.tables where database = 'alter_modify_order_by';"
    )
    expected = main_node.query(query)
    assert_eq_with_retry(snapshotting_node, query, expected)

    query = "show create table alter_modify_order_by.t1;"
    assert main_node.query(query) == snapshotting_node.query(query)

    main_node.query("DROP DATABASE IF EXISTS alter_modify_order_by SYNC;")
    snapshotting_node.query("DROP DATABASE IF EXISTS alter_modify_order_by SYNC;")
