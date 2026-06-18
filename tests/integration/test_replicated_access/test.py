from os import path as p

import pytest

from helpers.cluster import ClickHouseCluster

default_zk_config = p.join(p.dirname(p.realpath(__file__)), "configs/zookeeper.xml")
cluster = ClickHouseCluster(__file__, zookeeper_config_path="configs/zookeeper.xml")

node1 = cluster.add_instance(
    "node1",
    main_configs=["configs/config.xml"],
    with_zookeeper=True,
    stay_alive=True,
)

node2 = cluster.add_instance(
    "node2",
    main_configs=["configs/config.xml"],
    with_zookeeper=True,
    stay_alive=True,
)

all_nodes = [node1, node2]


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


@pytest.fixture(scope="function", autouse=True)
def prepare_test():
    node1.query("CREATE USER test")
    node1.query("CREATE TABLE IF NOT EXISTS table ON CLUSTER default (x UInt64) ENGINE=MergeTree ORDER BY x")
    node1.query("CREATE TABLE IF NOT EXISTS secret ON CLUSTER default (value String) ENGINE=MergeTree ORDER BY value")
    try:
        yield
    finally:
        node1.query("DROP USER IF EXISTS test")
        node1.query("DROP TABLE IF EXISTS table ON CLUSTER default")
        node1.query("DROP TABLE IF EXISTS secret ON CLUSTER default")


def test_noop_update_does_not_bump_znode(started_cluster):
    node1.query("GRANT SELECT ON *.* TO test")

    user_id = node1.query("SELECT id FROM system.users WHERE name = 'test'").strip()
    znode = f"/clickhouse/access/uuid/{user_id}"

    zk = started_cluster.get_kazoo_client("zoo1")
    try:
        _, stat_before = zk.get(znode)

        # Re-granting an already granted privilege leaves the entity identical, so it
        # must not write to ZooKeeper (and thus must not refresh the entity cluster-wide).
        node1.query("GRANT SELECT ON *.* TO test")
        _, stat_noop = zk.get(znode)
        assert stat_noop.version == stat_before.version

        # A privilege change that actually modifies the entity must still bump the version.
        node1.query("GRANT INSERT ON *.* TO test")
        _, stat_changed = zk.get(znode)
        assert stat_changed.version > stat_before.version
    finally:
        zk.stop()
        zk.close()


def test_initiator_user_in_ddl(started_cluster):
    node1.query("INSERT INTO secret VALUES ('super_secret')")

    node1.query("GRANT ALTER ON table TO test")
    node1.query("GRANT CLUSTER ON *.* TO test")

    query = """
    ALTER TABLE table ON CLUSTER default
        ADD PROJECTION test (
            SELECT
                x,
                (SELECT * FROM secret LIMIT 1) as bar
            ORDER BY x
        )
    SETTINGS distributed_ddl_entry_format_version = 8
    """

    error = node1.query_and_get_error(query, user="test")
    assert "Not enough privileges" in error


    for node in all_nodes:
        node.replace_in_config(
            "/etc/clickhouse-server/config.d/config.xml",
            "<distributed_ddl_use_initial_user_and_roles>1</distributed_ddl_use_initial_user_and_roles>",
            "<distributed_ddl_use_initial_user_and_roles>0</distributed_ddl_use_initial_user_and_roles>",
        )
        node.restart_clickhouse()

    node1.query(query, user="test")

    for node in all_nodes:
        node.replace_in_config(
            "/etc/clickhouse-server/config.d/config.xml",
            "<distributed_ddl_use_initial_user_and_roles>0</distributed_ddl_use_initial_user_and_roles>",
            "<distributed_ddl_use_initial_user_and_roles>1</distributed_ddl_use_initial_user_and_roles>",
        )
        node.restart_clickhouse()
