from os import path as p

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry

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


def test_initiator_user_in_nested_distributed_query(started_cluster):
    query = "CREATE TABLE t ON CLUSTER node1_only ENGINE = MergeTree ORDER BY x AS SELECT x FROM dist"
    settings = {"distributed_ddl_entry_format_version": 8}
    try:
        node2.query("CREATE TABLE src (x UInt32) ENGINE = MergeTree ORDER BY x")
        node2.query("INSERT INTO src SELECT number FROM numbers(10)")
        node1.query("CREATE TABLE dist (x UInt32) ENGINE = Distributed(node2_with_secret, default, src)")

        node1.query("CREATE USER reader, reader_without_grant")
        node1.query("CREATE ROW POLICY reader_policy ON src USING x < 5 TO reader")
        node1.query("GRANT CLUSTER ON *.* TO reader, reader_without_grant")
        node1.query("GRANT CREATE TABLE, INSERT, SELECT ON t TO reader, reader_without_grant")
        node1.query("GRANT SELECT ON dist TO reader, reader_without_grant")
        node1.query("GRANT SELECT ON src TO reader")
        assert_eq_with_retry(
            node2,
            "SELECT (SELECT count() FROM system.users WHERE name IN ('reader', 'reader_without_grant')), "
            "(SELECT count() FROM system.row_policies WHERE short_name = 'reader_policy'), "
            "(SELECT count() FROM system.grants WHERE user_name = 'reader' AND table = 'src')",
            "2\t1\t1",
        )

        node1.query(query, user="reader", settings=settings)
        assert node1.query("SELECT count() FROM t") == "5\n"
        node1.query("DROP TABLE t SYNC")

        error = node1.query_and_get_error(query, user="reader_without_grant", settings=settings)
        assert "ACCESS_DENIED" in error

        node1.query("DROP TABLE IF EXISTS t SYNC")
        # The initiator's own query holds its only slot while it waits for the entry.
        node1.query(
            "CREATE TABLE t ON CLUSTER node1_only (x UInt32) ENGINE = MergeTree ORDER BY x",
            user="reader",
            settings={**settings, "max_concurrent_queries_for_user": 1, "distributed_ddl_task_timeout": 30},
        )
        assert node1.query("EXISTS TABLE t") == "1\n"
    finally:
        node1.query("DROP TABLE IF EXISTS t SYNC")
        node1.query("DROP TABLE IF EXISTS dist SYNC")
        node2.query("DROP TABLE IF EXISTS src SYNC")
        node1.query("DROP ROW POLICY IF EXISTS reader_policy ON src")
        node1.query("DROP USER IF EXISTS reader, reader_without_grant")
