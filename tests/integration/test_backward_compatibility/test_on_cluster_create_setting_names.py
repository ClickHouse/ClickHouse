"""An `ON CLUSTER` `CREATE TABLE` or `CREATE DATABASE` has its `SETTINGS` names judged on the initiator, so a host
creates what an older initiator enqueued without judging them. 26.9 judges none of these names.
`IF NOT EXISTS` is still judged on each host.
"""

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node_old = cluster.add_instance(
    "node_old",
    main_configs=["configs/clusters_old_and_new.xml"],
    with_zookeeper=True,
    image="clickhouse/clickhouse-server",
    tag="26.9",
    stay_alive=True,
    with_installed_binary=True,
)
node_new = cluster.add_instance(
    "node_new",
    main_configs=["configs/clusters_old_and_new.xml"],
    with_zookeeper=True,
)

TABLE = "(x UInt8) ENGINE = Log SETTINGS not_a_setting_at_all = 1"
DATABASE = "ENGINE = Atomic SETTINGS not_a_setting_at_all = DEFAULT"
LEGACY_FORMAT = {"distributed_ddl_entry_format_version": 2}


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        node_old.query("CREATE DATABASE db ON CLUSTER old_and_new")
        node_old.query(
            "CREATE TABLE db.src ON CLUSTER old_and_new (x UInt8) ENGINE = Log"
        )
        yield cluster
    finally:
        cluster.shutdown()


def stored_table(node, table):
    return node.query(
        f"SELECT create_table_query FROM system.tables WHERE database = 'db' AND name = '{table}'"
    )


def stored_database(node, database):
    return node.query(
        f"SELECT engine_full FROM system.databases WHERE name = '{database}'"
    )


def test_host_creates_a_table_an_older_initiator_enqueued(start_cluster):
    node_old.query(f"CREATE TABLE db.t_old ON CLUSTER old_and_new {TABLE}")
    assert "not_a_setting_at_all" in stored_table(node_new, "t_old")


def test_host_creates_a_database_an_older_initiator_enqueued(start_cluster):
    node_old.query(f"CREATE DATABASE d_old ON CLUSTER old_and_new {DATABASE}")
    assert "not_a_setting_at_all" in stored_database(node_new, "d_old")


def test_host_creates_a_database_of_a_legacy_entry_format(start_cluster):
    # The initiator judges a database at every entry format.
    node_old.query(
        f"CREATE DATABASE d_old_legacy ON CLUSTER old_and_new {DATABASE}",
        settings=LEGACY_FORMAT,
    )
    assert "not_a_setting_at_all" in stored_database(node_new, "d_old_legacy")


def test_initiator_rejects_a_table_before_enqueueing(start_cluster):
    assert "UNKNOWN_SETTING" in node_new.query_and_get_error(
        f"CREATE TABLE db.t_new ON CLUSTER old_and_new {TABLE}"
    )
    assert node_old.query("EXISTS TABLE db.t_new") == "0\n"


def test_initiator_rejects_a_database_before_enqueueing(start_cluster):
    assert "UNKNOWN_SETTING" in node_new.query_and_get_error(
        f"CREATE DATABASE d_new ON CLUSTER old_and_new {DATABASE}"
    )
    assert node_old.query("EXISTS DATABASE d_new") == "0\n"


def test_host_judges_a_table_of_a_legacy_entry_format(start_cluster):
    # With a legacy format the initiator does not normalize the `CREATE TABLE`, so the hosts judge its names.
    assert "UNKNOWN_SETTING" in node_new.query_and_get_error(
        f"CREATE TABLE db.t_legacy ON CLUSTER old_and_new {TABLE}",
        settings=LEGACY_FORMAT,
    )


def test_initiator_rejects_a_materialized_view_before_enqueueing(start_cluster):
    assert "UNKNOWN_SETTING" in node_new.query_and_get_error(
        "CREATE MATERIALIZED VIEW db.mv_new ON CLUSTER old_and_new "
        "ENGINE = Log SETTINGS not_a_setting_at_all = 1 AS SELECT x FROM db.src"
    )
    assert node_old.query("EXISTS TABLE db.mv_new") == "0\n"


def test_if_not_exists_over_an_existing_table_is_a_no_op(start_cluster):
    node_new.query(
        "CREATE TABLE db.t_exists ON CLUSTER old_and_new (x UInt8) ENGINE = Log"
    )
    node_new.query(
        f"CREATE TABLE IF NOT EXISTS db.t_exists ON CLUSTER old_and_new {TABLE}"
    )


def test_host_judges_a_missing_table_of_if_not_exists(start_cluster):
    assert "UNKNOWN_SETTING" in node_new.query_and_get_error(
        f"CREATE TABLE IF NOT EXISTS db.t_missing ON CLUSTER old_and_new {TABLE}"
    )


def test_if_not_exists_over_an_existing_database_is_a_no_op(start_cluster):
    node_new.query("CREATE DATABASE d_exists ON CLUSTER old_and_new ENGINE = Atomic")
    node_new.query(
        f"CREATE DATABASE IF NOT EXISTS d_exists ON CLUSTER old_and_new {DATABASE}"
    )


def test_host_judges_a_missing_database_of_if_not_exists(start_cluster):
    assert "UNKNOWN_SETTING" in node_new.query_and_get_error(
        f"CREATE DATABASE IF NOT EXISTS d_missing ON CLUSTER old_and_new {DATABASE}"
    )
