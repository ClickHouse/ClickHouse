"""A `SETTINGS` name that is not a setting at all is refused when a MergeTree definition is
stated, but a definition an older server already stored has to keep working. A non-append
refresh re-issues the target's stored definition as a `CREATE`, and in a `Replicated` database
that `CREATE` travels through the DDL log, which rebuilds the query context. Only an older
server can write such a definition, so the older server is what plants it here.
"""

import pytest

from helpers.cluster import CLICKHOUSE_CI_MIN_TESTED_VERSION, ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    with_zookeeper=True,
    image="clickhouse/clickhouse-server",
    tag=CLICKHOUSE_CI_MIN_TESTED_VERSION,
    stay_alive=True,
    with_installed_binary=True,
    # A coordinated refresh reads its Keeper state with a multi-read.
    keeper_required_feature_flags=["multi_read", "create_if_not_exists"],
    macros={"shard": "s1", "replica": "r1"},
)

DB = "rdb_stored_setting_name"


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_refresh_keeps_stored_setting_name(start_cluster):
    node.query(f"CREATE DATABASE {DB} ENGINE = Replicated('/test/{DB}', 's1', 'r1')")
    node.query(
        f"CREATE TABLE {DB}.src (x UInt8) ENGINE = ReplicatedMergeTree ORDER BY x"
    )
    node.query(f"INSERT INTO {DB}.src VALUES (1)")
    node.query(
        f"CREATE MATERIALIZED VIEW {DB}.mv REFRESH EVERY 1 YEAR (x UInt8) "
        f"ENGINE = ReplicatedMergeTree ORDER BY x SETTINGS not_a_setting_at_all = DEFAULT "
        f"AS SELECT x FROM {DB}.src"
    )
    node.query(f"SYSTEM WAIT VIEW {DB}.mv")

    stored = node.query(
        f"SELECT create_table_query FROM system.tables "
        f"WHERE database = '{DB}' AND name LIKE '%inner%'"
    )
    # Without a planted definition the rest of the test would assert nothing, and a release that
    # refuses this clause can no longer plant it.
    assert "not_a_setting_at_all" in stored, stored
    assert node.query(f"SELECT count() FROM {DB}.mv") == "1\n"

    node.restart_with_latest_version()

    # The name is refused for a definition this server is asked to state.
    assert "UNKNOWN_SETTING" in node.query_and_get_error(
        f"CREATE TABLE {DB}.stated (x UInt8) ENGINE = ReplicatedMergeTree ORDER BY x "
        f"SETTINGS not_a_setting_at_all = DEFAULT"
    )

    # The refresh re-issues the stored one, and the new row is what proves it ran: `SYSTEM WAIT
    # VIEW` also returns for a refresh that never started.
    node.query(f"INSERT INTO {DB}.src VALUES (2)")
    node.query(f"SYSTEM REFRESH VIEW {DB}.mv")
    node.query(f"SYSTEM WAIT VIEW {DB}.mv")
    assert node.query(f"SELECT count() FROM {DB}.mv") == "2\n"

    node.query(f"DROP DATABASE {DB} SYNC")
