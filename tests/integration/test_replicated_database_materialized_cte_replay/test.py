# A Replicated database replica replays DDL committed by the initiator. When the initiator
# predates `force_materialized_cte`, its committed `CREATE VIEW` / `MODIFY QUERY` / `UPDATE` may
# carry a materialized CTE that the guard would reject on a fresh statement; the replay must not
# reject it, or the replica's DDL queue stalls. node1 plays the old initiator (guard off in its
# profile), node2 the upgraded replica (guard on, `enable_materialized_cte = 1` so only the guard
# could reject). DDL entry format 1 carries no settings, so the initial execution (which runs
# through the DDL worker with the worker's profile, not the session's settings) and the replay
# both see their own node's profile.

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry

cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance(
    "node1", user_configs=["configs/node1_profile.xml"], with_zookeeper=True
)
node2 = cluster.add_instance(
    "node2", user_configs=["configs/node2_profile.xml"], with_zookeeper=True
)

DDL_SETTINGS = {
    "distributed_ddl_entry_format_version": 1,
    "distributed_ddl_output_mode": "none",
}
MATERIALIZED_CTE = "WITH c AS MATERIALIZED (SELECT number AS x FROM numbers(3))"
ERRORS_QUERY = "SELECT value FROM system.errors WHERE name = 'SUPPORT_IS_DISABLED'"


def table_exists_query(name):
    return f"SELECT count() FROM system.tables WHERE database = 'rdb' AND name = '{name}'"


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_replay_of_materialized_cte_definitions_is_not_rejected(started_cluster):
    node1.query(
        "CREATE DATABASE rdb ENGINE = Replicated('/test/rdb', 'shard1', 'replica1')"
    )
    node2.query(
        "CREATE DATABASE rdb ENGINE = Replicated('/test/rdb', 'shard1', 'replica2')"
    )

    errors_before = node2.query(ERRORS_QUERY).strip() or "0"

    # CREATE VIEW: replay runs under SECONDARY_CREATE.
    node1.query(
        f"CREATE VIEW rdb.v AS {MATERIALIZED_CTE} SELECT count() AS n FROM c AS a, c AS b",
        settings=DDL_SETTINGS,
    )
    assert "c AS MATERIALIZED" in node1.query("SHOW CREATE rdb.v")

    # MODIFY QUERY: replay is exempted by the metadata-transaction rule.
    node1.query(
        "CREATE TABLE rdb.src (x UInt64) ENGINE = MergeTree ORDER BY x",
        settings=DDL_SETTINGS,
    )
    node1.query(
        "CREATE TABLE rdb.dst (n UInt64) ENGINE = MergeTree ORDER BY n",
        settings=DDL_SETTINGS,
    )
    node1.query(
        "CREATE MATERIALIZED VIEW rdb.mv TO rdb.dst AS SELECT count() AS n FROM rdb.src",
        settings=DDL_SETTINGS,
    )
    node1.query(
        "ALTER TABLE rdb.mv MODIFY QUERY WITH c AS MATERIALIZED (SELECT x FROM rdb.src) "
        "SELECT count() AS n FROM c AS a, c AS b",
        settings=DDL_SETTINGS,
    )
    assert "c AS MATERIALIZED" in node1.query("SHOW CREATE rdb.mv")

    # Lightweight UPDATE: same rule. Plain MergeTree, so each replica applies it to its own
    # copy of the data.
    node1.query(
        "CREATE TABLE rdb.t (id UInt64, v UInt64) ENGINE = MergeTree ORDER BY id "
        "SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1",
        settings=DDL_SETTINGS,
    )
    assert_eq_with_retry(node2, table_exists_query("t"), "1")
    node1.query("INSERT INTO rdb.t SELECT number, 0 FROM numbers(5)")
    node2.query("INSERT INTO rdb.t SELECT number, 0 FROM numbers(5)")
    node1.query(
        f"UPDATE rdb.t SET v = 1 WHERE id IN ({MATERIALIZED_CTE} SELECT a.x FROM c AS a, c AS b)",
        settings=DDL_SETTINGS,
    )
    assert node1.query("SELECT sum(v) FROM rdb.t").strip() == "3"

    # The replica replayed everything.
    assert_eq_with_retry(node2, table_exists_query("v"), "1")
    assert "c AS MATERIALIZED" in node2.query("SHOW CREATE rdb.v")
    assert_eq_with_retry(
        node2,
        "SELECT position(create_table_query, 'c AS MATERIALIZED') > 0 "
        "FROM system.tables WHERE database = 'rdb' AND name = 'mv'",
        "1",
    )
    assert_eq_with_retry(node2, "SELECT sum(v) FROM rdb.t", "3")

    # The queue keeps progressing and nothing was rejected on the replica.
    node1.query(
        "CREATE TABLE rdb.after (x UInt8) ENGINE = MergeTree ORDER BY x",
        settings=DDL_SETTINGS,
    )
    assert_eq_with_retry(node2, table_exists_query("after"), "1")
    errors_after = node2.query(ERRORS_QUERY).strip() or "0"
    assert errors_after == errors_before

    # Negative control: the guard is active on node2 for its own statements, and off on node1.
    assert "SUPPORT_IS_DISABLED" in node2.query_and_get_error(
        f"CREATE VIEW rdb.v2 AS {MATERIALIZED_CTE} SELECT count() AS n FROM c AS a, c AS b",
        settings=DDL_SETTINGS,
    )
    node1.query(
        f"CREATE VIEW rdb.v3 AS {MATERIALIZED_CTE} SELECT count() AS n FROM c AS a, c AS b",
        settings=DDL_SETTINGS,
    )
    assert_eq_with_retry(node2, table_exists_query("v3"), "1")

    node1.query("DROP DATABASE rdb SYNC")
    node2.query("DROP DATABASE IF EXISTS rdb SYNC")
