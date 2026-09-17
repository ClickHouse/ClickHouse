import pytest

from helpers.cluster import CLICKHOUSE_CI_MIN_TESTED_VERSION, ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry

cluster = ClickHouseCluster(__file__)

# A replica running a version without the mistyped-alias index reject can commit an
# ALTER_METADATA entry that introduces the violation. An upgraded replica must still apply
# that committed entry: entries execute in version order, so rejecting it would wedge the
# replication queue behind an entry it can never apply.
node_old = cluster.add_instance(
    "node_old",
    with_zookeeper=True,
    image="clickhouse/clickhouse-server",
    tag=CLICKHOUSE_CI_MIN_TESTED_VERSION,
    with_installed_binary=True,
)
node_new = cluster.add_instance("node_new", with_zookeeper=True)


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_replay_of_committed_alter_is_not_rejected(start_cluster):
    for node in (node_old, node_new):
        node.query(
            """
            CREATE TABLE t_alias_index_replay
            (
                event String,
                tok FixedString(3) ALIAS lower(event)
            )
            ENGINE = ReplicatedMergeTree('/clickhouse/tables/t_alias_index_replay', '{replica}')
            ORDER BY tuple()
            """.replace(
                "{replica}", node.name
            )
        )

    # The old replica accepts the index over the mistyped ALIAS and commits the entry.
    node_old.query(
        "ALTER TABLE t_alias_index_replay ADD INDEX i tok TYPE tokenbf_v1(256, 2, 0)"
    )

    # The upgraded replica applies the committed entry instead of wedging its queue on it.
    # Wait for the new metadata itself, not for an empty queue: the queue is also empty
    # before the entry has been pulled at all.
    assert_eq_with_retry(
        node_new,
        "SELECT count() FROM system.tables"
        " WHERE table = 't_alias_index_replay' AND create_table_query LIKE '%tokenbf_v1%'",
        "1",
    )
    assert_eq_with_retry(
        node_new,
        "SELECT count() FROM system.replication_queue WHERE table = 't_alias_index_replay'",
        "0",
    )

    # Replication stays alive past the applied entry.
    node_old.query("INSERT INTO t_alias_index_replay VALUES ('AbC')")
    assert_eq_with_retry(
        node_new, "SELECT count() FROM t_alias_index_replay", "1"
    )

    # A direct user ALTER introducing a fresh violation is still rejected on the new replica.
    assert "BAD_ARGUMENTS" in node_new.query_and_get_error(
        "ALTER TABLE t_alias_index_replay ADD INDEX j tok TYPE ngrambf_v1(3, 256, 2, 0)"
    )

    # The grandfathered table is not wedged for unrelated or curing ALTERs.
    node_new.query("ALTER TABLE t_alias_index_replay DROP INDEX i")

    for node in (node_old, node_new):
        node.query("DROP TABLE t_alias_index_replay SYNC")


def test_replicated_database_follower_replay_is_not_rejected(start_cluster):
    # A `Replicated` database re-executes the same ALTER on every replica, and only the first
    # run decides whether it is allowed. A follower on the new version reaches the reject through
    # `checkAlterIsPossible`, not `setTableStructure`, so that path has to know it is a replay too:
    # otherwise it keeps failing an entry a replica on the old version has already committed.
    for node in (node_old, node_new):
        node.query(
            "CREATE DATABASE rdb_alias ENGINE = Replicated('/clickhouse/rdb_alias', 'shard1', '{replica}')".replace(
                "{replica}", node.name
            )
        )

    node_old.query(
        """
        CREATE TABLE rdb_alias.t (event String, tok FixedString(3) ALIAS lower(event))
        ENGINE = MergeTree ORDER BY tuple()
        """
    )
    assert_eq_with_retry(
        node_new,
        "SELECT count() FROM system.tables WHERE database = 'rdb_alias' AND name = 't'",
        "1",
    )

    # The old replica accepts the index over the mistyped ALIAS; the follower replays the entry.
    node_old.query("ALTER TABLE rdb_alias.t ADD INDEX i tok TYPE tokenbf_v1(256, 2, 0)")
    assert_eq_with_retry(
        node_new,
        "SELECT count() FROM system.tables"
        " WHERE database = 'rdb_alias' AND name = 't' AND create_table_query LIKE '%tokenbf_v1%'",
        "1",
    )

    # A fresh violation initiated on the new replica is still rejected.
    assert "BAD_ARGUMENTS" in node_new.query_and_get_error(
        "ALTER TABLE rdb_alias.t ADD INDEX j tok TYPE ngrambf_v1(3, 256, 2, 0)"
    )

    for node in (node_old, node_new):
        node.query("DROP DATABASE rdb_alias SYNC")
