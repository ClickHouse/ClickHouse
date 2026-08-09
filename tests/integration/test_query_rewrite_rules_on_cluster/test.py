import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

# Two shards, each a single node. Rewrite-rule storage is local (the default), so a rule
# created on the initiator does not exist on the other node. Distributed DDL (`ON CLUSTER`)
# requires ZooKeeper.
node1 = cluster.add_instance(
    "node1", main_configs=["configs/remote_servers.xml"], with_zookeeper=True
)
node2 = cluster.add_instance(
    "node2", main_configs=["configs/remote_servers.xml"], with_zookeeper=True
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_on_cluster_ddl_skips_initiator_local_rule(started_cluster):
    # A rewrite rule stored only on the initiator (local storage, so node2 does not have it).
    node1.query("CREATE RULE rule_local AS (SELECT 123) REWRITE TO (SELECT 456)")

    # An `ON CLUSTER` DDL from a session that enables the initiator-local rule. Rewrite rules
    # are applied only on the initiator: `query_rules` is stripped from the settings recorded
    # in the DDL log entry, so the worker on node2 never looks up `rule_local` and must not
    # throw `REWRITE_RULE_DOESNT_EXIST`.
    node1.query(
        "CREATE TABLE t_local_rule ON CLUSTER test_cluster (x UInt64) ENGINE = MergeTree ORDER BY x",
        settings={"query_rules": "rule_local"},
    )
    for node in (node1, node2):
        assert node.query("EXISTS TABLE t_local_rule").strip() == "1"

    node1.query("DROP RULE rule_local")
    node1.query("DROP TABLE t_local_rule ON CLUSTER test_cluster SYNC")


def test_on_cluster_ddl_is_not_rewritten_second_time_on_workers(started_cluster):
    node1.query(
        "CREATE TABLE t_reject ON CLUSTER test_cluster (x UInt64) ENGINE = MergeTree ORDER BY x"
    )

    # The same rule exists on both nodes. Its source template matches the worker-local form of
    # the DDL below: the initiator strips `ON CLUSTER` and qualifies the table with the current
    # database before writing the query into the DDL log entry, so each worker replays
    # `DROP TABLE default.t_reject`. The user-submitted query (with `ON CLUSTER`, unqualified)
    # does not match the template, so the rule must not fire on the initiator either.
    for node in (node1, node2):
        node.query(
            "CREATE RULE rule_reject AS (DROP TABLE default.t_reject) REJECT WITH 'denied'"
        )

    # Rules are applied once, on the initiator, to the query as the user submitted it. Since
    # `query_rules` is stripped from the DDL log entry settings, the workers get no second
    # chance to match `rule_reject` against the replayed post-`ON CLUSTER` query, and the
    # `DROP` must succeed on both nodes. (No `SYNC` here: a `SYNC` flag would travel into the
    # replayed query and change its hash, which would mask the strip being tested.)
    node1.query(
        "DROP TABLE t_reject ON CLUSTER test_cluster",
        settings={"query_rules": "rule_reject"},
    )
    for node in (node1, node2):
        assert node.query("EXISTS TABLE t_reject").strip() == "0"

    # The rule itself still works when the matching query is submitted directly.
    assert "denied" in node1.query_and_get_error(
        "DROP TABLE default.t_reject",
        settings={"query_rules": "rule_reject"},
    )

    for node in (node1, node2):
        node.query("DROP RULE rule_reject")
