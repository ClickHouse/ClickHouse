import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

# Two shards, each a single node. Rewrite-rule storage is local (the default), so a rule
# created on the initiator does not exist on the other shard.
node1 = cluster.add_instance("node1", main_configs=["configs/remote_servers.xml"])
node2 = cluster.add_instance("node2", main_configs=["configs/remote_servers.xml"])


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_secondary_query_skips_local_rule(started_cluster):
    # A local table on each shard and a Distributed table over the cluster on the initiator.
    for node in (node1, node2):
        node.query("CREATE TABLE local_t (x UInt64) ENGINE = MergeTree ORDER BY x")
        node.query("INSERT INTO local_t VALUES (1), (2), (3)")
    node1.query(
        "CREATE TABLE dist_t AS local_t ENGINE = Distributed(test_cluster, default, local_t)"
    )

    # A rewrite rule stored only on the initiator (local storage, so node2 does not have it).
    node1.query("CREATE RULE rule_local AS (SELECT 123) REWRITE TO (SELECT 456)")

    # A distributed query that enables the local rule. The `query_rules` setting propagates to
    # the shards with the secondary query fragments, but node2 does not have `rule_local`.
    # Rewrite rules are applied only to the initial query, never to the secondary fragments, so
    # node2 must not throw `REWRITE_RULE_DOESNT_EXIST`, and the distributed query returns the
    # full result (sum over both shards: (1 + 2 + 3) * 2 = 12).
    assert (
        node1.query(
            "SELECT sum(x) FROM dist_t", settings={"query_rules": "rule_local"}
        ).strip()
        == "12"
    )

    node1.query("DROP RULE rule_local")
    node1.query("DROP TABLE dist_t")
    for node in (node1, node2):
        node.query("DROP TABLE local_t")
