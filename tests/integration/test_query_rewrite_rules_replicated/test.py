import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry

cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance(
    "node1",
    main_configs=["configs/query_rules_zookeeper.xml"],
    with_zookeeper=True,
)
node2 = cluster.add_instance(
    "node2",
    main_configs=["configs/query_rules_zookeeper.xml"],
    with_zookeeper=True,
)

# `query_rules` lists the names of the active rules (applied in order), so activate
# `rule_repl` by name rather than with the old boolean value.
QUERY_RULES = {"query_rules": "rule_repl"}


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_rules_propagate_between_replicas(started_cluster):
    node1.query("CREATE RULE rule_repl AS (SELECT 100) REWRITE TO (SELECT 200)")

    # `CREATE RULE` adds a child znode, which fires the child-list watch on the
    # other replica.
    assert_eq_with_retry(node2, "SELECT 100", "200", settings=QUERY_RULES)
    assert_eq_with_retry(
        node2, "SELECT name FROM system.query_rules", "rule_repl"
    )

    # `ALTER RULE` changes only the data of an existing child znode: the parent's
    # child list and `cversion` stay intact, so propagation relies on the
    # per-child watches and the `mzxid` comparison in `waitUpdate`.
    node1.query("ALTER RULE rule_repl AS (SELECT 100) REWRITE TO (SELECT 300)")
    assert_eq_with_retry(node2, "SELECT 100", "300", settings=QUERY_RULES)

    # The altering replica applies the new template immediately.
    assert node1.query("SELECT 100", settings=QUERY_RULES).strip() == "300"

    node1.query("DROP RULE rule_repl")
    # Wait for the drop to propagate to node2.
    assert_eq_with_retry(node2, "SELECT count() FROM system.query_rules", "0")
    # Once `rule_repl` is gone, listing it in `query_rules` raises on node2, which both
    # proves the drop propagated and shows the rewrite no longer applies.
    assert "REWRITE_RULE_DOESNT_EXIST" in node2.query_and_get_error(
        "SELECT 100", settings=QUERY_RULES
    )


def test_ddl_consults_keeper_not_the_stale_cache(started_cluster):
    # The per-replica cache of loaded rules is refreshed asynchronously by the background
    # watcher, so `ALTER RULE` / `DROP RULE` must consult Keeper instead. Every statement below
    # is issued immediately after the previous one, i.e. inside the watcher-lag window on the
    # other replica.
    node1.query("CREATE RULE rule_stale AS (SELECT 111) REWRITE TO (SELECT 222)")

    # node2 has most likely not reloaded yet: with the cache as the source of truth this used to
    # fail with `REWRITE_RULE_DOESNT_EXIST`.
    node2.query("ALTER RULE rule_stale AS (SELECT 111) REWRITE TO (SELECT 333)")
    assert (
        node2.query("SELECT 111", settings={"query_rules": "rule_stale"}).strip()
        == "333"
    )
    assert_eq_with_retry(
        node1, "SELECT 111", "333", settings={"query_rules": "rule_stale"}
    )

    # The inverse direction: node1 drops the rule, and node2 - whose cache may still hold it -
    # must honour `IF EXISTS` instead of failing on the missing znode.
    node1.query("DROP RULE rule_stale")
    node2.query("DROP RULE IF EXISTS rule_stale")

    # Without `IF EXISTS` the same situation is a normal `REWRITE_RULE_DOESNT_EXIST`, never a
    # Keeper `ZNONODE` error leaking through.
    assert "REWRITE_RULE_DOESNT_EXIST" in node2.query_and_get_error(
        "DROP RULE rule_stale"
    )

    # And the rule really is gone everywhere.
    assert_eq_with_retry(node1, "SELECT count() FROM system.query_rules", "0")
    assert_eq_with_retry(node2, "SELECT count() FROM system.query_rules", "0")
