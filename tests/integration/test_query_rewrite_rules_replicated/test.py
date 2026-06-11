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

QUERY_RULES = {"query_rules": 1}


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
    assert_eq_with_retry(node2, "SELECT 100", "100", settings=QUERY_RULES)
    assert_eq_with_retry(node2, "SELECT count() FROM system.query_rules", "0")
