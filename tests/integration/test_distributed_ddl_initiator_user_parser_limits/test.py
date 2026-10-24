import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance(
    "node1",
    main_configs=["configs/config.xml"],
    user_configs=["configs/users.xml"],
    with_zookeeper=True,
    macros={"replica": "node1"},
)

node2 = cluster.add_instance(
    "node2",
    main_configs=["configs/config.xml"],
    user_configs=["configs/users.xml"],
    with_zookeeper=True,
    macros={"replica": "node2"},
)

all_nodes = [node1, node2]

# Deeper than the `max_parser_depth` the `default` profile allows (10), shallower than the
# `deep_parser` profile allows (500). `identity` is not folded away when the accepted AST is
# formatted back into the DDL queue entry, unlike a nested literal.
NESTING = 40


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


@pytest.fixture(scope="function", autouse=True)
def prepare_test():
    try:
        yield
    finally:
        for node in all_nodes:
            node.query("DROP VIEW IF EXISTS default.deep_view SYNC")


def test_on_cluster_parses_the_entry_under_the_initiator_user_profile(started_cluster):
    """
    The DDL worker parses the queue entry before it builds the query context, so it used to clamp the
    entry's `max_parser_depth` / `max_parser_backtracks` against its own profile. With
    `distributed_ddl_use_initial_user_and_roles = 1` the entry is executed as the initiator's user,
    whose profile allows a deeper query than the worker's own one, so the pre-parse rejected a query
    that execution would have accepted.
    """
    deep_expression = "identity(" * NESTING + "1" + ")" * NESTING

    node1.query(
        f"CREATE VIEW default.deep_view ON CLUSTER cluster AS SELECT {deep_expression} AS x",
        user="deep_user",
        # The initiator's user and roles travel with the entry only from this entry format version on.
        settings={"distributed_ddl_entry_format_version": 8},
    )

    for node in all_nodes:
        assert (
            node.query(
                "SELECT count() FROM system.tables WHERE database = 'default' AND name = 'deep_view'"
            ).strip()
            == "1"
        )
        assert node.query("SELECT x FROM default.deep_view").strip() == "1"
