"""
A `Replicated` database whose table metadata in ZooKeeper is nested deeper than the recovering
replica's `max_parser_depth` could not be recovered: `recoverLostReplica` re-parsed the stored
`CREATE` text with the recovering server's own parser limits, threw `AST is too deep` before creating
the table and retried forever, while the replica that had created the very same table kept serving it.

Such a table comes to be on a server whose default profile raises the parser limits: the `Replicated`
DDL worker re-parses every entry with the default profile, so a session-level raise is not enough,
and the recovering server has no way to learn the values the creating one used. The metadata is the
server's own canonical output, so its depth (and length) is not the reader's business.
"""

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

creating_node = cluster.add_instance(
    "creating_node",
    user_configs=["configs/raised_parser_limits.xml"],
    with_zookeeper=True,
    macros={"shard": 1, "replica": 1},
)
recovering_node = cluster.add_instance(
    "recovering_node",
    user_configs=["configs/low_parser_limits.xml"],
    with_zookeeper=True,
    macros={"shard": 1, "replica": 2},
)

# Deeper than the `max_parser_depth` / `max_ast_depth` of the recovering node (about two levels of
# parser depth per nesting), yet shallow enough for the analyzer's recursion on the stack of sanitizer
# builds (under TSan `checkStackSize` allows only 5% of the stack, and 600 levels did not fit even
# the half of it allowed under ASan and MSan).
DEPTH = 30
DEEP_EXPRESSION = "(id + " * DEPTH + "id" + ")" * DEPTH


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_recover_table_with_metadata_deeper_than_parser_limits(started_cluster):
    creating_node.query(
        "CREATE DATABASE deep ENGINE = Replicated('/test/deep', '{shard}', '{replica}')"
    )

    # The definition exceeds the recovering node's limits, ...
    assert "TOO_DEEP_AST" in recovering_node.query_and_get_error(
        f"SELECT formatQuery('CREATE TABLE deep.t (id UInt64, d UInt64 DEFAULT {DEEP_EXPRESSION}) ENGINE = MergeTree ORDER BY id')"
    )
    # ... but not the creating server's (the client parses the text too, with its own settings). The
    # table is a plain `MergeTree`: `ReplicatedMergeTree` re-parses its column definitions from
    # ZooKeeper with a fixed limit of its own, which is not what this test is about.
    creating_node.query(
        f"CREATE TABLE deep.t (id UInt64, d UInt64 DEFAULT {DEEP_EXPRESSION}) ENGINE = MergeTree ORDER BY id",
        settings={"max_parser_depth": 100000, "max_ast_depth": 100000},
    )
    creating_node.query("INSERT INTO deep.t (id) SELECT number FROM numbers(3)")
    assert creating_node.query("SELECT sum(d) FROM deep.t") == f"{3 * (DEPTH + 1)}\n"

    # A new replica with the low limits has to recover the table from that metadata.
    recovering_node.query(
        "CREATE DATABASE deep ENGINE = Replicated('/test/deep', '{shard}', '{replica}')"
    )
    recovering_node.query("SYSTEM SYNC DATABASE REPLICA deep")
    assert recovering_node.query("EXISTS TABLE deep.t") == "1\n"
    recovering_node.query("INSERT INTO deep.t (id) SELECT number FROM numbers(3)")
    assert recovering_node.query("SELECT sum(d) FROM deep.t") == f"{3 * (DEPTH + 1)}\n"
    # The database's recovery never refused the metadata (the probe above did, in a query of its own).
    recovering_node.query("SYSTEM FLUSH LOGS text_log")
    assert (
        recovering_node.query(
            "SELECT count() FROM system.text_log WHERE message LIKE '%AST is too deep%' AND logger_name LIKE '%deep%'"
        )
        == "0\n"
    )

    for node in (creating_node, recovering_node):
        node.query("DROP DATABASE deep SYNC")
