import pytest
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance("node")


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        prepare()
        yield cluster
    finally:
        shutdown()
        cluster.shutdown()


def prepare():
    node.query(
        """
        CREATE TABLE IF NOT EXISTS test_rewrite_uniq_to_count
        (
            `a` UInt8,
            `b` UInt8,
            `c` UInt8
        )
        ENGINE = MergeTree
        ORDER BY `a`
        """
    )
    node.query("INSERT INTO test_rewrite_uniq_to_count values ('1', '1', '1'), ('1', '1', '1')")
    node.query("INSERT INTO test_rewrite_uniq_to_count values ('2', '2', '2'), ('2', '2', '2')")
    node.query("INSERT INTO test_rewrite_uniq_to_count values ('3', '3', '3'), ('3', '3', '3')")


def shutdown():
    node.query("DROP TABLE IF EXISTS test_rewrite_uniq_to_count SYNC")


def check(query, result):
    # old analyzer
    query = query + " settings optimize_uniq_to_count = 1"
    assert node.query(query) == f"{result}\n"
    assert "count" in node.query("EXPLAIN SYNTAX " + query)

    # # new analyzer
    # query = query + ", allow_experimental_analyzer = 1"
    # assert node.query(query) == f"{result}\n"
    # assert "count" in node.query("EXPLAIN QUERY_TREE " + query)


def test_rewrite_distinct(started_cluster):
    check("SELECT uniq(a) FROM (SELECT DISTINCT a FROM test_rewrite_uniq_to_count)",
          3)

    check("SELECT uniq(t.a) FROM (SELECT DISTINCT a FROM test_rewrite_uniq_to_count) t",
          3)

    check("SELECT uniq(a) FROM (SELECT DISTINCT test_rewrite_uniq_to_count.a FROM test_rewrite_uniq_to_count) t",
          3)

    check("SELECT uniq(a) FROM (SELECT DISTINCT test_rewrite_uniq_to_count.a as n FROM test_rewrite_uniq_to_count) t",
          3)


def test_rewrite_group_by(started_cluster):
    check("SELECT uniq(a) FROM (SELECT a, min(b) FROM test_rewrite_uniq_to_count GROUP BY a)",
          3)

    check("SELECT uniq(t.a) FROM (SELECT a, min(b) FROM test_rewrite_uniq_to_count GROUP BY a) t",
          3)

    check("SELECT uniq(t.alias_of_a) FROM (SELECT a as alias_of_a, min(b) FROM test_rewrite_uniq_to_count GROUP BY a) t",
          3)

    check("SELECT uniq(t.a) FROM (SELECT a as alias_of_a, min(b) FROM test_rewrite_uniq_to_count GROUP BY alias_of_a) t",
          3)

    check("SELECT uniq(t.alias_of_a) FROM (SELECT a as alias_of_a, min(b) FROM test_rewrite_uniq_to_count GROUP BY alias_of_a) t",
          3)
