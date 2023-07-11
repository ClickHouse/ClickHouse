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
    node.query(
        "INSERT INTO test_rewrite_uniq_to_count values ('1', '1', '1'), ('1', '1', '1')"
    )
    node.query(
        "INSERT INTO test_rewrite_uniq_to_count values ('2', '2', '2'), ('2', '2', '2')"
    )
    node.query(
        "INSERT INTO test_rewrite_uniq_to_count values ('3', '3', '3'), ('3', '3', '3')"
    )


def shutdown():
    node.query(
        "DROP TABLE IF EXISTS test_rewrite_uniq_to_count SYNC"
    )


def check(query, result):
    # old analyzer
    query = query + " settings optimize_uniq_to_count = 1"
    assert node.query(query) == f"{result}\n"
    assert "count()" in node.query("EXPLAIN SYNTAX " + query)

    # new analyzer
    query = query + ", allow_experimental_analyzer = 1"
    assert node.query(query) == f"{result}\n"
    assert "count()" in node.query("EXPLAIN QUERY TREE " + query)


def check_by_old_analyzer(query, result):
    # only old analyzer
    query = query + " settings optimize_uniq_to_count = 1"
    assert node.query(query) == f"{result}\n"
    assert "count()" in node.query("EXPLAIN SYNTAX " + query)


def test_rewrite_distinct(started_cluster):
    # simple test
    check(
        "SELECT uniq(a) FROM (SELECT DISTINCT a FROM test_rewrite_uniq_to_count)",
        3,
    )

    # test subquery alias
    check(
        "SELECT uniq(t.a) FROM (SELECT DISTINCT a FROM test_rewrite_uniq_to_count) t",
        3,
    )

    # test compound column name
    check(
        "SELECT uniq(a) FROM (SELECT DISTINCT test_rewrite_uniq_to_count.a FROM test_rewrite_uniq_to_count) t",
        3,
    )

    # test select expression alias
    check_by_old_analyzer(
        "SELECT uniq(a) FROM (SELECT DISTINCT test_rewrite_uniq_to_count.a as alias_of_a FROM test_rewrite_uniq_to_count) t",
        3,
    )

    # test select expression alias
    check_by_old_analyzer(
        "SELECT uniq(alias_of_a) FROM (SELECT DISTINCT a as alias_of_a FROM test_rewrite_uniq_to_count) t",
        3,
    )


def test_rewrite_group_by(started_cluster):
    # simple test
    check(
        "SELECT uniq(a) FROM (SELECT a, sum(b) FROM test_rewrite_uniq_to_count GROUP BY a)",
        3,
    )

    # test subquery alias
    check(
        "SELECT uniq(t.a) FROM (SELECT a, sum(b) FROM test_rewrite_uniq_to_count GROUP BY a) t",
          3,
    )

    # test select expression alias
    check_by_old_analyzer(
        "SELECT uniq(t.alias_of_a) FROM (SELECT a as alias_of_a, sum(b) FROM test_rewrite_uniq_to_count GROUP BY a) t",
        3,
    )

    # test select expression alias
    check_by_old_analyzer(
        "SELECT uniq(t.a) FROM (SELECT a as alias_of_a, sum(b) FROM test_rewrite_uniq_to_count GROUP BY alias_of_a) t",
        3,
    )

    # test select expression alias
    check_by_old_analyzer(
        "SELECT uniq(t.alias_of_a) FROM (SELECT a as alias_of_a, sum(b) FROM test_rewrite_uniq_to_count GROUP BY alias_of_a) t",
        3,
    )
