# pylint: disable=unused-argument
# pylint: disable=redefined-outer-name

import pytest

from helpers.client import QueryRuntimeException
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node = cluster.add_instance("node", with_zookeeper=True)


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()

        yield cluster
    finally:
        cluster.shutdown()


def get_counts():
    src = int(node.query("SELECT count() FROM test"))
    a = int(node.query("SELECT count() FROM test_mv_a"))
    b = int(node.query("SELECT count() FROM test_mv_b"))
    c = int(node.query("SELECT count() FROM test_mv_c"))
    return src, a, b, c


def test_basic(start_cluster):
    old_src, old_a, old_b, old_c = 0, 0, 0, 0

    node.query(
        """
        CREATE TABLE test (A Int64) ENGINE = ReplicatedMergeTree ('/clickhouse/test/tables/test','1') ORDER BY tuple();
        CREATE MATERIALIZED VIEW test_mv_a Engine=ReplicatedMergeTree ('/clickhouse/test/tables/test_mv_a','1') order by tuple() AS SELECT A FROM test;
        CREATE MATERIALIZED VIEW test_mv_b Engine=ReplicatedMergeTree ('/clickhouse/test/tables/test_mv_b','1') partition by A order by tuple() AS SELECT A FROM test;
        CREATE MATERIALIZED VIEW test_mv_c Engine=ReplicatedMergeTree ('/clickhouse/test/tables/test_mv_c','1') order by tuple() AS SELECT A FROM test;
        INSERT INTO test values(999);
        INSERT INTO test values(999);
        """
    )

    src, a, b, c = get_counts()
    assert src == old_src + 1
    assert a == old_a + 2
    assert b == old_b + 2
    assert c == old_c + 2
    old_src, old_a, old_b, old_c = src, a, b, c

    # that issert fails on test_mv_b due to partitions by A
    with pytest.raises(QueryRuntimeException):
        node.query(
            """
            SET max_partitions_per_insert_block = 3;
            INSERT INTO test SELECT number FROM numbers(10);
            """
        )
    src, a, b, c = get_counts()
    assert src == old_src + 10
    assert a == old_a + 10
    assert b == old_b
    assert c == old_c + 10
    old_src, old_a, old_b, old_c = src, a, b, c

    # deduplication only for src table
    node.query("INSERT INTO test SELECT number FROM numbers(10)")
    src, a, b, c = get_counts()
    assert src == old_src
    assert a == old_a + 10
    assert b == old_b + 10
    assert c == old_c + 10
    old_src, old_a, old_b, old_c = src, a, b, c

    # deduplication for MV tables does not work, because previous inserts have not written their deduplications tokens to the log due to `deduplicate_blocks_in_dependent_materialized_views = 0`.
    node.query(
        """
        SET deduplicate_blocks_in_dependent_materialized_views = 1;
        INSERT INTO test SELECT number FROM numbers(10);
        """
    )
    src, a, b, c = get_counts()
    assert src == old_src
    assert a == old_a + 10
    assert b == old_b + 10
    assert c == old_c + 10
    old_src, old_a, old_b, old_c = src, a, b, c

    # deduplication for all the tables
    node.query(
        """
        SET deduplicate_blocks_in_dependent_materialized_views = 1;
        INSERT INTO test SELECT number FROM numbers(10);
        """
    )
    src, a, b, c = get_counts()
    assert src == old_src
    assert a == old_a
    assert b == old_b
    assert c == old_c
    old_src, old_a, old_b, old_c = src, a, b, c

    # that issert fails on test_mv_b due to partitions by A, it is an uniq data which is not deduplicated
    with pytest.raises(QueryRuntimeException):
        node.query(
            """
            SET max_partitions_per_insert_block = 3;
            SET deduplicate_blocks_in_dependent_materialized_views = 1;
            INSERT INTO test SELECT number FROM numbers(100,10);
            """
        )
    src, a, b, c = get_counts()
    assert src == old_src + 10
    assert a == old_a + 10
    assert b == old_b
    assert c == old_c + 10
    old_src, old_a, old_b, old_c = src, a, b, c

    # deduplication for all tables, except test_mv_b. For test_mv_b it is an uniq data which is not deduplicated due to exception at previous insert
    node.query(
        """
        SET deduplicate_blocks_in_dependent_materialized_views = 1;
        INSERT INTO test SELECT number FROM numbers(100,10);
        """
    )
    src, a, b, c = get_counts()
    assert src == old_src
    assert a == old_a
    assert b == old_b + 10
    assert c == old_c
    old_src, old_a, old_b, old_c = src, a, b, c
