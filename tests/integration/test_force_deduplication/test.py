# pylint: disable=unused-argument
# pylint: disable=redefined-outer-name

import pytest
from helpers.client import QueryRuntimeException
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node = cluster.add_instance('node', with_zookeeper=True)


@pytest.fixture(scope='module')
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
    node.query(
        '''
        CREATE TABLE test (A Int64) ENGINE = ReplicatedMergeTree ('/clickhouse/test/tables/test','1') ORDER BY tuple();
        CREATE MATERIALIZED VIEW test_mv_a Engine=ReplicatedMergeTree ('/clickhouse/test/tables/test_mv_a','1') order by tuple() AS SELECT A FROM test;
        CREATE MATERIALIZED VIEW test_mv_b Engine=ReplicatedMergeTree ('/clickhouse/test/tables/test_mv_b','1') partition by A order by tuple() AS SELECT A FROM test;
        CREATE MATERIALIZED VIEW test_mv_c Engine=ReplicatedMergeTree ('/clickhouse/test/tables/test_mv_c','1') order by tuple() AS SELECT A FROM test;
        INSERT INTO test values(999);
        INSERT INTO test values(999);
        '''
    )
    with pytest.raises(QueryRuntimeException):
        node.query(
            '''
            SET max_partitions_per_insert_block = 3;
            INSERT INTO test SELECT number FROM numbers(10);
            '''
        )

    old_src, old_a, old_b, old_c = get_counts()
    # number of rows in test_mv_a and test_mv_c depends on order of inserts into views
    assert old_src == 11
    assert old_a in (1, 11)
    assert old_b == 1
    assert old_c in (1, 11)

    node.query("INSERT INTO test SELECT number FROM numbers(10)")
    src, a, b, c = get_counts()
    # no changes because of deduplication in source table
    assert src == old_src
    assert a == old_a
    assert b == old_b
    assert c == old_c

    node.query(
        '''
        SET deduplicate_blocks_in_dependent_materialized_views = 1;
        INSERT INTO test SELECT number FROM numbers(10);
        '''
    )
    src, a, b, c = get_counts()
    assert src == 11
    assert a == old_a + 10  # first insert could be succesfull with disabled dedup
    assert b == 11
    assert c == old_c + 10

    with pytest.raises(QueryRuntimeException):
        node.query(
            '''
            SET max_partitions_per_insert_block = 3;
            SET deduplicate_blocks_in_dependent_materialized_views = 1;
            INSERT INTO test SELECT number FROM numbers(100,10);
            '''
        )

    node.query(
        '''
        SET deduplicate_blocks_in_dependent_materialized_views = 1;
        INSERT INTO test SELECT number FROM numbers(100,10);
        '''
    )

    src, a, b, c = get_counts()
    assert src == 21
    assert a == old_a + 20
    assert b == 21
    assert c == old_c + 20
