# pylint: disable=unused-argument
# pylint: disable=redefined-outer-name

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.client import QueryRuntimeException

cluster = ClickHouseCluster(__file__)

node = cluster.add_instance('node', with_zookeeper=True)

@pytest.fixture(scope='module')
def start_cluster():
    try:
        cluster.start()

        yield cluster
    finally:
        cluster.shutdown()

def test_basic(start_cluster):
    with pytest.raises(QueryRuntimeException):
        node.query(
            '''
            CREATE TABLE test (A Int64) ENGINE = ReplicatedMergeTree ('/clickhouse/test/tables/test','1') ORDER BY tuple();
            CREATE MATERIALIZED VIEW test_mv_a Engine=ReplicatedMergeTree ('/clickhouse/test/tables/test_mv_a','1') order by tuple() AS SELECT A FROM test;
            CREATE MATERIALIZED VIEW test_mv_b Engine=ReplicatedMergeTree ('/clickhouse/test/tables/test_mv_b','1') partition by A order by tuple() AS SELECT A FROM test;
            CREATE MATERIALIZED VIEW test_mv_c Engine=ReplicatedMergeTree ('/clickhouse/test/tables/test_mv_c','1') order by tuple() AS SELECT A FROM test;
            INSERT INTO test values(999);
            INSERT INTO test values(999);
            SET max_partitions_per_insert_block = 3;
            INSERT INTO test SELECT number FROM numbers(10);
            '''
        )

    assert int(node.query("SELECT count() FROM test")) == 11
    assert int(node.query("SELECT count() FROM test_mv_a")) == 11
    assert int(node.query("SELECT count() FROM test_mv_b")) == 1
    assert int(node.query("SELECT count() FROM test_mv_c")) == 1

    node.query("INSERT INTO test SELECT number FROM numbers(10)")
    assert int(node.query("SELECT count() FROM test")) == 11
    assert int(node.query("SELECT count() FROM test_mv_a")) == 11
    assert int(node.query("SELECT count() FROM test_mv_b")) == 1
    assert int(node.query("SELECT count() FROM test_mv_c")) == 1

    node.query(
        '''
        SET deduplicate_blocks_in_dependent_materialized_views = 1;
        INSERT INTO test SELECT number FROM numbers(10);
        '''
    )
    assert int(node.query("SELECT count() FROM test")) == 11
    assert int(node.query("SELECT count() FROM test_mv_a")) == 21  # first insert was succesfull with disabled dedup..
    assert int(node.query("SELECT count() FROM test_mv_b")) == 11
    assert int(node.query("SELECT count() FROM test_mv_c")) == 11

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
    
    assert int(node.query("SELECT count() FROM test")) == 21
    assert int(node.query("SELECT count() FROM test_mv_a")) == 31
    assert int(node.query("SELECT count() FROM test_mv_b")) == 21
    assert int(node.query("SELECT count() FROM test_mv_c")) == 21
