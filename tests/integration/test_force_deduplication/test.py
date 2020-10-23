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
            CREATE MATERIALIZED VIEW test_mv Engine=ReplicatedMergeTree ('/clickhouse/test/tables/test_mv','1') partition by A order by tuple() AS SELECT A FROM test;
            SET max_partitions_per_insert_block = 3;
            INSERT INTO test SELECT number FROM numbers(10);
            '''
        )

    node.query("INSERT INTO test SELECT number FROM numbers(10)")
    assert int(node.query("SELECT count() FROM test")) == 10
    assert int(node.query("SELECT count() FROM test_mv")) == 0

    node.query(
        '''
        SET deduplicate_blocks_in_dependent_materialized_views = 1;
        INSERT INTO test SELECT number FROM numbers(10);
        '''
    )
    assert int(node.query("SELECT count() FROM test")) == 10
    assert int(node.query("SELECT count() FROM test_mv")) == 10
