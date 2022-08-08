import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node", main_configs=["configs/config.xml"], with_zookeeper=True
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_huge_column(started_cluster):
    # max_server_memory_usage is set to 1GB
    # Added column should be 1e6 * 2000 ~= 2GB
    # ALTER ADD COLUMN + MATRIALIZE COLUMN should not cause big memory consumption
    node.query(
        """
        create table test_fetch (x UInt64) engine = ReplicatedMergeTree('/clickhouse/tables/test_fetch', 'r1') order by x settings index_granularity=1024;
        insert into test_fetch select number from numbers(1e6);

        set mutations_sync=1;
        alter table test_fetch add column y String default repeat(' ', 2000) CODEC(NONE);
        alter table test_fetch materialize column y;

        create table test_fetch2 (x UInt64, y String default repeat(' ', 2000) CODEC(NONE)) engine = ReplicatedMergeTree('/clickhouse/tables/test_fetch', 'r2') order by x settings index_granularity=1024;
    """
    )

    # Here we just check that fetch has started.
    node.query(
        """
        set receive_timeout=1;
        system sync replica test_fetch2;
    """,
        ignore_error=True,
    )

    # Here we check that fetch did not use too much memory.
    # See https://github.com/ClickHouse/ClickHouse/issues/39915
    maybe_exception = node.query(
        "select last_exception from system.replication_queue where last_exception like '%Memory limit%';"
    )
    assert maybe_exception == ""
