import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    user_configs=["configs/users.xml"],
    stay_alive=True
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_metadata_cache_memleak(started_cluster):
    node.query(
        """
        drop table if exists test;
        drop dictionary if exists dict;

        create dictionary dict (key UInt64, value UInt64) PRIMARY KEY key SOURCE(CLICKHOUSE(QUERY 'select 1, 1')) LIFETIME(0) LAYOUT(HASHED());

        create table test
        (
            x UInt64,
            -- Previously the problem was that due to functions can hold copy of ContextPtr (one of such functions is dictGet) that stores metadata cache, and this creates recursive reference for shared_ptr, and leads to memory leak.
            y UInt64 DEFAULT dictGet('dict', 'value', x)
        ) engine=MergeTree
        order by x % 10;
        insert into test (x) select number from numbers(3);
        alter table test modify comment 'new description';
    """
    )

    node.restart_clickhouse()
