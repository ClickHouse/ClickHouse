import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance("node", user_configs=["configs/users.xml"])


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_metadata_cache_memleak(started_cluster):
    if not node.is_built_with_address_sanitizer():
        pytest.skip("Test is only valid with address(leak) sanitizer build")

    node.query(
        """
        drop table if exists test;
        create table test (id Int, key String) engine=MergeTree order by id % 10;
        insert into test select number, number::String from numbers(3);
        alter table test modify comment 'new description';
    """
    )

    node.restart_clickhouse()
