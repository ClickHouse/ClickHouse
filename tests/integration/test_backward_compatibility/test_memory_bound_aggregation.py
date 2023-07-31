import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance(
    "node1",
    with_zookeeper=False,
    image="yandex/clickhouse-server",
    tag="21.1",
    stay_alive=True,
    with_installed_binary=True,
    allow_analyzer=False,
)
node2 = cluster.add_instance(
    "node2",
    with_zookeeper=False,
    image="yandex/clickhouse-server",
    tag="21.1",
    stay_alive=True,
    with_installed_binary=True,
    allow_analyzer=False,
)
node3 = cluster.add_instance("node3", with_zookeeper=False, allow_analyzer=False)


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


def test_backward_compatability(start_cluster):
    node1.query("create table t (a UInt64) engine = MergeTree order by a")
    node2.query("create table t (a UInt64) engine = MergeTree order by a")
    node3.query("create table t (a UInt64) engine = MergeTree order by a")

    node1.query("insert into t select number % 100000 from numbers_mt(1000000)")
    node2.query("insert into t select number % 100000 from numbers_mt(1000000)")
    node3.query("insert into t select number % 100000 from numbers_mt(1000000)")

    assert (
        node1.query(
            """
            select count()
            from remote('node{1,2,3}', default, t)
            group by a
            limit 1 offset 12345
            settings optimize_aggregation_in_order = 1
        """
        )
        == "30\n"
    )

    assert (
        node2.query(
            """
            select count()
            from remote('node{1,2,3}', default, t)
            group by a
            limit 1 offset 12345
            settings optimize_aggregation_in_order = 1
        """
        )
        == "30\n"
    )

    assert (
        node3.query(
            """
            select count()
            from remote('node{1,2,3}', default, t)
            group by a
            limit 1 offset 12345
            settings optimize_aggregation_in_order = 1, enable_memory_bound_merging_of_aggregation_results = 0
        """
        )
        == "30\n"
    )

    node1.query("drop table t")
    node2.query("drop table t")
    node3.query("drop table t")
