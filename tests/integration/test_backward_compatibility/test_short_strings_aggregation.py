import pytest

from helpers.cluster import CLICKHOUSE_CI_MIN_TESTED_VERSION, ClickHouseCluster, is_arm

# For arm version see https://github.com/ClickHouse/ClickHouse/pull/59132
cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance(
    "node1",
    with_zookeeper=False,
    image="clickhouse/clickhouse-server",
    tag="24.1" if is_arm() else CLICKHOUSE_CI_MIN_TESTED_VERSION,
    stay_alive=True,
    with_installed_binary=True,
)
node2 = cluster.add_instance(
    "node2",
    with_zookeeper=False,
    image="clickhouse/clickhouse-server",
    tag="24.1" if is_arm() else CLICKHOUSE_CI_MIN_TESTED_VERSION,
    stay_alive=True,
    with_installed_binary=True,
)
node3 = cluster.add_instance("node3", with_zookeeper=False, use_old_analyzer=True)


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


def test_backward_compatability(start_cluster):
    node1.query("create table tab (s String) engine = MergeTree order by s")
    node2.query("create table tab (s String) engine = MergeTree order by s")
    node1.query("insert into tab select number from numbers(50)")
    node2.query("insert into tab select number from numbers(1000000)")
    res = node3.query(
        "select s, count() from remote('node{1,2}', default, tab) group by s order by toUInt64(s) limit 50"
    )
    print(res)
    assert res == "".join("{}\t2\n".format(i) for i in range(50))
    node1.query("drop table tab")
    node2.query("drop table tab")
