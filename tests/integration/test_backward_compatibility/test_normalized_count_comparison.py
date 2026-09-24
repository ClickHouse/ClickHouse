import pytest

from helpers.cluster import CLICKHOUSE_CI_MIN_TESTED_VERSION, ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance("node1", with_zookeeper=False, use_old_analyzer=True)
node2 = cluster.add_instance(
    "node2",
    with_zookeeper=False,
    image="clickhouse/clickhouse-server",
    tag=CLICKHOUSE_CI_MIN_TESTED_VERSION,
    stay_alive=True,
    with_installed_binary=True,
)


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


def test_select_aggregate_alias_column(start_cluster):
    node1.query(
        "create table tab (x UInt64, y String, z Nullable(Int64)) engine = Memory"
    )
    node2.query(
        "create table tab (x UInt64, y String, z Nullable(Int64)) engine = Memory"
    )
    node1.query("insert into tab values (1, 'a', null)")
    node2.query("insert into tab values (1, 'a', null)")

    node1.query(
        "select count(), count(1), count(x), count(y), count(z) from remote('node{1,2}', default, tab)"
    )
    node2.query(
        "select count(), count(1), count(x), count(y), count(z) from remote('node{1,2}', default, tab)"
    )

    node1.query("drop table tab")
    node2.query("drop table tab")
