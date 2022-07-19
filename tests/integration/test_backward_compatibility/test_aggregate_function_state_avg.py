import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance(
    "node1",
    with_zookeeper=False,
    image="yandex/clickhouse-server",
    tag="19.16.9.37",
    stay_alive=True,
    with_installed_binary=True,
)
node2 = cluster.add_instance(
    "node2",
    with_zookeeper=False,
    image="yandex/clickhouse-server",
    tag="19.16.9.37",
    stay_alive=True,
    with_installed_binary=True,
)
node3 = cluster.add_instance("node3", with_zookeeper=False)
node4 = cluster.add_instance("node4", with_zookeeper=False)


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


# We will test that serialization of internal state of "avg" function is compatible between different versions.
# TODO Implement versioning of serialization format for aggregate function states.
# NOTE This test is too ad-hoc.


def test_backward_compatability(start_cluster):
    node1.query("create table tab (x UInt64) engine = Memory")
    node2.query("create table tab (x UInt64) engine = Memory")
    node3.query("create table tab (x UInt64) engine = Memory")
    node4.query("create table tab (x UInt64) engine = Memory")

    node1.query("INSERT INTO tab VALUES (1)")
    node2.query("INSERT INTO tab VALUES (2)")
    node3.query("INSERT INTO tab VALUES (3)")
    node4.query("INSERT INTO tab VALUES (4)")

    assert (
        node1.query("SELECT avg(x) FROM remote('node{1..4}', default, tab)") == "2.5\n"
    )
    assert (
        node2.query("SELECT avg(x) FROM remote('node{1..4}', default, tab)") == "2.5\n"
    )
    assert (
        node3.query("SELECT avg(x) FROM remote('node{1..4}', default, tab)") == "2.5\n"
    )
    assert (
        node4.query("SELECT avg(x) FROM remote('node{1..4}', default, tab)") == "2.5\n"
    )

    # Also check with persisted aggregate function state

    node1.query("create table state (x AggregateFunction(avg, UInt64)) engine = Log")
    node1.query(
        "INSERT INTO state SELECT avgState(arrayJoin(CAST([1, 2, 3, 4] AS Array(UInt64))))"
    )

    assert node1.query("SELECT avgMerge(x) FROM state") == "2.5\n"

    node1.restart_with_latest_version(fix_metadata=True)

    assert node1.query("SELECT avgMerge(x) FROM state") == "2.5\n"

    node1.query("drop table tab")
    node1.query("drop table state")
    node2.query("drop table tab")
    node3.query("drop table tab")
    node4.query("drop table tab")
