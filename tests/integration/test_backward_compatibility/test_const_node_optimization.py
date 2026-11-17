import pytest

from helpers.cluster import CLICKHOUSE_CI_MIN_TESTED_VERSION, ClickHouseCluster

cluster = ClickHouseCluster(__file__)
old_node = cluster.add_instance(
    "node1",
    image="clickhouse/clickhouse-server",
    tag="24.8",
    stay_alive=True,
    with_installed_binary=True,
)
new_node = cluster.add_instance(
    "node2",
)


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_const_node_optimization(start_cluster):
    assert new_node.query(
        """
    set optimize_const_name_size = 0;
    select count() from (select arrayResize([1], 100000) from remote('node{1,2}', numbers(1))) group by ();
        """
    ) == "2\n"

def test_const_node_optimization_from_old(start_cluster):
    assert old_node.query(
        """
    select count() from (select arrayResize([1], 100000) from remote('node{1,2}', numbers(1))) group by ();
        """
    ) == "2\n"

def test_const_node_optimization_group_by(start_cluster):
    assert new_node.query(
        """
    set optimize_const_name_size = 0;
    select count() from (select sum(number) from remote('node{1,2}', numbers(1)) group by [number] || arrayResize([1], 100000));
        """
    ) == "1\n"

def test_const_node_optimization_group_by_from_old(start_cluster):
    assert old_node.query(
        """
    select count() from (select sum(number) from remote('node{1,2}', numbers(1)) group by [number] || arrayResize([1], 100000));
        """
    ) == "1\n"
