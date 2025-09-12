import pytest

from helpers.cluster import CLICKHOUSE_CI_MIN_TESTED_VERSION, ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance(
    "node1",
    image="clickhouse/clickhouse-server",
    tag=CLICKHOUSE_CI_MIN_TESTED_VERSION,
    stay_alive=True,
    with_installed_binary=True,
)
node2 = cluster.add_instance(
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
    assert node2.query(
        """
    set optimize_const_name_size = 0;
    select count() from (select arrayResize([1], 100000) from remote('node{1,2}', numbers(1))) group by ();
        """
    ) == "2\n"
