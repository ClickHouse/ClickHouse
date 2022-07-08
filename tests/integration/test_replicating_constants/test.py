import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance("node1", with_zookeeper=True)
node2 = cluster.add_instance(
    "node2",
    with_zookeeper=True,
    image="yandex/clickhouse-server",
    tag="19.1.14",
    with_installed_binary=True,
)


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()

        yield cluster
    finally:
        cluster.shutdown()


def test_different_versions(start_cluster):
    assert (
        node1.query(
            "SELECT uniqExact(x) FROM (SELECT version() as x from remote('node{1,2}', system.one))"
        )
        == "2\n"
    )
