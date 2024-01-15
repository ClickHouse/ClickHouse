import pytest

from helpers.cluster import ClickHouseCluster
from helpers.client import QueryRuntimeException

cluster = ClickHouseCluster(__file__)

node_shard = cluster.add_instance("node1", main_configs=["configs/remote_servers.xml"])

node_dist = cluster.add_instance(
    "node2",
    main_configs=["configs/remote_servers.xml"],
    image="yandex/clickhouse-server",
    tag="21.11.9.1",
    stay_alive=True,
    with_installed_binary=True,
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        node_shard.query(
            "CREATE TABLE local_table(id UInt32, val String) ENGINE = MergeTree ORDER BY id"
        )
        node_dist.query(
            "CREATE TABLE local_table(id UInt32, val String) ENGINE = MergeTree ORDER BY id"
        )
        node_dist.query(
            "CREATE TABLE dist_table(id UInt32, val String) ENGINE = Distributed(test_cluster, default, local_table, rand())"
        )

        yield cluster

    finally:
        cluster.shutdown()


def test_distributed_in_tuple(started_cluster):
    node_dist.query("SYSTEM STOP DISTRIBUTED SENDS dist_table")

    node_dist.query("INSERT INTO dist_table VALUES (1, 'foo')")
    assert node_dist.query("SELECT count() FROM dist_table") == "0\n"
    assert node_shard.query("SELECT count() FROM local_table") == "0\n"

    node_dist.restart_with_latest_version(signal=9)
    node_dist.query("SYSTEM FLUSH DISTRIBUTED dist_table")

    assert node_dist.query("SELECT count() FROM dist_table") == "1\n"
    assert node_shard.query("SELECT count() FROM local_table") == "1\n"
