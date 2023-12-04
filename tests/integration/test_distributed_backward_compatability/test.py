import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node_old = cluster.add_instance(
    "node1",
    main_configs=["configs/remote_servers.xml"],
    image="yandex/clickhouse-server",
    tag="20.8.9.6",
    stay_alive=True,
    with_installed_binary=True,
)
node_new = cluster.add_instance(
    "node2",
    main_configs=["configs/remote_servers.xml"],
    user_configs=["configs/legacy.xml"],
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        for node in (node_old, node_new):
            node.query(
                "CREATE TABLE local_table(id UInt32, val String) ENGINE = MergeTree ORDER BY id"
            )

        node_old.query("INSERT INTO local_table VALUES (1, 'node1')")
        node_new.query("INSERT INTO local_table VALUES (2, 'node2')")

        node_old.query(
            "CREATE TABLE distributed(id UInt32, val String) ENGINE = Distributed(test_cluster, default, local_table)"
        )
        node_new.query(
            "CREATE TABLE distributed(id UInt32, val String) ENGINE = Distributed(test_cluster, default, local_table)"
        )

        yield cluster

    finally:
        cluster.shutdown()


def test_distributed_in_tuple(started_cluster):
    query1 = "SELECT count() FROM distributed WHERE (id, val) IN ((1, 'node1'), (2, 'a'), (3, 'b'))"
    query2 = (
        "SELECT sum((id, val) IN ((1, 'node1'), (2, 'a'), (3, 'b'))) FROM distributed"
    )
    assert node_old.query(query1) == "1\n"
    assert node_old.query(query2) == "1\n"
    assert node_new.query(query1) == "1\n"
    assert node_new.query(query2) == "1\n"

    large_set = "(" + ",".join([str(i) for i in range(1000)]) + ")"
    query3 = "SELECT count() FROM distributed WHERE id IN " + large_set
    query4 = "SELECT sum(id IN {}) FROM distributed".format(large_set)
    assert node_old.query(query3) == "2\n"
    assert node_old.query(query4) == "2\n"
    assert node_new.query(query3) == "2\n"
    assert node_new.query(query4) == "2\n"
