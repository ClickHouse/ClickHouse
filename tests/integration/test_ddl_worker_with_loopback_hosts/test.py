import time

import pytest

from helpers.client import QueryRuntimeException
from helpers.cluster import ClickHouseCluster
from helpers.network import PartitionManager

cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance(
    "node1",
    main_configs=[
        "configs/remote_servers.xml",
    ],
    with_zookeeper=True,
)
node2 = cluster.add_instance(
    "node2",
    main_configs=[
        "configs/remote_servers.xml",
    ],
    with_zookeeper=True,
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        yield cluster
    finally:
        cluster.shutdown()


def count_table(node, table_name):
    return int(
        node.query(
            f"SELECT count() FROM system.tables WHERE name='{table_name}'"
        ).strip()
    )


def test_ddl_worker_with_loopback_hosts(
    started_cluster,
):
    node1.query("DROP TABLE IF EXISTS t1 SYNC")
    node2.query("DROP TABLE IF EXISTS t1 SYNC")
    node1.query("DROP TABLE IF EXISTS t2 SYNC")
    node2.query("DROP TABLE IF EXISTS t2 SYNC")
    node1.query("DROP TABLE IF EXISTS t3 SYNC")
    node2.query("DROP TABLE IF EXISTS t3 SYNC")
    node1.query("DROP TABLE IF EXISTS t4 SYNC")
    node2.query("DROP TABLE IF EXISTS t4 SYNC")

    node1.query(
        "CREATE TABLE t1 ON CLUSTER 'test_cluster' (x INT) ENGINE=MergeTree() ORDER BY x",
        settings={
            "distributed_ddl_task_timeout": 10,
        },
    )

    node2.query(
        "CREATE TABLE t2 ON CLUSTER 'test_cluster' (x INT) ENGINE=MergeTree() ORDER BY x",
        settings={
            "distributed_ddl_task_timeout": 10,
        },
    )

    assert count_table(node1, "t2") == 1
    assert count_table(node2, "t1") == 1

    node1.query(
        "CREATE TABLE t3 ON CLUSTER 'test_loopback_cluster1' (x INT) ENGINE=MergeTree() ORDER BY x",
        settings={
            "distributed_ddl_task_timeout": 10,
        },
    )
    # test_loopback_cluster1 has a loopback host, only 1 replica processed the query
    assert count_table(node1, "t3") == 1 or count_table(node2, "t3") == 1

    node2.query(
        "CREATE TABLE t4 ON CLUSTER 'test_loopback_cluster2' (x INT) ENGINE=MergeTree() ORDER BY x",
        settings={
            "distributed_ddl_task_timeout": 10,
        },
    )
    # test_loopback_cluster2 has a loopback host, only 1 replica processed the query
    assert count_table(node1, "t4") == 1 or count_table(node2, "t4") == 1

    node1.query("DROP TABLE IF EXISTS t1 SYNC")
    node2.query("DROP TABLE IF EXISTS t1 SYNC")
    node1.query("DROP TABLE IF EXISTS t2 SYNC")
    node2.query("DROP TABLE IF EXISTS t2 SYNC")
    node1.query("DROP TABLE IF EXISTS t3 SYNC")
    node2.query("DROP TABLE IF EXISTS t3 SYNC")
    node1.query("DROP TABLE IF EXISTS t4 SYNC")
    node2.query("DROP TABLE IF EXISTS t4 SYNC")
