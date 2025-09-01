import time

import pytest

from helpers.client import QueryRuntimeException
from helpers.cluster import ClickHouseCluster
from helpers.network import PartitionManager

cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance(
    "node1",
    main_configs=[
        "configs/remote_servers1.xml",
        "configs/disallow_ddl_loopback_hosts.xml",
    ],
    with_zookeeper=True,
)
node2 = cluster.add_instance(
    "node2",
    main_configs=[
        "configs/remote_servers1.xml",
        "configs/disallow_ddl_loopback_hosts.xml",
    ],
    with_zookeeper=True,
)

node3 = cluster.add_instance(
    "node3",
    main_configs=[
        "configs/remote_servers2.xml",
        "configs/allow_ddl_loopback_hosts.xml",
    ],
    with_zookeeper=True,
)
node4 = cluster.add_instance(
    "node4",
    main_configs=[
        "configs/remote_servers2.xml",
        "configs/allow_ddl_loopback_hosts.xml",
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


def test_ddl_worker_with_loopback_hosts_without_allowing_ddl_loopback_hosts(
    started_cluster,
):
    node1.query("DROP TABLE IF EXISTS t1 SYNC")
    node2.query("DROP TABLE IF EXISTS t1 SYNC")
    node1.query("DROP TABLE IF EXISTS t2")
    node2.query("DROP TABLE IF EXISTS t2")

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

    assert (
        node1.query_with_retry(
            "SELECT count() FROM system.tables WHERE name='t2'",
            check_callback=lambda x: x.strip() == "1",
        ).strip()
        == "1"
    )
    assert (
        node2.query_with_retry(
            "SELECT count() FROM system.tables WHERE name='t1'",
            check_callback=lambda x: x.strip() == "1",
        ).strip()
        == "1"
    )

    node1.query("DROP TABLE IF EXISTS t1 SYNC")
    node2.query("DROP TABLE IF EXISTS t1 SYNC")
    node1.query("DROP TABLE IF EXISTS t2")
    node2.query("DROP TABLE IF EXISTS t2")


def test_ddl_worker_with_loopback_hosts_with_allowing_ddl_loopback_hosts(
    started_cluster,
):
    node3.query("DROP TABLE IF EXISTS t1 SYNC")
    node4.query("DROP TABLE IF EXISTS t1 SYNC")
    node3.query("DROP TABLE IF EXISTS t2")
    node4.query("DROP TABLE IF EXISTS t2")

    assert "is not finished on" in node3.query_and_get_error(
        "CREATE TABLE t1 ON CLUSTER 'test_cluster' (x INT) ENGINE=MergeTree() ORDER BY x",
        settings={
            "distributed_ddl_task_timeout": 10,
        },
    )

    assert "is not finished on" in node4.query_and_get_error(
        "CREATE TABLE t2 ON CLUSTER 'test_cluster' (x INT) ENGINE=MergeTree() ORDER BY x",
        settings={
            "distributed_ddl_task_timeout": 10,
        },
    )

    node3.query("DROP TABLE IF EXISTS t1 SYNC")
    node4.query("DROP TABLE IF EXISTS t1 SYNC")
    node3.query("DROP TABLE IF EXISTS t2")
    node4.query("DROP TABLE IF EXISTS t2")
