import time

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance(
    "node1",
    main_configs=["configs/remote_servers.xml"],
    with_zookeeper=True,
    stay_alive=True,
)
node2 = cluster.add_instance(
    "node2", main_configs=["configs/remote_servers.xml"], with_zookeeper=True
)
node3 = cluster.add_instance(
    "node3", main_configs=["configs/remote_servers.xml"], with_zookeeper=True
)
node4 = cluster.add_instance(
    "node4",
    main_configs=["configs/remote_servers.xml"],
    with_zookeeper=True,
    stay_alive=True,
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


def test_stop_waiting_for_offline_hosts(started_cluster):
    timeout = 10
    settings = {"distributed_ddl_task_timeout": timeout}

    node1.query(
        "DROP TABLE IF EXISTS test_table ON CLUSTER test_cluster SYNC",
        settings=settings,
    )

    node1.query(
        "CREATE TABLE test_table ON CLUSTER test_cluster (x Int) Engine=Memory",
        settings=settings,
    )

    try:
        node4.stop_clickhouse()

        start = time.time()
        assert "Code: 159. DB::Exception" in node1.query_and_get_error(
            "DROP TABLE IF EXISTS test_table ON CLUSTER test_cluster SYNC",
            settings=settings,
        )
        assert time.time() - start >= timeout

        start = time.time()
        assert "Code: 159. DB::Exception" in node1.query_and_get_error(
            "CREATE TABLE test_table ON CLUSTER test_cluster (x Int) Engine=Memory",
            settings=settings,
        )
        assert time.time() - start >= timeout

        # set `distributed_ddl_output_mode` = `throw_only_active``
        settings = {
            "distributed_ddl_task_timeout": timeout,
            "distributed_ddl_output_mode": "throw_only_active",
        }

        start = time.time()
        node1.query(
            "DROP TABLE IF EXISTS test_table ON CLUSTER test_cluster SYNC",
            settings=settings,
        )

        start = time.time()
        node1.query(
            "CREATE TABLE test_table ON CLUSTER test_cluster (x Int) Engine=Memory",
            settings=settings,
        )
    finally:
        node4.start_clickhouse()
