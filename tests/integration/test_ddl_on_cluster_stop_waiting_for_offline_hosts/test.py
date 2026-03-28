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
    timeout = 5
    settings = {"distributed_ddl_task_timeout": timeout}

    node1.query(
        "DROP TABLE IF EXISTS test_table ON CLUSTER test_cluster SYNC",
        settings=settings,
    )

    node1.query(
        "CREATE TABLE test_table ON CLUSTER test_cluster (x Int) Engine=Memory",
        settings=settings,
    )
    distributed_ddl_output_mode_pass_list = [
        "null_status_on_timeout",
        "never_throw",
        "none_only_active",
        "null_status_on_timeout_only_active",
        "throw_only_active",
    ]
    distributed_ddl_output_mode_error_list = ["throw", "none"]

    try:
        node4.stop_clickhouse()
        for distributed_ddl_output_mode in distributed_ddl_output_mode_pass_list:
            settings = {
                "distributed_ddl_task_timeout": timeout,
                "distributed_ddl_output_mode": distributed_ddl_output_mode,
            }

            start = time.time()
            print(f"distributed_ddl_output_mode {distributed_ddl_output_mode}")
            node1.query(
                f"CREATE TABLE test_table_{distributed_ddl_output_mode} ON CLUSTER test_cluster (x Int) Engine=Memory",
                settings=settings,
            )

        for distributed_ddl_output_mode in distributed_ddl_output_mode_error_list:
            settings = {
                "distributed_ddl_task_timeout": timeout,
                "distributed_ddl_output_mode": distributed_ddl_output_mode,
            }

            start = time.time()
            print(f"distributed_ddl_output_mode {distributed_ddl_output_mode}")
            assert "Code: 159. DB::Exception" in node1.query_and_get_error(
                f"CREATE TABLE test_table_{distributed_ddl_output_mode} ON CLUSTER test_cluster (x Int) Engine=Memory",
                settings=settings,
            )
            assert time.time() - start >= timeout
    finally:
        node4.start_clickhouse()

    settings = {"distributed_ddl_task_timeout": timeout}
    for distributed_ddl_output_mode in distributed_ddl_output_mode_pass_list:
        node1.query(
            f"DROP TABLE IF EXISTS test_table_{distributed_ddl_output_mode} ON CLUSTER test_cluster SYNC",
            settings=settings,
        )
    for distributed_ddl_output_mode in distributed_ddl_output_mode_error_list:
        node1.query(
            f"DROP TABLE IF EXISTS test_table_{distributed_ddl_output_mode} ON CLUSTER test_cluster SYNC",
            settings=settings,
        )
