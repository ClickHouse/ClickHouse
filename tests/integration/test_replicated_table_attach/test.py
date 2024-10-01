import os

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.network import PartitionManager

cluster = ClickHouseCluster(__file__)
CONFIG_DIR = os.path.join(os.path.dirname(os.path.realpath(__file__)), "configs")

node = cluster.add_instance(
    "node",
    main_configs=["configs/config.xml"],
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


def start_clean_clickhouse():
    # remove fault injection if present
    if "fault_injection.xml" in node.exec_in_container(
        ["bash", "-c", "ls /etc/clickhouse-server/config.d"]
    ):
        print("Removing fault injection")
        node.exec_in_container(
            ["bash", "-c", "rm /etc/clickhouse-server/config.d/fault_injection.xml"]
        )
        node.restart_clickhouse()


def test_startup_with_small_bg_pool(started_cluster):
    start_clean_clickhouse()
    node.query("DROP TABLE IF EXISTS replicated_table SYNC")
    node.query(
        "CREATE TABLE replicated_table (k UInt64, i32 Int32) ENGINE=ReplicatedMergeTree('/clickhouse/replicated_table', 'r1') ORDER BY k"
    )

    node.query("INSERT INTO replicated_table VALUES(20, 30)")

    def assert_values():
        assert node.query("SELECT * FROM replicated_table") == "20\t30\n"

    assert_values()
    node.restart_clickhouse(stop_start_wait_sec=10)
    assert_values()


def test_startup_with_small_bg_pool_partitioned(started_cluster):
    start_clean_clickhouse()
    node.query("DROP TABLE IF EXISTS replicated_table_partitioned SYNC")
    node.query(
        "CREATE TABLE replicated_table_partitioned (k UInt64, i32 Int32) ENGINE=ReplicatedMergeTree('/clickhouse/replicated_table_partitioned', 'r1') ORDER BY k"
    )

    node.query("INSERT INTO replicated_table_partitioned VALUES(20, 30)")

    def assert_values():
        assert node.query("SELECT * FROM replicated_table_partitioned") == "20\t30\n"

    assert_values()
    with PartitionManager() as pm:
        pm.drop_instance_zk_connections(node)
        node.stop_clickhouse(stop_wait_sec=150)
        node.copy_file_to_container(
            os.path.join(CONFIG_DIR, "fault_injection.xml"),
            "/etc/clickhouse-server/config.d/fault_injection.xml",
        )
        node.start_clickhouse(start_wait_sec=150)
        assert_values()

    # check that we activate it in the end
    node.query_with_retry(
        "INSERT INTO replicated_table_partitioned VALUES(20, 30)",
        retry_count=20,
        sleep_time=3,
    )
