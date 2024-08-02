import os

import pytest
from helpers.test_tools import TSV
from helpers.cluster import ClickHouseCluster

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
CONFIG_DIR = os.path.join(SCRIPT_DIR, "configs")

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance("node", main_configs=["configs/disks.xml"], stay_alive=True)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_storage_policy_configuration_change(started_cluster):
    node.query(
        "CREATE TABLE a (x UInt64) ENGINE = MergeTree ORDER BY x SETTINGS storage_policy = 'test_policy'"
    )

    node.stop_clickhouse()
    node.copy_file_to_container(
        os.path.join(CONFIG_DIR, "disk2_only.xml"),
        "/etc/clickhouse-server/config.d/disks.xml",
    )
    node.start_clickhouse()

    node.stop_clickhouse()
    node.copy_file_to_container(
        os.path.join(CONFIG_DIR, "disks.xml"),
        "/etc/clickhouse-server/config.d/disks.xml",
    )
    node.start_clickhouse()


def test_disk_is_immutable(started_cluster):
    node.query("DROP TABLE IF EXISTS test_1")

    node.query(
        """
        create table test_1 (a Int32)
        engine = MergeTree()
        order by tuple()
        settings
            disk=disk(
                name='not_uniq_disk_name',
                type = object_storage,
                object_storage_type = local_blob_storage,
                path='./03215_data_test_1/')
        """
    )

    node.query("INSERT INTO test_1 VALUES (1)")
    node.query("SYSTEM FLUSH LOGS;")

    print(
        node.query(
            "SELECT 'test_1', * FROM system.blob_storage_log"
        )
    )

    print(
        node.query(
            "SELECT 'test_1', * FROM test_1"
        )
    )

    node.query("DROP TABLE test_1 SYNC")
    node.query("DROP TABLE IF EXISTS test_2")

    node.query(
        """
        create table test_2 (a Int32)
        engine = MergeTree()
        order by tuple()
        settings
            disk=disk(
                name='not_uniq_disk_name',
                type = object_storage,
                object_storage_type = local_blob_storage,
                path='./03215_data_test_2/')
        """
    )

    node.query("INSERT INTO test_2 VALUES (1)")
    node.query("SYSTEM FLUSH LOGS;")

    print(
        node.query(
            "SELECT 'test_2', * FROM system.blob_storage_log"
        )
    )

    print(
        node.query(
            "SELECT 'test_2', * FROM test_2"
        )
    )

    node.restart_clickhouse()

    print(
        node.query(
            "SELECT 'test_2', * FROM system.blob_storage_log"
        )
    )

    print(
        node.query(
            "SELECT 'test_2', * FROM test_2"
        )
    )
