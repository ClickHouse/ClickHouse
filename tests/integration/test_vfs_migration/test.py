#!/usr/bin/env python3

import time
import io
import os
import pytest
from kazoo.client import KazooClient
from helpers.cluster import ClickHouseCluster, ClickHouseInstance
from helpers.network import PartitionManager

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))


# TODO myrrc add tests for 0copy
@pytest.fixture(scope="module")
def started_cluster(request):
    cluster = ClickHouseCluster(__file__)
    try:
        for i in range(1, 4):
            cluster.add_instance(
                f"node{i}",
                main_configs=["configs/config.xml"],
                with_zookeeper=True,
                with_minio=True,
                macros={"replica": f"node{i}"},
            )
        cluster.start()

        yield cluster
    finally:
        cluster.shutdown()


@pytest.fixture(scope="module")
def prepare_table(started_cluster):
    node: ClickHouseInstance = started_cluster.instances["node1"]
    node.query(
        "CREATE TABLE test ON CLUSTER cluster (i UInt32) "
        "ENGINE=ReplicatedMergeTree('/clickhouse/tables/test', '{replica}') "
        "ORDER BY i PARTITION BY i % 100"
    )
    node.query("INSERT INTO test SELECT numbers(10000000)")
    node.query("ALTER TABLE test UPDATE i = i + 1234 WHERE i > 10000 AND i % 1000 = 0")
    node.query("INSERT INTO test SELECT numbers(10000000, 20000000)")


def test_to_vfs(started_cluster, prepare_table):
    for i in range(1, 4):
        node: ClickHouseInstance = started_cluster.instances[f"node{i}"]
        node.copy_file_to_container(
            os.path.join(SCRIPT_DIR, f"configs/vfs.xml"),
            "/etc/clickhouse-server/config.d/vfs.xml",
        )
        node.restart_clickhouse()
        node.wait_for_log_line("VFSMigration(disk): Migrated disk")
        # TODO myrrc can we get rid of this and just use wait_for_log_line?
        assert node.contains_in_log("VFSMigration(disk): Migrated disk")

    started_cluster.instances["node1"].query("DROP TABLE test ON CLUSTER cluster SYNC")
