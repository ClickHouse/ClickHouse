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
                stay_alive=True,
            )
        cluster.start()

        yield cluster
    finally:
        cluster.shutdown()


@pytest.fixture(scope="module")
def prepare_table(started_cluster):
    node: ClickHouseInstance = started_cluster.instances["node1"]
    node.query(
        "CREATE TABLE test ON CLUSTER cluster (i UInt32, t UInt32 DEFAULT i) "
        "ENGINE=ReplicatedMergeTree('/clickhouse/tables/test', '{replica}') "
        "ORDER BY i"
    )
    node.query("INSERT INTO test (i) SELECT * FROM numbers(100000)")
    node.query("ALTER TABLE test UPDATE t = t + 1234 WHERE i > 10000 AND i % 1000 = 0")
    node.query("INSERT INTO test (i) SELECT * FROM numbers(100000, 200000)")


def test_to_vfs(started_cluster, prepare_table):
    for i in range(1, 4):
        node: ClickHouseInstance = started_cluster.instances[f"node{i}"]
        node.query("SYSTEM SYNC REPLICA test")
        node.copy_file_to_container(
            os.path.join(SCRIPT_DIR, f"configs/vfs.xml"),
            "/etc/clickhouse-server/config.d/vfs.xml",
        )
        node.restart_clickhouse()
        node.wait_for_log_line("VFSMigration(disk): Migrated disk")

    started_cluster.instances["node1"].query("DROP TABLE test ON CLUSTER cluster SYNC")
