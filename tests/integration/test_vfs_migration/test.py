#!/usr/bin/env python3

import time
import io
import os
import pytest
from kazoo.client import KazooClient
from helpers.cluster import ClickHouseCluster, ClickHouseInstance
from helpers.network import PartitionManager
from concurrent.futures import ThreadPoolExecutor, wait

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))


# TODO myrrc dimension for 0copy
# TODO myrrc dimension for large data
@pytest.fixture(scope="module", params=[False, True], ids=["sequential", "parallel"])
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

        yield cluster, request.param
    finally:
        cluster.shutdown()


def test_to_vfs(started_cluster):
    cluster, parallel = started_cluster
    node1: ClickHouseInstance = cluster.instances["node1"]

    node1.query(
        "CREATE TABLE test ON CLUSTER cluster (i UInt32, t UInt32 DEFAULT i) "
        "ENGINE=ReplicatedMergeTree('/clickhouse/tables/test', '{replica}') "
        "ORDER BY i PARTITION BY i % 10"
    )
    node1.query("INSERT INTO test (i) SELECT * FROM numbers(2)")
    node1.query("ALTER TABLE test UPDATE t = t + 5 WHERE i > 0")
    node1.query("INSERT INTO test (i) SELECT * FROM numbers(2, 1)")

    def validate(node, a, b):
        assert node.query("SELECT count(), uniqExact(t) FROM test") == f"{a}\t{b}\n"

    validate(node1, 3, 3)

    def migrate(i):
        print(f"Migrating node {i}")
        node: ClickHouseInstance = cluster.instances[f"node{i}"]
        node.query("SYSTEM SYNC REPLICA test")
        node.copy_file_to_container(
            os.path.join(SCRIPT_DIR, f"configs/vfs.xml"),
            "/etc/clickhouse-server/config.d/vfs.xml",
        )
        node.restart_clickhouse()
        assert node.contains_in_log("Migrated disk s3")
        node.query(f"INSERT INTO test (i) SELECT * FROM numbers(3 + {i}, 1)")

    with ThreadPoolExecutor(max_workers=3 if parallel else 1) as exe:
        exe.map(migrate, range(1, 4))
        exe.shutdown(wait=True)

    for i in range(1, 4):
        node: ClickHouseInstance = cluster.instances[f"node{i}"]
        node.query("SYSTEM SYNC REPLICA test")
        validate(node, 6, 5)

    node1.query("DROP TABLE test ON CLUSTER cluster SYNC")
