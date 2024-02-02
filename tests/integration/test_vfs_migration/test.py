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


# TODO myrrc add tests for 0copy
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


@pytest.fixture(scope="module")
def prepare_table(started_cluster):
    cluster, parallel = started_cluster
    node: ClickHouseInstance = cluster.instances["node1"]
    node.query(
        "CREATE TABLE test ON CLUSTER cluster (i UInt32, t UInt32 DEFAULT i) "
        "ENGINE=ReplicatedMergeTree('/clickhouse/tables/test', '{replica}') "
        "ORDER BY i PARTITION BY i % 100"
    )
    node.query("INSERT INTO test (i) SELECT * FROM numbers(100000)")
    node.query("ALTER TABLE test UPDATE t = t + 1234 WHERE i % 100 > 0")
    node.query("INSERT INTO test (i) SELECT * FROM numbers(100000, 100000)")


def test_to_vfs(started_cluster, prepare_table):
    cluster, parallel = started_cluster
    node1 = cluster.instances["node1"]

    def validate(a, b):
        assert node1.query("SELECT count(), uniqExact(t) FROM test") == f"{a}\t{b}\n"

    validate(200_000, 199_911)

    def migrate(i):
        node: ClickHouseInstance = cluster.instances[f"node{i}"]
        node.query("SYSTEM SYNC REPLICA test")
        node.copy_file_to_container(
            os.path.join(SCRIPT_DIR, f"configs/vfs.xml"),
            "/etc/clickhouse-server/config.d/vfs.xml",
        )
        node.restart_clickhouse()
        assert node.contains_in_log("Migrated disk s3")
        node.query(
            f"INSERT INTO test (i) SELECT * FROM numbers(200000 + 1000 * {i}, 1000)"
        )

    for i in range(1, 4):
        migrate(i)
    # with ThreadPoolExecutor(max_workers=3 if parallel else 1) as exe:
    #     wait([exe.submit(lambda: migrate(i)) for i in range(1, 4)], timeout=20.0)

    validate(203_000, 199_911)
    node1.query("DROP TABLE test ON CLUSTER cluster SYNC")
