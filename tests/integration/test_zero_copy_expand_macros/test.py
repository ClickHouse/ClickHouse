#!/usr/bin/env python3

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)


def get_macros():
    return {
        "shard": "01",
    }

@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.add_instance(
            "node",
            main_configs=["configs/storage_conf.xml"],
            with_minio=True,
            with_zookeeper=True,
            macros=get_macros(),
        )
        cluster.start()

        yield cluster
    finally:
        cluster.shutdown()


def test_drop_after_fetch(started_cluster):
    node = cluster.instances["node"]

    node.query(
        """
        CREATE TABLE test_s3(c1 Int8, c2 Date)
        ENGINE = ReplicatedMergeTree('/test/tables/{shard}/test_s3', '1')
        PARTITION BY c2
        ORDER BY c2
        SETTINGS
            storage_policy = 's3',
            allow_remote_fs_zero_copy_replication=1,
            remote_fs_zero_copy_zookeeper_path = '/clickhouse/zero_copy/{shard}'
        """
    )

    macros = get_macros()

    shard = node.query("SELECT name FROM system.zookeeper WHERE path='/clickhouse/zero_copy/'").strip()
    assert shard == macros["shard"]

    out = node.query(f"SELECT name FROM system.zookeeper WHERE path='/clickhouse/zero_copy/{shard}'").strip()
    assert out == "zero_copy_s3"


    node.query("DROP TABLE test_s3 SYNC")
