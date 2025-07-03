import logging
import os
import time

import pytest

from helpers.cluster import ClickHouseCluster
cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=["configs/split_cache.xml"],
    stay_alive=True,
    with_minio=True,
)

@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()

def test_split_cache(started_cluster):
    node.query("DROP TABLE IF EXISTS t0")
    node.query(
        """CREATE TABLE t0 (
            key String,
            value String
        )
        ENGINE = MergeTree
        PRIMARY KEY key
        SETTINGS storage_policy = 'split_cache'"""
    )
    node.query("INSERT INTO t0 VALUES ('key1', 'value1')")
    assert node.query("SHOW FILESYSTEM CACHES") == "split_s3_cache_system\nsplit_s3_cache_data\n"
    node.query("DETACH TABLE t0")
    node.query("ATTACH TABLE t0")
    node.restart_clickhouse()
    node.query("DROP TABLE t0")
