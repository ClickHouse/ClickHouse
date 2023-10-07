#!/usr/bin/env python3

import logging
import random
import string
import time

import pytest
from helpers.cluster import ClickHouseCluster
import minio


cluster = ClickHouseCluster(__file__)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.add_instance(
            "node1",
            main_configs=["configs/storage_conf.xml"],
            with_minio=True,
            with_zookeeper=True,
        )
        cluster.start()

        yield cluster
    finally:
        cluster.shutdown()


def get_objects_in_data_path():
    minio = cluster.minio_client
    objects = minio.list_objects(cluster.minio_bucket, "data/", recursive=True)
    return [obj.object_name for obj in objects]


def test_drop_after_fetch(started_cluster):
    node1 = cluster.instances["node1"]

    node1.query(
        """
CREATE TABLE test_local(c1 Int8, c2 Date) ENGINE = ReplicatedMergeTree('/test/tables/shard/test_local', '1') PARTITION BY c2 ORDER BY c2
        """
    )
    node1.query(
        """
CREATE TABLE test_s3(c1 Int8, c2 Date) ENGINE = ReplicatedMergeTree('/test/tables/shard/test_s3', '1') PARTITION BY c2 ORDER BY c2 SETTINGS storage_policy = 's3', allow_remote_fs_zero_copy_replication=0
        """
    )

    node1.query("INSERT INTO test_local VALUES (1, '2023-10-04'), (2, '2023-10-04')")

    assert node1.query("SELECT count() FROM test_local") == "2\n"

    objects_before = get_objects_in_data_path()
    node1.query(
        "ALTER TABLE test_s3 FETCH PARTITION '2023-10-04' FROM '/test/tables/shard/test_local'"
    )

    node1.query(
        "ALTER TABLE test_s3 DROP DETACHED PARTITION '2023-10-04' SETTINGS allow_drop_detached = 1"
    )

    objects_after = get_objects_in_data_path()

    assert objects_before == objects_after
