#!/usr/bin/env python3

import logging
import random
import string
import time

import minio
import pytest

from helpers.cluster import ClickHouseCluster

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


def test_drop_complex_columns(started_cluster):
    start_objects = get_objects_in_data_path()
    print("Objects before", start_objects)
    node1 = cluster.instances["node1"]
    node1.query(
        """
CREATE TABLE test_s3_complex_types(
c1	Int8,
c2	Date,
`c3.k`	Array(String),
`c3.v1`	Array(Int64),
`c3.v3`	Array(Int64),
`c3.v4`	Array(Int64)
) ENGINE = MergeTree
order by (c1,c2) SETTINGS storage_policy = 's3',
min_bytes_for_wide_part=1,
vertical_merge_algorithm_min_rows_to_activate=1,
vertical_merge_algorithm_min_columns_to_activate=1;"""
    )

    node1.query(
        "insert into test_s3_complex_types values(1,toDate('2020-10-01'), ['a','b'], [1,2], [3,4], [5,6])"
    )

    node1.query(
        "insert into test_s3_complex_types values(1,toDate('2020-10-01'), ['a','b'], [7,8], [9,10], [11,12])"
    )

    print("Objects in insert", get_objects_in_data_path())
    node1.query("optimize table test_s3_complex_types final")

    print("Objects in optimize", get_objects_in_data_path())

    node1.query("DROP TABLE test_s3_complex_types SYNC")
    end_objects = get_objects_in_data_path()
    print("Objects after drop", end_objects)
    assert start_objects == end_objects
