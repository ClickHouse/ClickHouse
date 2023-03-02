#!/usr/bin/env python3

import logging
import random
import string
import time

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
        cluster.add_instance(
            "node2",
            main_configs=["configs/storage_conf.xml"],
            with_minio=True,
            with_zookeeper=True,
        )
        cluster.start()

        yield cluster
    finally:
        cluster.shutdown()


def test_fetch_correct_volume(started_cluster):
    node1 = cluster.instances["node1"]
    node2 = cluster.instances["node2"]

    node1.query(
        """
CREATE TABLE test1 (EventDate Date, CounterID UInt32)
ENGINE = ReplicatedMergeTree('/clickhouse-tables/test1', 'r1')
PARTITION BY toMonday(EventDate)
ORDER BY (CounterID, EventDate)
SETTINGS index_granularity = 8192, storage_policy = 's3'"""
    )

    node1.query(
        "INSERT INTO test1 SELECT toDate('2023-01-01') + toIntervalDay(number), number + 1000 from system.numbers limit 20"
    )

    def get_part_to_disk(query_result):
        part_to_disk = {}
        for row in query_result.strip().split("\n"):
            print(row)
            disk, part = row.split("\t")
            part_to_disk[part] = disk
        return part_to_disk

    part_to_disk = get_part_to_disk(
        node1.query(
            "SELECT disk_name, name FROM system.parts where table = 'test1' and active"
        )
    )
    for disk in part_to_disk.values():
        assert disk == "default"

    node1.query("ALTER TABLE test1 MOVE PARTITION '2022-12-26' TO DISK 's3'")
    node1.query("ALTER TABLE test1 MOVE PARTITION '2023-01-02' TO DISK 's3'")
    node1.query("ALTER TABLE test1 MOVE PARTITION '2023-01-09' TO DISK 's3'")

    part_to_disk = get_part_to_disk(
        node1.query(
            "SELECT disk_name, name FROM system.parts where table = 'test1' and active"
        )
    )
    assert part_to_disk["20221226_0_0_0"] == "s3"
    assert part_to_disk["20230102_0_0_0"] == "s3"
    assert part_to_disk["20230109_0_0_0"] == "s3"
    assert part_to_disk["20230116_0_0_0"] == "default"

    node2.query(
        """
CREATE TABLE test1 (EventDate Date, CounterID UInt32)
ENGINE = ReplicatedMergeTree('/clickhouse-tables/test1', 'r2')
PARTITION BY toMonday(EventDate)
ORDER BY (CounterID, EventDate)
SETTINGS index_granularity = 8192, storage_policy = 's3'"""
    )

    node2.query("SYSTEM SYNC REPLICA test1")

    part_to_disk = get_part_to_disk(
        node2.query(
            "SELECT disk_name, name FROM system.parts where table = 'test1' and active"
        )
    )
    assert part_to_disk["20221226_0_0_0"] == "s3"
    assert part_to_disk["20230102_0_0_0"] == "s3"
    assert part_to_disk["20230109_0_0_0"] == "s3"
    assert part_to_disk["20230116_0_0_0"] == "default"
