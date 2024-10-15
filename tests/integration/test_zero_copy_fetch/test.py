#!/usr/bin/env python3

import logging
import random
import string
import time
from multiprocessing.dummy import Pool

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.add_instance(
            "node1",
            main_configs=["configs/storage_conf.xml"],
            user_configs=["configs/users.xml"],
            with_minio=True,
            with_zookeeper=True,
        )
        cluster.add_instance(
            "node2",
            main_configs=["configs/storage_conf.xml"],
            user_configs=["configs/users.xml"],
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
SETTINGS index_granularity = 8192, storage_policy = 's3', temporary_directories_lifetime=1"""
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


def test_concurrent_move_to_s3(started_cluster):
    node1 = cluster.instances["node1"]
    node2 = cluster.instances["node2"]

    node1.query(
        """
CREATE TABLE test_concurrent_move (EventDate Date, CounterID UInt32)
ENGINE = ReplicatedMergeTree('/clickhouse-tables/test_concurrent_move', 'r1')
PARTITION BY CounterID
ORDER BY (CounterID, EventDate)
SETTINGS index_granularity = 8192, storage_policy = 's3'"""
    )

    node2.query(
        """
CREATE TABLE test_concurrent_move (EventDate Date, CounterID UInt32)
ENGINE = ReplicatedMergeTree('/clickhouse-tables/test_concurrent_move', 'r2')
PARTITION BY CounterID
ORDER BY (CounterID, EventDate)
SETTINGS index_granularity = 8192, storage_policy = 's3'"""
    )
    partitions = range(10)

    for i in partitions:
        node1.query(
            f"INSERT INTO test_concurrent_move SELECT toDate('2023-01-01') + toIntervalDay(number), {i} from system.numbers limit 20"
        )
        node1.query(
            f"INSERT INTO test_concurrent_move SELECT toDate('2023-01-01') + toIntervalDay(number) + rand(), {i} from system.numbers limit 20"
        )
        node1.query(
            f"INSERT INTO test_concurrent_move SELECT toDate('2023-01-01') + toIntervalDay(number) + rand(), {i} from system.numbers limit 20"
        )
        node1.query(
            f"INSERT INTO test_concurrent_move SELECT toDate('2023-01-01') + toIntervalDay(number) + rand(), {i} from system.numbers limit 20"
        )

    node2.query("SYSTEM SYNC REPLICA test_concurrent_move")

    # check that we can move parts concurrently without exceptions
    p = Pool(3)
    for i in partitions:

        def move_partition_to_s3(node):
            node.query(
                f"ALTER TABLE test_concurrent_move MOVE PARTITION '{i}' TO DISK 's3'"
            )

        j1 = p.apply_async(move_partition_to_s3, (node1,))
        j2 = p.apply_async(move_partition_to_s3, (node2,))
        j1.get()
        j2.get()

    def get_part_to_disk(query_result):
        part_to_disk = {}
        for row in query_result.strip().split("\n"):
            disk, part = row.split("\t")
            part_to_disk[part] = disk
        return part_to_disk

    part_to_disk = get_part_to_disk(
        node1.query(
            "SELECT disk_name, name FROM system.parts where table = 'test_concurrent_move' and active"
        )
    )

    assert all([value == "s3" for value in part_to_disk.values()])

    part_to_disk = get_part_to_disk(
        node2.query(
            "SELECT disk_name, name FROM system.parts where table = 'test_concurrent_move' and active"
        )
    )
    assert all([value == "s3" for value in part_to_disk.values()])


def test_zero_copy_mutation(started_cluster):
    node1 = cluster.instances["node1"]
    node2 = cluster.instances["node2"]

    node1.query(
        """
CREATE TABLE test_zero_copy_mutation (EventDate Date, CounterID UInt32)
ENGINE = ReplicatedMergeTree('/clickhouse-tables/test_zero_copy_mutation', 'r1')
ORDER BY (CounterID, EventDate)
SETTINGS index_granularity = 8192, storage_policy = 's3_only'"""
    )

    node2.query(
        """
CREATE TABLE test_zero_copy_mutation (EventDate Date, CounterID UInt32)
ENGINE = ReplicatedMergeTree('/clickhouse-tables/test_zero_copy_mutation', 'r2')
ORDER BY (CounterID, EventDate)
SETTINGS index_granularity = 8192, storage_policy = 's3_only'"""
    )

    node1.query(
        "INSERT INTO test_zero_copy_mutation SELECT toDate('2023-01-01') + toIntervalDay(number) + rand(), number * number from system.numbers limit 10"
    )

    node2.query("SYSTEM STOP REPLICATION QUEUES test_zero_copy_mutation")
    p = Pool(3)

    def run_long_mutation(node):
        node1.query(
            "ALTER TABLE test_zero_copy_mutation DELETE WHERE sleepEachRow(1) == 1"
        )

    job = p.apply_async(run_long_mutation, (node1,))

    for i in range(30):
        count = node1.query(
            "SELECT count() FROM system.replication_queue WHERE type = 'MUTATE_PART'"
        ).strip()
        if int(count) > 0:
            break
        else:
            time.sleep(0.1)

    node2.query("SYSTEM START REPLICATION QUEUES test_zero_copy_mutation")

    node2.query("SYSTEM SYNC REPLICA test_zero_copy_mutation")

    job.get()

    assert node2.contains_in_log("all_0_0_0_1/part_exclusive_lock exists")
    assert node2.contains_in_log("Removing zero-copy lock on")
    assert node2.contains_in_log("all_0_0_0_1/part_exclusive_lock doesn't exist")
