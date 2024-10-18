#!/usr/bin/env python3

import logging
import os
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
            with_minio=True,
            with_zookeeper=True,
        )
        cluster.start()

        yield cluster
    finally:
        cluster.shutdown()


def test_drop_detached_part(started_cluster):
    node1 = cluster.instances["node1"]

    node1.query(
        """
CREATE TABLE test1 (EventDate Date, CounterID UInt32)
ENGINE = ReplicatedMergeTree('/clickhouse-tables/test1', 'r1')
ORDER BY (CounterID, EventDate)"""
    )

    node1.query(
        "INSERT INTO test1 SELECT toDate('2023-01-01') + toIntervalDay(number), number + 1000 from system.numbers limit 20"
    )
    node1.query("ALTER TABLE test1 DETACH PART 'all_0_0_0'")

    def get_path_to_detached_part(query_result):
        part_to_disk = {}
        for row in query_result.strip().split("\n"):
            print(row)
            return row

    path_to_detached_part = get_path_to_detached_part(
        node1.query("SELECT path FROM system.detached_parts where table = 'test1'")
    )

    new_part_name = "ignored_" + os.path.basename(path_to_detached_part)
    new_path_to_detached_part_name = (
        os.path.dirname(path_to_detached_part) + os.sep + new_part_name
    )

    node1.exec_in_container(
        [
            "bash",
            "-c",
            f"mv {path_to_detached_part} {new_path_to_detached_part_name}",
        ],
        privileged=True,
        user="root",
    )

    assert (
        node1.query(
            "SELECT path FROM system.detached_parts where table = 'test1'"
        ).strip()
        == new_path_to_detached_part_name
    )

    node1.query(
        f"ALTER TABLE test1 DROP DETACHED PART '{new_part_name}'",
        settings={"allow_drop_detached": 1},
    )

    assert (
        node1.query(
            "SELECT path FROM system.detached_parts where table = 'test1'"
        ).strip()
        == ""
    )
