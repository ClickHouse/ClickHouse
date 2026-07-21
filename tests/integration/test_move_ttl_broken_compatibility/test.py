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
            image="clickhouse/clickhouse-server",
            with_minio=True,
            tag="24.1",
            stay_alive=True,
            with_installed_binary=True,
        )
        cluster.start()

        yield cluster
    finally:
        cluster.shutdown()


def test_bc_compatibility(started_cluster):
    node1 = cluster.instances["node1"]
    node1.query(
        """
        CREATE TABLE test_ttl_table (
            generation UInt64,
            date_key DateTime,
            number UInt64,
            text String,
            expired DateTime DEFAULT now()
        )
        ENGINE=MergeTree
        ORDER BY (generation, date_key)
        PARTITION BY toMonth(date_key)
        TTL expired + INTERVAL 20 SECONDS TO DISK 's3'
        SETTINGS storage_policy = 's3';
    """
    )

    node1.query(
        """
        INSERT INTO test_ttl_table (
            generation,
            date_key,
            number,
            text
        )
        SELECT
            1,
            toDateTime('2000-01-01 00:00:00') + rand(number) % 365 * 86400,
            number,
            toString(number)
        FROM numbers(10000);
    """
    )

    disks = (
        node1.query(
            """
        SELECT distinct disk_name
        FROM system.parts
        WHERE table = 'test_ttl_table'
    """
        )
        .strip()
        .split("\n")
    )
    print("Disks before", disks)

    assert len(disks) == 1
    assert disks[0] == "default"

    node1.restart_with_latest_version()

    for _ in range(60):
        disks = (
            node1.query(
                """
            SELECT distinct disk_name
            FROM system.parts
            WHERE table = 'test_ttl_table'
        """
            )
            .strip()
            .split("\n")
        )
        print("Disks after", disks)
        if "s3" in disks:
            break
        time.sleep(1)
    assert "s3" in disks
