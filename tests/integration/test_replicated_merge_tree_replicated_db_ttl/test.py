#!/usr/bin/env python3

import logging
import random
import string
import time
from multiprocessing.dummy import Pool

import minio
import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.add_instance(
            "node1",
            main_configs=["configs/node1_macro.xml"],
            user_configs=[
                "configs/enable_parallel_replicas.xml",
            ],
            with_minio=True,
            with_zookeeper=True,
        )
        cluster.start()

        yield cluster
    finally:
        cluster.shutdown()


def test_replicated_db_and_ttl(started_cluster):
    node1 = cluster.instances["node1"]
    node1.query("DROP DATABASE default")
    node1.query("CREATE DATABASE default ENGINE Replicated('/replicated')")

    node1.query(
        "CREATE TABLE 02908_main (a UInt32) ENGINE = ReplicatedMergeTree ORDER BY a"
    )
    node1.query(
        "CREATE TABLE 02908_dependent (a UInt32, ts DateTime) ENGINE = ReplicatedMergeTree ORDER BY a TTL ts + 1 WHERE a IN (SELECT a FROM 02908_main)"
    )

    node1.query("INSERT INTO 02908_main VALUES (1)")
    node1.query("INSERT INTO 02908_dependent VALUES (1, now())")

    node1.query("SELECT * FROM 02908_dependent")
