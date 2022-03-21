import json
import random
import re
import string
import threading
import time
from multiprocessing.dummy import Pool

import pytest
from helpers.client import QueryRuntimeException
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance(
    "node1",
    main_configs=["configs/config.d/storage_configuration.xml",],
    with_zookeeper=True,
    stay_alive=True,
    tmpfs=["/jbod1:size=100M", "/jbod2:size=100M", "/jbod3:size=100M"],
    macros={"shard": 0, "replica": 1},
)


node2 = cluster.add_instance(
    "node2",
    main_configs=["configs/config.d/storage_configuration.xml"],
    with_zookeeper=True,
    stay_alive=True,
    tmpfs=["/jbod1:size=100M", "/jbod2:size=100M", "/jbod3:size=100M"],
    macros={"shard": 0, "replica": 2},
)


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


def check_balance(node, table):

    partitions = node.query(
        """
        WITH
            arraySort(groupArray(c)) AS array_c,
            arrayEnumerate(array_c) AS array_i,
            sum(c) AS sum_c,
            count() AS n,
            if(sum_c = 0, 0, (((2. * arraySum(arrayMap((c, i) -> (c * i), array_c, array_i))) / n) / sum_c) - (((n + 1) * 1.) / n)) AS gini
        SELECT
            partition
        FROM
        (
            SELECT
                partition,
                disk_name,
                sum(bytes_on_disk) AS c
            FROM system.parts
            WHERE active AND level > 0 AND disk_name like 'jbod%' AND table = '{}'
            GROUP BY
                partition, disk_name
        )
        GROUP BY partition
        HAVING gini < 0.1
        """.format(
            table
        )
    ).splitlines()

    assert set(partitions) == set(["0", "1"])


def test_jbod_balanced_merge(start_cluster):
    try:
        node1.query(
            """
            CREATE TABLE tbl (p UInt8, d String)
            ENGINE = MergeTree
            PARTITION BY p
            ORDER BY tuple()
            SETTINGS
                storage_policy = 'jbod',
                min_bytes_to_rebalance_partition_over_jbod = 1024,
                max_bytes_to_merge_at_max_space_in_pool = 4096
        """
        )
        node1.query("create table tmp1 as tbl")
        node1.query("create table tmp2 as tbl")

        p = Pool(20)

        def task(i):
            print("Processing insert {}/{}".format(i, 200))
            # around 1k per block
            node1.query(
                "insert into tbl select randConstant() % 2, randomPrintableASCII(16) from numbers(50)"
            )
            node1.query(
                "insert into tmp1 select randConstant() % 2, randomPrintableASCII(16) from numbers(50)"
            )
            node1.query(
                "insert into tmp2 select randConstant() % 2, randomPrintableASCII(16) from numbers(50)"
            )

        p.map(task, range(200))

        time.sleep(1)

        check_balance(node1, "tbl")

    finally:
        node1.query(f"DROP TABLE IF EXISTS tbl SYNC")
        node1.query(f"DROP TABLE IF EXISTS tmp1 SYNC")
        node1.query(f"DROP TABLE IF EXISTS tmp2 SYNC")


def test_replicated_balanced_merge_fetch(start_cluster):
    try:
        for i, node in enumerate([node1, node2]):
            node.query(
                """
                CREATE TABLE tbl (p UInt8, d String)
                ENGINE = ReplicatedMergeTree('/clickhouse/tbl', '{}')
                PARTITION BY p
                ORDER BY tuple()
                SETTINGS
                    storage_policy = 'jbod',
                    old_parts_lifetime = 1,
                    cleanup_delay_period = 1,
                    cleanup_delay_period_random_add = 2,
                    min_bytes_to_rebalance_partition_over_jbod = 1024,
                    max_bytes_to_merge_at_max_space_in_pool = 4096
            """.format(
                    i
                )
            )

            node.query(
                """
                CREATE TABLE tmp1 (p UInt8, d String)
                ENGINE = MergeTree
                PARTITION BY p
                ORDER BY tuple()
                SETTINGS
                    storage_policy = 'jbod',
                    min_bytes_to_rebalance_partition_over_jbod = 1024,
                    max_bytes_to_merge_at_max_space_in_pool = 4096
            """
            )

            node.query("create table tmp2 as tmp1")

        node2.query("alter table tbl modify setting always_fetch_merged_part = 1")
        p = Pool(20)

        def task(i):
            print("Processing insert {}/{}".format(i, 200))
            # around 1k per block
            node1.query(
                "insert into tbl select randConstant() % 2, randomPrintableASCII(16) from numbers(50)"
            )
            node1.query(
                "insert into tmp1 select randConstant() % 2, randomPrintableASCII(16) from numbers(50)"
            )
            node1.query(
                "insert into tmp2 select randConstant() % 2, randomPrintableASCII(16) from numbers(50)"
            )
            node2.query(
                "insert into tmp1 select randConstant() % 2, randomPrintableASCII(16) from numbers(50)"
            )
            node2.query(
                "insert into tmp2 select randConstant() % 2, randomPrintableASCII(16) from numbers(50)"
            )

        p.map(task, range(200))

        node2.query("SYSTEM SYNC REPLICA tbl", timeout=10)

        check_balance(node1, "tbl")
        check_balance(node2, "tbl")

    finally:
        for node in [node1, node2]:
            node.query("DROP TABLE IF EXISTS tbl SYNC")
            node.query("DROP TABLE IF EXISTS tmp1 SYNC")
            node.query("DROP TABLE IF EXISTS tmp2 SYNC")
