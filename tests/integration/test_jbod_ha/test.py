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
    main_configs=[
        "configs/config.d/storage_configuration.xml",
    ],
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


def test_jbod_ha(start_cluster):
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
                    max_bytes_to_merge_at_max_space_in_pool = 4096
            """.format(
                    i
                )
            )

        for i in range(50):
            # around 1k per block
            node1.query(
                "insert into tbl select randConstant() % 2, randomPrintableASCII(16) from numbers(50)"
            )

        node2.query("SYSTEM SYNC REPLICA tbl", timeout=10)

        # mimic disk failure
        node1.exec_in_container(
            ["bash", "-c", "chmod -R 000 /jbod1"], privileged=True, user="root"
        )

        time.sleep(3)

        # after 3 seconds jbod1 will be set as broken disk. Let's wait for another 5 seconds for data to be recovered
        time.sleep(5)

        assert (
            int(
                node1.query("select total_space from system.disks where name = 'jbod1'")
            )
            == 0
        )

        assert int(node1.query("select count(p) from tbl")) == 2500

        # mimic disk recovery
        node1.exec_in_container(
            ["bash", "-c", "chmod -R 755 /jbod1"],
            privileged=True,
            user="root",
        )
        node1.query("system restart disk jbod1")

        time.sleep(5)

        assert (
            int(
                node1.query("select total_space from system.disks where name = 'jbod1'")
            )
            > 0
        )

    finally:
        for node in [node1, node2]:
            node.query("DROP TABLE IF EXISTS tbl SYNC")
