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
        "configs/logs_config.xml",
        "configs/config.d/local_disks.xml",
        "configs/config.d/cluster.xml",
    ],
    with_zookeeper=False,
    stay_alive=True,
    tmpfs=["/tmp/data1:size=40M", "/tmp/data2:size=40M"],
    macros={"shard": 0},
)



@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


def test_attach_part_from_different_disk(start_cluster):
    try:

        node1.query(
            """
            DROP TABLE IF EXISTS table_01;
            """
        )

        node1.query(
            """
            DROP TABLE IF EXISTS table_02;
            """
        )

        node1.query(
            """
            CREATE TABLE table_01 (
                date Date,
                n Int32
            ) ENGINE = MergeTree()
            PARTITION BY date
            ORDER BY date;
            """
        )

        node1.query(
            """
            CREATE TABLE table_02 (
                date Date,
                n Int32
            ) ENGINE = MergeTree()
            PARTITION BY date
            ORDER BY date
            Settings storage_policy='local1';
            """
        )

        node1.query(
            """
            CREATE TABLE table_03 (
                date Date,
                n Int32
            ) ENGINE = MergeTree()
            PARTITION BY date
            ORDER BY date
            Settings storage_policy='local2';
            """
        )

        node1.query(
            """
            INSERT INTO table_01 SELECT toDate('2019-10-01'), number FROM system.numbers LIMIT 1000;
            """
        )

        assert node1.query(
            """
            SELECT COUNT() FROM table_01;
            """
        ) == "1000\n"

        node1.query(
            """
            ALTER TABLE table_02 ATTACH PARTITION '2019-10-01' FROM table_01;
            """
        )

        assert node1.query(
            """
            SELECT COUNT() FROM table_02;
            """
        ) == "1000\n"

        node1.query(
            """
            ALTER TABLE table_03 ATTACH PARTITION '2019-10-01' FROM table_02;
            """
        )

        assert node1.query(
            """
            SELECT COUNT() FROM table_03;
            """
        ) == "1000\n"

    finally:
        node1.query("""
        DROP TABLE IF EXISTS table_01;
        """)

        node1.query("""
        DROP TABLE IF EXISTS table_02;
        """)

        node1.query("""
        DROP TABLE IF EXISTS table_03;
        """)