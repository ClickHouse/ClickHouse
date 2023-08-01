#!/usr/bin/env python3
from helpers.cluster import ClickHouseCluster
import pytest
import random
import string
from helpers.network import NetThroughput
import subprocess
import time
import statistics

cluster = ClickHouseCluster(__file__)
apac1 = cluster.add_instance("apac1", main_configs=["configs/apac1.xml"], with_zookeeper=True)
apac2 = cluster.add_instance("apac2", main_configs=["configs/apac2.xml"], with_zookeeper=True)
us3 = cluster.add_instance("us3", main_configs=["configs/us3.xml"], with_zookeeper=True)


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()

        yield cluster
    finally:
        cluster.shutdown()


def get_random_string(length):
    return "".join(
        random.choice(string.ascii_uppercase + string.digits) for _ in range(length)
    )


def test_follower_only_fetch_from_leader(start_cluster):
    try:
        for i, node in enumerate([apac1, apac2, us3]): #apac1 will become leader of APAC
            node.query(
                f"CREATE TABLE us_table(key UInt64, data String) ENGINE = ReplicatedMergeTree('/clickhouse/tables/us_table', '{i}') ORDER BY tuple() PARTITION BY key"
            )
            time.sleep(1)

        apac1.query("SYSTEM STOP FETCHES us_table")
        apac2.query("SYSTEM STOP FETCHES us_table")

        for i in range(5):
            us3.query(
                "INSERT INTO us_table SELECT {}, (select randomPrintableASCII(104857)) FROM numbers(300)".format(
                    i
                )
            )
        time.sleep(1)
        apac1_net = NetThroughput(apac1)
        apac2_net = NetThroughput(apac2)

        apac2.query("SYSTEM START FETCHES us_table")
        apac2_fetch_speed = []
        for i in range(10):
            apac1_in, apac1_out = apac1_net.measure_speed("megabytes")
            apac2_in, apac2_out = apac2_net.measure_speed("megabytes")
            print("[N1] input:", apac1_in, "MB/s", "output:", apac1_out, "MB/s")
            print("[N2] input:", apac2_in, "MB/s", "output:", apac2_out, "MB/s")
            apac2_fetch_speed.append(apac2_in)
            time.sleep(0.5)

        max_fetch_speed = max(apac2_fetch_speed)
        # apac2 should have almost no traffic because leader apac1 doesn't fetch
        assert max_fetch_speed < 0.5, (
            "Follower shouldn't have any traffic because leader stopped fetching, but we got "
            + str(max_fetch_speed)
        )

        # stop fetch from apac2 and start fetching from apac1
        apac2.query("SYSTEM STOP FETCHES us_table")
        apac1.query("SYSTEM START FETCHES us_table")
        apac1.query("SYSTEM SYNC REPLICA us_table LIGHTWEIGHT")

        # restart apac1, so apac2 should becomes leader and will start fetching from apac1
        cluster.restart_instance(apac1)

        us3_net = NetThroughput(us3)
        apac2_fetch_speed = []
        us3_send_speed = []
        apac2.query("SYSTEM START FETCHES us_table")
        for i in range(10):
            us3_in, us3_out = us3_net.measure_speed("megabytes")
            apac2_in, apac2_out = apac2_net.measure_speed("megabytes")
            print("[N1] input:", us3_in, "MB/s", "output:", us3_out, "MB/s")
            print("[N2] input:", apac2_in, "MB/s", "output:", apac2_out, "MB/s")
            apac2_fetch_speed.append(apac2_in)
            us3_send_speed.append(us3_out);
            time.sleep(0.5)

        max_fetch_speed = max(apac2_fetch_speed)
        # apac2 should have high traffic now
        assert max_fetch_speed > 100, (
            "Follower should become leader and have high traffic for fetching, but we got "
            + str(max_fetch_speed)
        )

        # apac2 should fetch everything from apac1, not us3
        max_send_speed = max(us3_send_speed)
        assert max_send_speed < 0.5, (
            "Replica should prefer fetching from node in same region, but out of region node has traffic "
            + str(max_send_speed)
        )

    finally:
        for node in [apac1, apac2, us3]:
            node.query("DROP TABLE IF EXISTS us_table SYNC")
