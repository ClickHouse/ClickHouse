#!/usr/bin/env python3
import random
import statistics
import string
import subprocess
import time

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.network import NetThroughput

cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance("node1", with_zookeeper=True)
node2 = cluster.add_instance("node2", with_zookeeper=True)
node3 = cluster.add_instance(
    "node3", main_configs=["configs/limit_replication_config.xml"], with_zookeeper=True
)


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


def test_limited_fetch_single_table(start_cluster):
    print("Limited fetches single table")
    try:
        for i, node in enumerate([node1, node2]):
            node.query(
                f"CREATE TABLE limited_fetch_table(key UInt64, data String) ENGINE = ReplicatedMergeTree('/clickhouse/tables/limited_fetch_table', '{i}') ORDER BY tuple() PARTITION BY key SETTINGS max_replicated_fetches_network_bandwidth=10485760"
            )

        node2.query("SYSTEM STOP FETCHES limited_fetch_table")

        for i in range(5):
            node1.query(
                "INSERT INTO limited_fetch_table SELECT {}, (select randomPrintableASCII(104857)) FROM numbers(300)".format(
                    i
                )
            )

        n1_net = NetThroughput(node1)
        n2_net = NetThroughput(node2)

        node2.query("SYSTEM START FETCHES limited_fetch_table")
        n2_fetch_speed = []
        for i in range(10):
            n1_in, n1_out = n1_net.measure_speed("megabytes")
            n2_in, n2_out = n2_net.measure_speed("megabytes")
            print("[N1] input:", n1_in, "MB/s", "output:", n1_out, "MB/s")
            print("[N2] input:", n2_in, "MB/s", "output:", n2_out, "MB/s")
            n2_fetch_speed.append(n2_in)
            time.sleep(0.5)

        median_speed = statistics.median(n2_fetch_speed)
        # approximate border. Without limit we will have more than 100 MB/s for very slow builds.
        assert median_speed <= 15, (
            "We exceeded max fetch speed for more than 10MB/s. Must be around 10 (+- 5), got "
            + str(median_speed)
        )

    finally:
        for node in [node1, node2]:
            node.query("DROP TABLE IF EXISTS limited_fetch_table SYNC")


def test_limited_send_single_table(start_cluster):
    print("Limited sends single table")
    try:
        for i, node in enumerate([node1, node2]):
            node.query(
                f"CREATE TABLE limited_send_table(key UInt64, data String) ENGINE = ReplicatedMergeTree('/clickhouse/tables/limited_fetch_table', '{i}') ORDER BY tuple() PARTITION BY key SETTINGS max_replicated_sends_network_bandwidth=5242880"
            )

        node2.query("SYSTEM STOP FETCHES limited_send_table")

        for i in range(5):
            node1.query(
                "INSERT INTO limited_send_table SELECT {}, (select randomPrintableASCII(104857)) FROM numbers(150)".format(
                    i
                )
            )

        n1_net = NetThroughput(node1)
        n2_net = NetThroughput(node2)

        node2.query("SYSTEM START FETCHES limited_send_table")
        n1_sends_speed = []
        for i in range(10):
            n1_in, n1_out = n1_net.measure_speed("megabytes")
            n2_in, n2_out = n2_net.measure_speed("megabytes")
            print("[N1] input:", n1_in, "MB/s", "output:", n1_out, "MB/s")
            print("[N2] input:", n2_in, "MB/s", "output:", n2_out, "MB/s")
            n1_sends_speed.append(n1_out)
            time.sleep(0.5)

        median_speed = statistics.median(n1_sends_speed)
        # approximate border. Without limit we will have more than 100 MB/s for very slow builds.
        assert median_speed <= 10, (
            "We exceeded max send speed for more than 5MB/s. Must be around 5 (+- 5), got "
            + str(median_speed)
        )

    finally:
        for node in [node1, node2]:
            node.query("DROP TABLE IF EXISTS limited_send_table SYNC")


def test_limited_fetches_for_server(start_cluster):
    print("Limited fetches for server")
    try:
        for i, node in enumerate([node1, node3]):
            for j in range(5):
                node.query(
                    f"CREATE TABLE limited_fetches{j}(key UInt64, data String) ENGINE = ReplicatedMergeTree('/clickhouse/tables/limited_fetches{j}', '{i}') ORDER BY tuple() PARTITION BY key"
                )

        for j in range(5):
            node3.query(f"SYSTEM STOP FETCHES limited_fetches{j}")
            for i in range(5):
                node1.query(
                    "INSERT INTO limited_fetches{} SELECT {}, (select randomPrintableASCII(104857)) FROM numbers(150)".format(
                        j, i
                    )
                )

        n1_net = NetThroughput(node1)
        n3_net = NetThroughput(node3)

        for j in range(5):
            node3.query(f"SYSTEM START FETCHES limited_fetches{j}")

        n3_fetches_speed = []
        for i in range(5):
            n1_in, n1_out = n1_net.measure_speed("megabytes")
            n3_in, n3_out = n3_net.measure_speed("megabytes")
            print("[N1] input:", n1_in, "MB/s", "output:", n1_out, "MB/s")
            print("[N3] input:", n3_in, "MB/s", "output:", n3_out, "MB/s")
            n3_fetches_speed.append(n3_in)
            time.sleep(0.5)

        median_speed = statistics.median(n3_fetches_speed)
        # approximate border. Without limit we will have more than 100 MB/s for very slow builds.
        assert median_speed <= 15, (
            "We exceeded max fetch speed for more than 15MB/s. Must be around 5 (+- 10), got "
            + str(median_speed)
        )

    finally:
        for node in [node1, node3]:
            for j in range(5):
                node.query(f"DROP TABLE IF EXISTS limited_fetches{j} SYNC")


def test_limited_sends_for_server(start_cluster):
    print("Limited sends for server")
    try:
        for i, node in enumerate([node1, node3]):
            for j in range(5):
                node.query(
                    f"CREATE TABLE limited_sends{j}(key UInt64, data String) ENGINE = ReplicatedMergeTree('/clickhouse/tables/limited_sends{j}', '{i}') ORDER BY tuple() PARTITION BY key"
                )

        for j in range(5):
            node1.query(f"SYSTEM STOP FETCHES limited_sends{j}")
            for i in range(5):
                node3.query(
                    "INSERT INTO limited_sends{} SELECT {}, (select randomPrintableASCII(104857)) FROM numbers(150)".format(
                        j, i
                    )
                )

        n1_net = NetThroughput(node1)
        n3_net = NetThroughput(node3)

        for j in range(5):
            node1.query(f"SYSTEM START FETCHES limited_sends{j}")

        n3_sends_speed = []
        for i in range(5):
            n1_in, n1_out = n1_net.measure_speed("megabytes")
            n3_in, n3_out = n3_net.measure_speed("megabytes")
            print("[N1] input:", n1_in, "MB/s", "output:", n1_out, "MB/s")
            print("[N3] input:", n3_in, "MB/s", "output:", n3_out, "MB/s")
            n3_sends_speed.append(n3_out)
            time.sleep(0.5)

        median_speed = statistics.median(n3_sends_speed)
        # approximate border. Without limit we will have more than 100 MB/s for very slow builds.
        assert median_speed <= 20, (
            "We exceeded max send speed for more than 20MB/s. Must be around 5 (+- 10), got "
            + str(median_speed)
        )

    finally:
        for node in [node1, node3]:
            for j in range(5):
                node.query(f"DROP TABLE IF EXISTS limited_sends{j} SYNC")


def test_should_execute_fetch(start_cluster):
    print("Should execute fetch")
    try:
        for i, node in enumerate([node1, node2]):
            node.query(
                f"CREATE TABLE should_execute_table(key UInt64, data String) ENGINE = ReplicatedMergeTree('/clickhouse/tables/should_execute_table', '{i}') ORDER BY tuple() PARTITION BY key SETTINGS max_replicated_fetches_network_bandwidth=3505253"
            )

        node2.query("SYSTEM STOP FETCHES should_execute_table")

        for i in range(3):
            node1.query(
                "INSERT INTO should_execute_table SELECT {}, (select randomPrintableASCII(104857)) FROM numbers(200)".format(
                    i
                )
            )

        n1_net = NetThroughput(node1)
        n2_net = NetThroughput(node2)

        node2.query("SYSTEM START FETCHES should_execute_table")

        for i in range(10):
            node1.query(
                "INSERT INTO should_execute_table SELECT {}, (select randomPrintableASCII(104857)) FROM numbers(3)".format(
                    i
                )
            )

        n2_fetch_speed = []
        replication_queue_data = []
        for i in range(10):
            n1_in, n1_out = n1_net.measure_speed("megabytes")
            n2_in, n2_out = n2_net.measure_speed("megabytes")
            fetches_count = node2.query("SELECT count() FROM system.replicated_fetches")
            if fetches_count == "0\n":
                break

            print("Fetches count", fetches_count)
            replication_queue_data.append(
                node2.query(
                    "SELECT count() FROM system.replication_queue WHERE postpone_reason like '%fetches have already throttled%'"
                )
            )
            n2_fetch_speed.append(n2_in)
            time.sleep(0.5)

        node2.query("SYSTEM SYNC REPLICA should_execute_table")
        assert any(int(f.strip()) != 0 for f in replication_queue_data)
        assert node2.query("SELECT COUNT() FROM should_execute_table") == "630\n"
    finally:
        for node in [node1, node2]:
            node.query("DROP TABLE IF EXISTS should_execute_table SYNC")
