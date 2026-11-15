#!/usr/bin/env python3

import os
import random
import string
import time

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.network import PartitionManager

cluster = ClickHouseCluster(__file__)
SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
node1 = cluster.add_instance(
    "node1", main_configs=["configs/custom_settings.xml"], with_zookeeper=True
)
node2 = cluster.add_instance(
    "node2", main_configs=["configs/custom_settings.xml"], with_zookeeper=True
)

MAX_THREADS_FOR_FETCH = 3


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        yield cluster

    finally:
        cluster.shutdown()


def get_random_string(length):
    return "".join(
        random.choice(string.ascii_uppercase + string.digits) for _ in range(length)
    )


def test_limited_fetches(started_cluster):
    """
    Test checks that that we utilize all available threads for fetches
    """
    node1.query(
        "CREATE TABLE t (key UInt64, data String) ENGINE = ReplicatedMergeTree('/clickhouse/test/t', '1') ORDER BY tuple() PARTITION BY key"
    )
    node2.query(
        "CREATE TABLE t (key UInt64, data String) ENGINE = ReplicatedMergeTree('/clickhouse/test/t', '2') ORDER BY tuple() PARTITION BY key"
    )

    with PartitionManager() as pm:
        node2.query("SYSTEM STOP FETCHES t")
        node1.query(
            "INSERT INTO t SELECT 1, '{}' FROM numbers(5000)".format(
                get_random_string(104857)
            )
        )
        node1.query(
            "INSERT INTO t SELECT 2, '{}' FROM numbers(5000)".format(
                get_random_string(104857)
            )
        )
        node1.query(
            "INSERT INTO t SELECT 3, '{}' FROM numbers(5000)".format(
                get_random_string(104857)
            )
        )
        node1.query(
            "INSERT INTO t SELECT 4, '{}' FROM numbers(5000)".format(
                get_random_string(104857)
            )
        )
        node1.query(
            "INSERT INTO t SELECT 5, '{}' FROM numbers(5000)".format(
                get_random_string(104857)
            )
        )
        node1.query(
            "INSERT INTO t SELECT 6, '{}' FROM numbers(5000)".format(
                get_random_string(104857)
            )
        )
        pm.add_network_delay(node1, 80)
        node2.query("SYSTEM START FETCHES t")
        fetches_result = []
        background_fetches_metric = []
        fetched_parts = set([])
        for _ in range(1000):
            result = (
                node2.query("SELECT result_part_name FROM system.replicated_fetches")
                .strip()
                .split()
            )
            background_fetches_metric.append(
                int(
                    node2.query(
                        "select value from system.metrics where metric = 'BackgroundFetchesPoolTask'"
                    ).strip()
                )
            )
            if not result:
                if len(fetched_parts) == 6:
                    break
                time.sleep(0.1)
            else:
                for part in result:
                    fetched_parts.add(part)
                fetches_result.append(result)
                print(fetches_result[-1])
                print(background_fetches_metric[-1])
                time.sleep(0.1)

    for concurrently_fetching_parts in fetches_result:
        if len(concurrently_fetching_parts) > MAX_THREADS_FOR_FETCH:
            assert False, "Found more than {} concurrently fetching parts: {}".format(
                MAX_THREADS_FOR_FETCH, ", ".join(concurrently_fetching_parts)
            )

    assert (
        max([len(parts) for parts in fetches_result]) == 3
    ), "Strange, but we don't utilize max concurrent threads for fetches"
    assert (
        max(background_fetches_metric)
    ) == 3, "Just checking metric consistent with table"

    node1.query("DROP TABLE IF EXISTS t SYNC")
    node2.query("DROP TABLE IF EXISTS t SYNC")
