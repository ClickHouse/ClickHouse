#!/usr/bin/env python3


import pytest
import time
from helpers.cluster import ClickHouseCluster
from helpers.network import PartitionManager
from helpers.test_tools import assert_eq_with_retry
import random
import string
import json

cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance(
    "node1", main_configs=["configs/cluster.xml"], with_zookeeper=True
)
node2 = cluster.add_instance(
    "node2", main_configs=["configs/cluster.xml"], with_zookeeper=True
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        yield cluster

    finally:
        cluster.shutdown()


def test_system_start_stop_listen_queries(started_cluster):
    node1.query("SYSTEM STOP LISTEN QUERIES")

    assert "Connection refused" in node1.query_and_get_error("SELECT 1", timeout=5)

    node2.query("SYSTEM START LISTEN ON CLUSTER default QUERIES")

    node1.query("SELECT 1")
