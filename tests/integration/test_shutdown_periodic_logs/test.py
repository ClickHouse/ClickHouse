#!/usr/bin/env python3

import time

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node = cluster.add_instance(
    "node", main_configs=["configs/periodic_logs.xml"], stay_alive=True
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_shutdown_does_not_wait_for_periodic_log_collection(started_cluster):
    """The collectors of the periodic system logs, configured with a one-hour
    `collect_interval_milliseconds`, must not delay a graceful shutdown."""
    node.query("SELECT 1")

    start = time.time()
    assert node.stop_clickhouse(stop_wait_sec=90) is True
    elapsed = time.time() - start
    assert elapsed < 90, f"Shutdown took {elapsed} seconds"

    node.start_clickhouse()
    assert node.query("SELECT 1") == "1\n"
