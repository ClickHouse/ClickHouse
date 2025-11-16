import os
import shutil
import time

import pytest

from helpers.cluster import ClickHouseCluster

# Tests that SYSTEM RELOAD ASYNCHRONOUS METRICS works.

# Config default.xml sets a large refresh interval of asynchronous metrics, so that the periodic updates don't interfere with the manual
# update below.
cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=["configs/default.xml"],
    stay_alive=True,
)


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
CONFIG_DIR = os.path.join(SCRIPT_DIR, "configs")


def test_system_reload_async_metrics(start_cluster):
    node.query("SYSTEM DROP QUERY CACHE")

    res1 = node.query(
        "SELECT value FROM system.asynchronous_metrics WHERE metric = 'NumberOfTables'"
    )

    # create table and test that the table creation is reflected in the asynchronous metrics
    node.query("CREATE TABLE tab (col UInt64) ENGINE MergeTree ORDER BY tuple()")

    node.query("SYSTEM RELOAD ASYNCHRONOUS METRICS")

    res2 = node.query(
        "SELECT value FROM system.asynchronous_metrics WHERE metric = 'NumberOfTables'"
    )
    assert int(res1.rstrip()) + 1 == int(res2.rstrip())
