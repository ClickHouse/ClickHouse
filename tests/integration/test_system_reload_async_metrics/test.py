import os
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
    node.query("DROP TABLE IF EXISTS tab SYNC;")

    # Reload asynchronous metrics to get the latest state (async metrics update period has been set to a large value
    # intentionally in the test config to test the manual reload functionality)
    node.query("SYSTEM RELOAD ASYNCHRONOUS METRICS")

    # Get table count before creating new table
    res1 = node.query(
        "SELECT value FROM system.asynchronous_metrics WHERE metric = 'NumberOfTables'"
    )

    # Create table and test that SYSTEM RELOAD ASYNCHRONOUS METRICS reflects the change
    node.query("CREATE TABLE tab (col UInt64) ENGINE MergeTree ORDER BY tuple()")

    node.query("SYSTEM RELOAD ASYNCHRONOUS METRICS")

    res2 = node.query(
        "SELECT value FROM system.asynchronous_metrics WHERE metric = 'NumberOfTables'"
    )

    assert int(res2.strip()) == int(res1.strip()) + 1
