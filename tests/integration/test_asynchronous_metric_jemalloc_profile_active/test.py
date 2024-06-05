import time

import pytest
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance(
    "node1",
    main_configs=["configs/asynchronous_metrics_update_period_s.xml"],
    env_variables={"MALLOC_CONF": "background_thread:true,prof:true"},
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


# Tests that the system.asynchronous_metric_log table gets populated.
# asynchronous metrics are updated once every 60s by default. To make the test run faster, the setting
# asynchronous_metric_update_period_s is being set to 2s so that the metrics are populated faster and
# are available for querying during the test.
def test_event_time_microseconds_field(started_cluster):
    res_t = node1.query("SYSTEM JEMALLOC ENABLE PROFILE")
    res_o = node1.query("SELECT * FROM system.asynchronous_metrics WHERE metric ILIKE '%jemalloc.prof.active%' FORMAT Vertical;")
    assert (
        res_o== """Row 1:
──────
metric:      jemalloc.prof.active
value:       1
description: An internal metric of the low-level memory allocator (jemalloc). See https://jemalloc.net/jemalloc.3.html
"""
    )
    node1.query("SYSTEM JEMALLOC DISABLE PROFILE")
    time.sleep(5)
    res_t = node1.query("SELECT * FROM system.asynchronous_metrics WHERE metric ILIKE '%jemalloc.prof.active%' FORMAT Vertical;")
    assert (
        res_t== """Row 1:
──────
metric:      jemalloc.prof.active
value:       0
description: An internal metric of the low-level memory allocator (jemalloc). See https://jemalloc.net/jemalloc.3.html
"""
    )
