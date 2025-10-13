import time

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance(
    "node1",
    main_configs=["configs/asynchronous_metrics_update_period_s.xml"],
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


# asynchronous metrics are updated once every 60s by default. To make the test run faster, the setting
# asynchronous_metric_update_period_s is being set to 1s so that the metrics are populated faster and
# are available for querying during the test.
def test_asynchronous_metric_jemalloc_profile_active(started_cluster):
    # default open
    if node1.is_built_with_sanitizer():
        pytest.skip("Disabled for sanitizers")

    res = node1.query(
        "SELECT * FROM system.asynchronous_metrics WHERE metric ILIKE '%jemalloc.prof.active%' FORMAT Vertical;"
    )
    assert (
        res
        == """Row 1:
──────
metric:      jemalloc.prof.active
value:       0
description: An internal metric of the low-level memory allocator (jemalloc). See https://jemalloc.net/jemalloc.3.html
"""
    )
    # enable
    node1.query("SYSTEM JEMALLOC ENABLE PROFILE")
    node1.query("SYSTEM RELOAD ASYNCHRONOUS METRICS")
    res = node1.query(
        "SELECT * FROM system.asynchronous_metrics WHERE metric ILIKE '%jemalloc.prof.active%' FORMAT Vertical;"
    )
    assert (
        res
        == """Row 1:
──────
metric:      jemalloc.prof.active
value:       1
description: An internal metric of the low-level memory allocator (jemalloc). See https://jemalloc.net/jemalloc.3.html
"""
    )
    # disable
    node1.query("SYSTEM JEMALLOC DISABLE PROFILE")
    node1.query("SYSTEM RELOAD ASYNCHRONOUS METRICS")
    res = node1.query(
        "SELECT * FROM system.asynchronous_metrics WHERE metric ILIKE '%jemalloc.prof.active%' FORMAT Vertical;"
    )
    assert (
        res
        == """Row 1:
──────
metric:      jemalloc.prof.active
value:       0
description: An internal metric of the low-level memory allocator (jemalloc). See https://jemalloc.net/jemalloc.3.html
"""
    )
