#!/usr/bin/env python3
import time

import pytest

# FIXME This test is too flaky
# https://github.com/ClickHouse/ClickHouse/issues/45887

pytestmark = pytest.mark.skip

from helpers.cluster import ClickHouseCluster


single_node_cluster = ClickHouseCluster(__file__)
small_node = single_node_cluster.add_instance(
    "small_node", main_configs=["configs/s3.xml"], with_minio=True
)


@pytest.fixture(scope="module")
def started_single_node_cluster():
    try:
        single_node_cluster.start()

        yield single_node_cluster
    finally:
        single_node_cluster.shutdown()


def test_move_and_s3_memory_usage(started_single_node_cluster):
    if small_node.is_built_with_sanitizer() or small_node.is_debug_build():
        pytest.skip("Disabled for debug and sanitizers. Too slow.")

    small_node.query(
        "CREATE TABLE s3_test_with_ttl (x UInt32, a String codec(NONE), b String codec(NONE), c String codec(NONE), d String codec(NONE), e String codec(NONE)) engine = MergeTree order by x partition by x SETTINGS storage_policy='s3_and_default'"
    )

    for _ in range(10):
        small_node.query(
            "insert into s3_test_with_ttl select 0, repeat('a', 100), repeat('b', 100), repeat('c', 100), repeat('d', 100), repeat('e', 100) from zeros(400000) settings max_block_size = 8192, max_insert_block_size=10000000, min_insert_block_size_rows=10000000"
        )

    # After this, we should have 5 columns per 10 * 100 * 400000 ~ 400 MB; total ~2G data in partition
    small_node.query(
        "optimize table s3_test_with_ttl final",
        settings={
            "send_logs_level": "error",
            "allow_prefetched_read_pool_for_remote_filesystem": 0,
        },
    )

    small_node.query("system flush logs")
    # Will take memory usage from metric_log.
    # It is easier then specifying total memory limit (insert queries can hit this limit).
    small_node.query("truncate table system.metric_log")

    small_node.query(
        "alter table s3_test_with_ttl move partition 0 to volume 'external'",
        settings={
            "send_logs_level": "error",
            "allow_prefetched_read_pool_for_remote_filesystem": 0,
        },
    )
    small_node.query("system flush logs")
    max_usage = small_node.query(
        """
        select max(m.val - am.val * 4096) from
        (select toStartOfMinute(event_time) as time, max(CurrentMetric_MemoryTracking) as val from system.metric_log group by time) as m join
        (select toStartOfMinute(event_time) as time, min(value) as val from system.asynchronous_metric_log where metric='jemalloc.arenas.all.pdirty' group by time) as am using time;"""
    )
    # 3G limit is a big one. However, we can hit it anyway with parallel s3 writes enabled.
    # Also actual value can be bigger because of memory drift.
    # Increase it a little bit if test fails.
    assert int(max_usage) < 3e9
    res = small_node.query(
        "select * from system.errors where last_error_message like '%Memory limit%' limit 1",
        settings={
            "allow_prefetched_read_pool_for_remote_filesystem": 0,
        },
    )
    assert res == ""
