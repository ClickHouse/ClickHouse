import time

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.client import QueryRuntimeException

cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance(
    "node1", main_configs=["configs/jemalloc_global_profiler.xml"], stay_alive=True
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


def set_config(config, value):
    node1.replace_in_config(
        "/etc/clickhouse-server/config.d/jemalloc_global_profiler.xml",
        f"{config}>.",
        f"{config}>{value}",
    )


def is_global_profiler_active():
    return int(
        node1.query(
            "SELECT value FROM system.asynchronous_metrics WHERE metric = 'jemalloc.prof.thread_active_init'"
        ).strip()
    )


def has_jemalloc_sample_in_trace_log(extra_query_condition=""):
    node1.query("SYSTEM FLUSH LOGS")
    return int(
        node1.query(
            f"SELECT count() > 0 FROM system.trace_log WHERE trace_type = 'JemallocSample' {extra_query_condition}"
        ).strip()
    )


def test_jemalloc_global_profiler(started_cluster):
    # default open
    if node1.is_built_with_sanitizer():
        pytest.skip("Disabled for sanitizers")

    set_config("jemalloc_enable_global_profiler", "0")
    set_config("jemalloc_collect_global_profile_samples_in_trace_log", "0")
    node1.restart_clickhouse()

    try:
        node1.query("TRUNCATE system.trace_log")
    except QueryRuntimeException as e:
        assert e.returncode == 60  # UNKNOWN_TABLE

    assert is_global_profiler_active() == 0
    assert has_jemalloc_sample_in_trace_log() == 0

    set_config("jemalloc_enable_global_profiler", "1")
    node1.restart_clickhouse()

    assert is_global_profiler_active() == 1
    assert has_jemalloc_sample_in_trace_log() == 0

    # we enable collecting samples to trace_log for specific query only
    profiled_query_id = "test_jemalloc_query_profiler"
    node1.query(
        "SELECT * FROM numbers(1000000) ORDER BY number",
        settings={
            "max_bytes_ratio_before_external_sort": 0,
            "jemalloc_enable_profiler": 0,
            "jemalloc_collect_profile_samples_in_trace_log": 1,
        },
        query_id=profiled_query_id,
    )

    def assert_only_traces_from_query(query_id):
        assert (
            has_jemalloc_sample_in_trace_log(
                extra_query_condition=f"AND query_id = '{query_id}'"
            )
            == 1
        )
        # we can deallocate outside query some allocations we did in the query
        assert (
            has_jemalloc_sample_in_trace_log(
                extra_query_condition=f"AND query_id != '{query_id}' AND size > 0"
            )
            == 0
        )

    assert_only_traces_from_query(profiled_query_id)

    set_config("jemalloc_collect_global_profile_samples_in_trace_log", "1")
    node1.restart_clickhouse()

    assert is_global_profiler_active() == 1
    assert has_jemalloc_sample_in_trace_log() == 1

    set_config("jemalloc_enable_global_profiler", "0")
    node1.restart_clickhouse()
    assert is_global_profiler_active() == 0

    node1.query("TRUNCATE system.trace_log")

    # we enable jemalloc profiler for query but rely on global config to collect samples to trace_log
    profiled_query_id = "test_jemalloc_query_profiler"
    node1.query(
        "SELECT * FROM numbers(1000000) ORDER BY number",
        settings={
            "max_bytes_ratio_before_external_sort": 0,
            "jemalloc_enable_profiler": 1,
            "jemalloc_collect_profile_samples_in_trace_log": 0,
        },
        query_id=profiled_query_id,
    )

    assert_only_traces_from_query(profiled_query_id)
