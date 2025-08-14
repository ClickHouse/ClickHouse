import time

import pytest

from helpers.cluster import ClickHouseCluster

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


def test_jemalloc_global_profiler(started_cluster):
    # default open
    if node1.is_built_with_sanitizer():
        pytest.skip("Disabled for sanitizers")

    def is_global_profiler_active():
        return int(
            node1.query(
                "SELECT value FROM system.asynchronous_metrics WHERE metric = 'jemalloc.prof.thread_active_init'"
            ).strip()
        )

    def has_jemalloc_sample_in_trace_log():
        node1.query("SYSTEM FLUSH LOGS")
        return int(
            node1.query(
                "SELECT count() > 0 FROM system.trace_log WHERE trace_type = 'JemallocSample'"
            ).strip()
        )

    assert is_global_profiler_active() == 0
    assert has_jemalloc_sample_in_trace_log() == 0

    node1.replace_in_config(
        "/etc/clickhouse-server/config.d/jemalloc_global_profiler.xml",
        "jemalloc_enable_global_profiler>.",
        "jemalloc_enable_global_profiler>1",
    )
    node1.restart_clickhouse()

    assert is_global_profiler_active() == 1
    assert has_jemalloc_sample_in_trace_log() == 0

    node1.replace_in_config(
        "/etc/clickhouse-server/config.d/jemalloc_global_profiler.xml",
        "jemalloc_collect_global_profile_samples_in_trace_log>.",
        "jemalloc_collect_global_profile_samples_in_trace_log>1",
    )
    node1.restart_clickhouse()

    assert is_global_profiler_active() == 1
    assert has_jemalloc_sample_in_trace_log() == 1
