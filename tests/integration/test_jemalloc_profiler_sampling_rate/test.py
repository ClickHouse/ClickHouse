import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=["configs/jemalloc_profiler_sampling_rate.xml"],
    stay_alive=True,
    env_variables={"MALLOC_CONF": "prof:true"},
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def get_lg_prof_sample():
    node.query("SYSTEM RELOAD ASYNCHRONOUS METRICS")
    return int(
        node.query(
            "SELECT value FROM system.asynchronous_metrics WHERE metric = 'jemalloc.prof.lg_sample'"
        ).strip()
    )


def set_config(config, value):
    node.replace_in_config(
        "/etc/clickhouse-server/config.d/jemalloc_profiler_sampling_rate.xml",
        f"{config}>.*<",
        f"{config}>{value}<",
    )


def test_jemalloc_profiler_sampling_rate(started_cluster):
    if node.is_built_with_sanitizer() or node.is_built_with_llvm_coverage():
        pytest.skip("Disabled for sanitizers and llvm coverage builds")

    # Default value of lg_prof_sample is 19
    assert get_lg_prof_sample() == 19

    set_config("jemalloc_profiler_sampling_rate", "15")
    node.restart_clickhouse()

    assert get_lg_prof_sample() == 15

    set_config("jemalloc_profiler_sampling_rate", "22")
    node.restart_clickhouse()

    assert get_lg_prof_sample() == 22

    # Restore default
    set_config("jemalloc_profiler_sampling_rate", "19")
    node.restart_clickhouse()

    assert get_lg_prof_sample() == 19
