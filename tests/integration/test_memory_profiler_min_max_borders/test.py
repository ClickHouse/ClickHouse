import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=["configs/memory_profiler.xml"],
    user_configs=["configs/max_untracked_memory.xml"],
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


def test_trace_boundaries_work(started_cluster):
    if node.is_built_with_sanitizer():
        pytest.skip("Disabled for sanitizers")

    node.query("select randomPrintableASCII(number) from numbers(1000) FORMAT Null")
    node.query("SYSTEM FLUSH LOGS")

    assert (
        node.query(
            "SELECT countDistinct(abs(size)) > 0 FROM system.trace_log where trace_type = 'MemorySample'"
        )
        == "1\n"
    )
    assert (
        node.query(
            "SELECT count() FROM system.trace_log where trace_type = 'MemorySample' and (abs(size) > 8192 or abs(size) < 4096)"
        )
        == "0\n"
    )
