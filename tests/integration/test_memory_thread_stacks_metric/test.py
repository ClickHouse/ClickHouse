import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=["configs/heavy_metrics.xml"],
)


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_memory_thread_stacks_metric(start_cluster):
    """`MemoryThreadStacks{Count,Virtual,Resident}` are populated by the
    heavy-metrics cadence (smaps walk). Force a refresh, then check that
    all three metrics exist with sensible values."""
    node.query("SYSTEM RELOAD ASYNCHRONOUS METRICS;")

    presence = node.query(
        "SELECT count() FROM system.asynchronous_metrics "
        "WHERE metric IN ('MemoryThreadStacksCount', 'MemoryThreadStacksVirtual', 'MemoryThreadStacksResident');"
    ).strip()
    assert presence == "3", f"expected 3 metrics, got {presence}"

    invariants = node.query(
        "WITH "
        "    (SELECT value FROM system.asynchronous_metrics WHERE metric = 'MemoryThreadStacksCount') AS cnt, "
        "    (SELECT value FROM system.asynchronous_metrics WHERE metric = 'MemoryThreadStacksVirtual') AS virt, "
        "    (SELECT value FROM system.asynchronous_metrics WHERE metric = 'MemoryThreadStacksResident') AS rss "
        "SELECT cnt > 4, virt >= cnt * 16384, rss >= 0, rss <= virt;"
    ).strip()
    assert invariants == "1\t1\t1\t1", f"invariants violated: {invariants}"
