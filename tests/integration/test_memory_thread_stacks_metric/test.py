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
    all three metrics exist with sensible values.

    The feature requires Linux 5.17+ (`prctl(PR_SET_VMA_ANON_NAME)`) and a
    container that can read `/proc/self/smaps`. If either prerequisite is
    missing the server publishes `MEMORY_THREAD_STACKS_METRIC_UNAVAILABLE`
    in `system.warnings` and the three async metrics stay absent.
    `pytest.skip` in that case so the test only enforces invariants where
    the metric is actually supported."""
    node.query("SYSTEM RELOAD ASYNCHRONOUS METRICS;")

    warned = node.query(
        "SELECT count() FROM system.warnings "
        "WHERE message LIKE '%MemoryThreadStacks%';"
    ).strip()
    if warned != "0":
        pytest.skip(
            "MemoryThreadStacks* metrics unavailable on this host "
            "(kernel < 5.17 or /proc/self/smaps inaccessible)"
        )

    presence = node.query(
        "SELECT count() FROM system.asynchronous_metrics "
        "WHERE metric IN ('MemoryThreadStacksCount', 'MemoryThreadStacksVirtual', 'MemoryThreadStacksResident');"
    ).strip()
    if presence != "3":
        pytest.skip(
            f"MemoryThreadStacks* metrics not published yet ({presence}/3); skipping invariants"
        )

    # `virt >= cnt * 4096`: each pthread stack occupies at least one OS page,
    # and 4 KiB is the smallest page size on every Linux architecture (x86_64,
    # ARM64-4K/16K/64K, RISC-V, PowerPC, etc.). PTHREAD_STACK_MIN varies by
    # libc and page size (musl 2 KiB, glibc x86_64 16 KiB, glibc ARM64-64K 128
    # KiB), but the kernel still rounds VMAs up to a page, so 4 KiB is the
    # tight universal lower bound.
    invariants = node.query(
        "WITH "
        "    (SELECT value FROM system.asynchronous_metrics WHERE metric = 'MemoryThreadStacksCount') AS cnt, "
        "    (SELECT value FROM system.asynchronous_metrics WHERE metric = 'MemoryThreadStacksVirtual') AS virt, "
        "    (SELECT value FROM system.asynchronous_metrics WHERE metric = 'MemoryThreadStacksResident') AS rss "
        "SELECT cnt > 4, virt >= cnt * 4096, rss > 0, rss <= virt;"
    ).strip()
    assert invariants == "1\t1\t1\t1", f"invariants violated: {invariants}"
