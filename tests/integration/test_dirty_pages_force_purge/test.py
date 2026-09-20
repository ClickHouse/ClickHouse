import time

import pytest

from helpers.cluster import ClickHouseCluster

# Jemalloc configuration used in this test disables automated dirty pages purge,
# to simplify its accumulation, and verify that they'll be cleaned up by the
# MemoryWorker
MALLOC_CONF = "background_thread:false,dirty_decay_ms:-1,muzzy_decay_ms:0,oversize_threshold:0"
PEAK_MEMORY_UPPER_BOUND = 3 * 1024 * 1024 * 1024

PEAK_MEMORY_COUNTER_PATHS = [
    "/sys/fs/cgroup/memory/memory.max_usage_in_bytes",  # cgroup v1
    "/sys/fs/cgroup/memory.peak",                       # cgroup v2
]

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=["configs/overrides.yaml"],
    env_variables={"MALLOC_CONF": MALLOC_CONF},
)


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_dirty_pages_force_purge(start_cluster):
    if node.is_built_with_sanitizer():
        pytest.skip("Jemalloc disabled in sanitizer builds")

    purges = ""
    for _ in range(100):
        node.query("""
            SELECT arrayMap(x -> randomPrintableASCII(40), range(4096))
            FROM numbers(2048)
            FORMAT Null
        """)

        purges = node.query("SELECT value from system.events where event = 'MemoryAllocatorPurge'")
        if purges:
            break

        time.sleep(0.2)

    if not purges:
        raise TimeoutError("Timed out waiting for MemoryAllocatorPurge event")

    for path in PEAK_MEMORY_COUNTER_PATHS:
        try:
            peak_memory = int(node.exec_in_container(["cat", path]))
            break
        except Exception as ex:
            if not str(ex).lower().strip().endswith("no such file or directory"):
                raise
    else:
        raise RuntimeError("Failed to find peak memory counter")

    # Assert peak memory usage is lower than expected peak, which is noticeably lower
    # than max_server_memory_usage, to guarantee that purge has been cause by dirty pages
    # volume, not the memory tracker limit
    assert(peak_memory < PEAK_MEMORY_UPPER_BOUND)
