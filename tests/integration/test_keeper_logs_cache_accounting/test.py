"""
The latest logs cache is bounded by `latest_logs_cache_size_threshold`, and that bound is meant to
limit memory. Charging only the serialized payload of each entry understates the real cost several
times over for small entries: every cached entry also carries the `nuraft::log_entry` object, two
`shared_ptr` control blocks, a hash node and a bucket slot, and its buffer allocation is rounded up
to an allocator size class.

This test writes many small entries into a Keeper with a small cache threshold and checks, through
`lgif`, that the reported cache size accounts for that per-entry overhead and that the threshold is
actually respected.
"""

import logging
import time

import pytest

import helpers.keeper_utils as keeper_utils
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance(
    "node1",
    main_configs=["configs/enable_keeper1.xml"],
    stay_alive=True,
    with_zookeeper=False,
)

CACHE_SIZE_THRESHOLD = 65536  # keep in sync with configs/enable_keeper1.xml
NUM_ZNODES = 3000  # payload of all of these together far exceeds the threshold
ZNODE_ROOT = "/cache_accounting"

# Fixed per-entry cost charged by `cachedLogEntryBytes`, in bytes: the `log_entry` object with its
# control block, the buffer's control block, and the hash node with its bucket slot. The znodes
# below are deliberately tiny (short path, no data), so an entry's payload stays well under this,
# which is what makes the average below discriminating: payload-only accounting cannot reach it.
FIXED_OVERHEAD_PER_ENTRY = 184


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def get_log_info(node):
    """Send `lgif` and parse its "<name>\t<value>" lines into a dict of ints."""
    data = keeper_utils.send_4lw_cmd(cluster, node, "lgif")
    result = {}
    for line in data.strip().split("\n"):
        parts = line.split("\t")
        if len(parts) == 2:
            result[parts[0]] = int(parts[1])
    return result


def test_latest_logs_cache_accounts_per_entry_overhead(started_cluster):
    keeper_utils.wait_until_connected(cluster, node1)

    zk = keeper_utils.get_fake_zk(cluster, node1.name)
    try:
        zk.create(ZNODE_ROOT)
        for i in range(NUM_ZNODES):
            zk.create(f"{ZNODE_ROOT}/{i:04d}")
    finally:
        zk.stop()
        zk.close()

    # Eviction runs on flush, so the steady state is reached shortly after the last write. Poll
    # rather than sleep: the bound is what we are asserting, so waiting for it is part of the test.
    deadline = time.time() + 30
    while True:
        log_info = get_log_info(node1)
        cache_size = log_info["latest_logs_cache_size"]
        cache_entries = log_info["latest_logs_cache_entries"]
        if cache_size <= CACHE_SIZE_THRESHOLD:
            break
        assert time.time() < deadline, (
            f"Cache stayed above its threshold: {cache_size} > {CACHE_SIZE_THRESHOLD} "
            f"with {cache_entries} entries"
        )
        time.sleep(0.5)

    assert cache_entries > 0, "Nothing was left in the cache to make assertions about"

    logging.info(
        "latest_logs_cache_size=%s latest_logs_cache_entries=%s (%.1f bytes per entry)",
        cache_size,
        cache_entries,
        cache_size / cache_entries,
    )

    # The whole point of the change: the charge per entry covers the per-entry objects, not just the
    # serialized entry. With payload-only accounting these tiny entries average far less than this.
    average_bytes = cache_size / cache_entries
    assert average_bytes >= FIXED_OVERHEAD_PER_ENTRY, (
        f"Cache charges {average_bytes:.1f} bytes per entry, which is below the fixed per-entry "
        f"overhead of {FIXED_OVERHEAD_PER_ENTRY} bytes - the accounting ignores it"
    )

    # And the entries really are small, so the average above is overhead rather than payload: the
    # cache holds far fewer entries than the threshold would admit if only payloads were charged.
    assert cache_entries < CACHE_SIZE_THRESHOLD / FIXED_OVERHEAD_PER_ENTRY + 1, (
        f"Cache holds {cache_entries} entries under a {CACHE_SIZE_THRESHOLD} byte threshold, "
        f"which is more than the per-entry overhead alone allows"
    )
