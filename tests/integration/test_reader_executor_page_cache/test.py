import uuid

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.add_instance(
            "node",
            main_configs=["configs/conf.xml"],
            with_minio=True,
        )
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


# Settings that engage the experimental ReaderExecutor with the userspace page (memory) cache as the
# only tier on a raw-s3 disk (no filesystem cache):
#  - use_reader_executor: turn on the executor path.
#  - remote_filesystem_read_method='read': avoid the async-prefetch stage (not implemented by the
#    executor, would force a fallback to the legacy path).
#  - use_page_cache_for_disks_without_file_cache: enable the page cache on the cache-less s3 disk, so
#    DiskObjectStorage::prepareRead requests the memory-cache stage and the executor builds the
#    PageCacheProvider tier.
#  - page_cache_inject_eviction=0: keep the cold read's populate alive for the warm read.
#  - read_from_page_cache_if_exists_otherwise_bypass_cache=0: populate on miss (not read-only bypass).
READER_EXECUTOR_PAGE_CACHE_SETTINGS = (
    "use_reader_executor=1, "
    "remote_filesystem_read_method='read', "
    "use_page_cache_for_disks_without_file_cache=1, "
    "page_cache_inject_eviction=0, "
    "read_from_page_cache_if_exists_otherwise_bypass_cache=0"
)


def _profile_events(node, query_id):
    node.query("system flush logs")
    row = node.query(
        "SELECT "
        "ProfileEvents['ReaderExecutorBytesFromSource'], "
        "ProfileEvents['ReaderExecutorCachePopulateRequests'], "
        "ProfileEvents['PageCacheMisses'], "
        "ProfileEvents['PageCacheHits'] "
        f"FROM system.query_log WHERE query_id='{query_id}' AND type='QueryFinish'"
    ).strip()
    src, populate, misses, hits = (int(x) for x in row.split("\t"))
    return src, populate, misses, hits


def test_reader_executor_populates_and_serves_page_cache(started_cluster):
    node = cluster.instances["node"]

    node.query(
        "CREATE TABLE t_re_page_cache (k UInt64 CODEC(NONE)) "
        "ENGINE = MergeTree ORDER BY k "
        "SETTINGS storage_policy = 's3', min_bytes_for_wide_part = 0"
    )
    # One stable wide part, not populated on write.
    node.query("SYSTEM STOP MERGES t_re_page_cache")
    node.query("INSERT INTO t_re_page_cache SELECT number FROM numbers(1000000)")
    node.query("SYSTEM DROP PAGE CACHE")

    # Cold read: nothing cached, so the executor reads from the source and populates the page cache.
    cold_id = uuid.uuid4().hex
    node.query(
        f"SELECT sum(k) FROM t_re_page_cache SETTINGS {READER_EXECUTOR_PAGE_CACHE_SETTINGS}",
        query_id=cold_id,
    )
    cold_src, cold_populate, cold_misses, _ = _profile_events(node, cold_id)
    # The executor engaged (read physical bytes from the source) and populated the page cache itself.
    # These ReaderExecutor* counters are emitted only by the executor, so they also prove engagement.
    assert cold_src > 0, "executor did not read from the source (did it fall back?)"
    assert cold_populate > 0, "executor did not populate the page cache"
    assert cold_misses > 0, "cold read did not miss the page cache"

    # Warm read: the same bytes are now served from the userspace page cache.
    warm_id = uuid.uuid4().hex
    node.query(
        f"SELECT sum(k) FROM t_re_page_cache SETTINGS {READER_EXECUTOR_PAGE_CACHE_SETTINGS}",
        query_id=warm_id,
    )
    warm_src, _, _, warm_hits = _profile_events(node, warm_id)
    assert warm_hits > 0, "warm read did not hit the page cache"
    assert (
        warm_src < cold_src
    ), "warm read did not read fewer source bytes than the cold read"

    node.query("DROP TABLE t_re_page_cache")
    node.query("SYSTEM DROP PAGE CACHE")
