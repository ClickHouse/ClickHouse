import time
import uuid

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.add_instance(
            "node1",
            main_configs=["configs/conf.xml"],
            with_minio=True,
            mem_limit='20g',
            cpu_limit=7,
        )
        cluster.add_instance(
            "node_smol",
            main_configs=["configs/conf.xml", "configs/smol.xml"],
            with_minio=True,
            mem_limit='20g',
            cpu_limit=7,
        )
        cluster.start()

        yield cluster
    finally:
        cluster.shutdown()


def test_basics(started_cluster):
    # Write ~8 MB of data.
    node = cluster.instances["node1"]
    node.query(
        "create table a (k Int64 CODEC(NONE)) engine MergeTree order by k settings storage_policy = 's3';"
        "system stop merges a;"
        "insert into a select * from numbers(1000000);"
    )

    # Check that page cache is initially ~empty.
    assert (
        int(
            node.query(
                "select value from system.metrics where metric = 'PageCacheBytes'"
            )
        )
        < 4000000
    )

    # Cold read, should miss cache. (Populating cache on write is not implemented.)
    query_id = uuid.uuid4().hex
    node.query(
        "select sum(k) from a settings use_page_cache_for_disks_without_file_cache=1",
        query_id=query_id,
    )
    node.query("system flush logs")
    # Don't check that number of hits is zero - it's usually not.
    assert (
        int(
            node.query(
                f"select ProfileEvents['PageCacheMisses'] from system.query_log where query_id='{query_id}' and type = 'QueryFinish'"
            )
        )
        > 0
    )
    assert (
        int(
            node.query(
                "select value from system.metrics where metric = 'PageCacheBytes'"
            )
        )
        > 4000000
    )

    # Repeat read, should hit cache.
    query_id = uuid.uuid4().hex
    node.query(
        "select sum(k) from a settings use_page_cache_for_disks_without_file_cache=1",
        query_id=query_id,
    )
    node.query("system flush logs")
    assert (
        node.query(
            f"select ProfileEvents['PageCacheMisses'], ProfileEvents['PageCacheHits'] > 0 from system.query_log where query_id='{query_id}' and type = 'QueryFinish'"
        )
        == "0\t1\n"
    )

    # Drop cache and read again, should miss. Also don't write to cache.
    node.query("system drop page cache")
    query_id = uuid.uuid4().hex
    node.query(
        "select sum(k) from a settings use_page_cache_for_disks_without_file_cache=1, read_from_page_cache_if_exists_otherwise_bypass_cache=1",
        query_id=query_id,
    )
    node.query("system flush logs")
    assert (
        int(
            node.query(
                f"select ProfileEvents['PageCacheMisses'] from system.query_log where query_id='{query_id}' and type = 'QueryFinish'"
            )
        )
        > 0
    )
    assert (
        int(
            node.query(
                "select value from system.metrics where metric = 'PageCacheBytes'"
            )
        )
        < 4000000
    )

    # Repeat read, should still miss, but populate cache.
    query_id = uuid.uuid4().hex
    node.query(
        "select sum(k) from a settings use_page_cache_for_disks_without_file_cache=1",
        query_id=query_id,
    )
    node.query("system flush logs")
    assert (
        int(
            node.query(
                f"select ProfileEvents['PageCacheMisses'] from system.query_log where query_id='{query_id}' and type = 'QueryFinish'"
            )
        )
        > 0
    )
    assert (
        int(
            node.query(
                "select value from system.metrics where metric = 'PageCacheBytes'"
            )
        )
        > 4000000
    )

    # Read again, hit the cache.
    query_id = uuid.uuid4().hex
    node.query(
        "select sum(k) from a settings use_page_cache_for_disks_without_file_cache=1",
        query_id=query_id,
    )
    node.query("system flush logs")
    assert (
        node.query(
            f"select ProfileEvents['PageCacheMisses'], ProfileEvents['PageCacheHits'] > 0 from system.query_log where query_id='{query_id}' and type = 'QueryFinish'"
        )
        == "0\t1\n"
    )

    node.query("drop table a;" "system drop page cache;")


def test_cache_arena_isolation(started_cluster):
    """Test that page cache allocations land in the dedicated jemalloc cache arena
    and that SYSTEM DROP PAGE CACHE reclaims arena pages."""
    node = cluster.instances["node1"]

    # Skip if jemalloc is not available
    jemalloc_enabled = node.query(
        "SELECT value IN ('ON', '1') FROM system.build_options WHERE name = 'USE_JEMALLOC'"
    ).strip()
    if jemalloc_enabled != "1":
        pytest.skip("jemalloc not available")

    node.query(
        "CREATE TABLE IF NOT EXISTS t_arena_page_cache (k Int64 CODEC(NONE)) "
        "ENGINE MergeTree ORDER BY k SETTINGS storage_policy = 's3';"
        "SYSTEM STOP MERGES t_arena_page_cache;"
        "INSERT INTO t_arena_page_cache SELECT * FROM numbers(1000000);"
    )

    # Start clean
    node.query("SYSTEM DROP PAGE CACHE")
    node.query("SYSTEM DROP MARK CACHE")
    time.sleep(2)  # Wait for asynchronous_metrics update

    # Record baseline arena state
    baseline_pactive = int(
        node.query(
            "SELECT value FROM system.asynchronous_metrics "
            "WHERE metric = 'jemalloc.cache_arena.pactive'"
        )
    )

    # Populate page cache + mark cache via S3 read
    node.query(
        "SELECT sum(k) FROM t_arena_page_cache "
        "SETTINGS use_page_cache_for_disks_without_file_cache=1"
    )

    # Verify page cache is populated
    page_cache_bytes = int(
        node.query(
            "SELECT value FROM system.metrics WHERE metric = 'PageCacheBytes'"
        )
    )
    assert page_cache_bytes > 0, f"Expected PageCacheBytes > 0, got {page_cache_bytes}"

    time.sleep(2)  # Wait for asynchronous_metrics update

    # Verify cache arena has more active pages than baseline
    current_pactive = int(
        node.query(
            "SELECT value FROM system.asynchronous_metrics "
            "WHERE metric = 'jemalloc.cache_arena.pactive'"
        )
    )
    assert current_pactive > baseline_pactive, (
        f"Expected cache arena pactive to increase: "
        f"baseline={baseline_pactive}, current={current_pactive}"
    )

    # Drop page cache and mark cache (both trigger arena purge)
    node.query("SYSTEM DROP PAGE CACHE")
    node.query("SYSTEM DROP MARK CACHE")

    # Verify page cache is empty
    page_cache_bytes_after = int(
        node.query(
            "SELECT value FROM system.metrics WHERE metric = 'PageCacheBytes'"
        )
    )
    assert page_cache_bytes_after == 0, (
        f"Expected PageCacheBytes = 0, got {page_cache_bytes_after}"
    )

    time.sleep(2)  # Wait for asynchronous_metrics update

    # Verify cache arena pages are reclaimed
    final_pactive = int(
        node.query(
            "SELECT value FROM system.asynchronous_metrics "
            "WHERE metric = 'jemalloc.cache_arena.pactive'"
        )
    )
    assert final_pactive == 0, (
        f"Expected cache arena pactive = 0 after drop + purge, got {final_pactive}"
    )

    node.query("DROP TABLE t_arena_page_cache SYNC")


def test_size_adjustment(started_cluster):
    node = cluster.instances["node_smol"]

    if (
        node.is_built_with_thread_sanitizer()
        or node.is_built_with_memory_sanitizer()
        or node.is_built_with_address_sanitizer()
    ):
        pytest.skip("sanitizer build has higher memory consumption; also it is slow")

    rss = int(
        node.query(
            "select value from system.asynchronous_metrics where metric = 'MemoryResident'"
        )
    )
    # Check there's at least some free memory for page cache. If this fails, maybe server's memory
    # usage bloated enough that max_server_memory_usage needs to be increased in this test
    # (along with numbers in the next query below).
    assert rss < 2.5e9

    # Insert 3.2 GB of data, more than max_server_memory_usage (3.0 GB). Then read it with cache enabled.
    node.query(
        "create table a (k Int64 CODEC(NONE)) engine MergeTree order by k settings storage_policy = 's3';"
        "system stop merges a;"
        "insert into a select * from numbers(400000000);"
        "select sum(k) from a settings use_page_cache_for_disks_without_file_cache=1;"
    )
    initial_cache_size = int(
        node.query(
            "select value from system.metrics where metric = 'PageCacheBytes'"
        )
    )
    assert initial_cache_size > 50000000

    # Do a query that uses lots of memory (and fails), check that the cache was shrunk to ~page_cache_min_size.
    err = node.query_and_get_error(
        "select groupArray(number) from numbers(10000000000)"
    )
    assert "MEMORY_LIMIT_EXCEEDED" in err

    # (There used to be a check here that system.query_log shows high enough memory usage for the previous
    #  query, but it was flaky because log flush sometimes hits memory limit and fails.)

    assert (
        int(
            node.query(
                "select value from system.metrics where metric = 'PageCacheBytes'"
            )
        )
        < 50000000
    )

    node.query("drop table a;" "system drop page cache;")
