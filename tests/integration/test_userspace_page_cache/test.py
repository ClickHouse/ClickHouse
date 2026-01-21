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
        )
        cluster.add_instance(
            "node_smol",
            main_configs=["configs/conf.xml", "configs/smol.xml"],
            with_minio=True,
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
    node.query("system reload asynchronous metrics;")
    assert (
        int(
            node.query(
                "select value from system.asynchronous_metrics where metric = 'PageCacheBytes'"
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
    node.query("system flush logs; system reload asynchronous metrics;")
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
                "select value from system.asynchronous_metrics where metric = 'PageCacheBytes'"
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
    node.query("system flush logs; system reload asynchronous metrics;")
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
                "select value from system.asynchronous_metrics where metric = 'PageCacheBytes'"
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
    node.query("system flush logs; system reload asynchronous metrics;")
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
                "select value from system.asynchronous_metrics where metric = 'PageCacheBytes'"
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
    node.query("system reload asynchronous metrics")
    initial_cache_size = int(
        node.query(
            "select value from system.asynchronous_metrics where metric = 'PageCacheBytes'"
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

    node.query("system reload asynchronous metrics;")
    assert (
        int(
            node.query(
                "select value from system.asynchronous_metrics where metric = 'PageCacheBytes'"
            )
        )
        < 50000000
    )

    node.query("drop table a;" "system drop page cache;")
