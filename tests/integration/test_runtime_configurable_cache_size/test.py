import os
import time
import pytest
from helpers.cluster import ClickHouseCluster

# Tests that sizes of in-memory caches (mark / uncompressed / index mark / index uncompressed / mmapped file / query cache) can be changed
# at runtime (issue #51085). This file tests only the mark cache (which uses the SLRU cache policy) and the query cache (which uses the TTL
# cache policy). As such, both tests are representative for the other caches.

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=["configs/default.xml"],
    stay_alive=True,
)


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
CONFIG_DIR = os.path.join(SCRIPT_DIR, "configs")


# temporarily disabled due to https://github.com/ClickHouse/ClickHouse/pull/51446#issuecomment-1687066351
# def test_mark_cache_size_is_runtime_configurable(start_cluster):
#     # the initial config specifies the mark cache size as 496 bytes, just enough to hold two marks
#     node.query("SYSTEM DROP MARK CACHE")
#
#     node.query("CREATE TABLE test1 (val String) ENGINE=MergeTree ORDER BY val")
#     node.query("INSERT INTO test1 VALUES ('abc') ('def') ('ghi')")
#     node.query("SELECT * FROM test1 WHERE val = 'def'")  # cache 1st mark
#
#     node.query("CREATE TABLE test2 (val String) ENGINE=MergeTree ORDER BY val")
#     node.query("INSERT INTO test2 VALUES ('abc') ('def') ('ghi')")
#     node.query("SELECT * FROM test2 WHERE val = 'def'")  # cache 2nd mark
#
#     # Result checking is based on asynchronous metrics. These are calculated by default every 1.0 sec, and this is also the
#     # smallest possible value. Found no statement to force-recalculate them, therefore waaaaait...
#     time.sleep(2.0)
#     res = node.query(
#         "SELECT value FROM system.asynchronous_metrics WHERE metric LIKE 'MarkCacheFiles'"
#     )
#     assert res == "2\n"
#     res = node.query(
#         "SELECT value FROM system.asynchronous_metrics WHERE metric LIKE 'MarkCacheBytes'"
#     )
#     assert res == "496\n"
#
#     # switch to a config with a mark cache size of 248 bytes
#     node.copy_file_to_container(
#         os.path.join(CONFIG_DIR, "smaller_mark_cache.xml"),
#         "/etc/clickhouse-server/config.d/default.xml",
#     )
#
#     node.query("SYSTEM RELOAD CONFIG")
#
#     # check that eviction worked as expected
#     time.sleep(2.0)
#     res = node.query(
#         "SELECT value FROM system.asynchronous_metrics WHERE metric LIKE 'MarkCacheFiles'"
#     )
#     assert res == "1\n"
#     res = node.query(
#         "SELECT value FROM system.asynchronous_metrics WHERE metric LIKE 'MarkCacheBytes'"
#     )
#     assert res == "248\n"
#
#     # check that the new mark cache maximum size is respected when more marks are cached
#     node.query("CREATE TABLE test3 (val String) ENGINE=MergeTree ORDER BY val")
#     node.query("INSERT INTO test3 VALUES ('abc') ('def') ('ghi')")
#     node.query("SELECT * FROM test3 WHERE val = 'def'")
#     time.sleep(2.0)
#     res = node.query(
#         "SELECT value FROM system.asynchronous_metrics WHERE metric LIKE 'MarkCacheFiles'"
#     )
#     assert res == "1\n"
#     res = node.query(
#         "SELECT value FROM system.asynchronous_metrics WHERE metric LIKE 'MarkCacheBytes'"
#     )
#     assert res == "248\n"
#
#     # restore the original config
#     node.copy_file_to_container(
#         os.path.join(CONFIG_DIR, "default.xml"),
#         "/etc/clickhouse-server/config.d/default.xml",
#     )


def test_query_cache_size_is_runtime_configurable(start_cluster):
    # the initial config specifies the maximum query cache size as 2, run 3 queries, expect 2 cache entries
    node.query("SYSTEM DROP QUERY CACHE")
    node.query("SELECT 1 SETTINGS use_query_cache = 1, query_cache_ttl = 1")
    node.query("SELECT 2 SETTINGS use_query_cache = 1, query_cache_ttl = 1")
    node.query("SELECT 3 SETTINGS use_query_cache = 1, query_cache_ttl = 1")

    time.sleep(2)
    node.query("SYSTEM RELOAD ASYNCHRONOUS METRICS")
    res = node.query(
        "SELECT value FROM system.asynchronous_metrics WHERE metric = 'QueryCacheEntries'",
    )
    assert res == "2\n"

    # switch to a config with a maximum query cache size of 1
    node.copy_file_to_container(
        os.path.join(CONFIG_DIR, "smaller_query_cache.xml"),
        "/etc/clickhouse-server/config.d/default.xml",
    )

    node.query("SYSTEM RELOAD CONFIG")

    # check that eviction worked as expected
    time.sleep(2)
    node.query("SYSTEM RELOAD ASYNCHRONOUS METRICS")
    res = node.query(
        "SELECT value FROM system.asynchronous_metrics WHERE metric = 'QueryCacheEntries'",
    )
    assert (
        res == "2\n"
    )  # "Why not 1?", you think. Reason is that QC uses the TTLCachePolicy that evicts lazily only upon insert.
    # Not a real issue, can be changed later, at least there's a test now.

    # Also, you may also wonder "why query_cache_ttl = 1"? Reason is that TTLCachePolicy only removes *stale* entries. With the default TTL
    # (60 sec), no entries would be removed at all. Again: not a real issue, can be changed later and there's at least a test now.

    # check that the new query cache maximum size is respected when more queries run
    node.query("SELECT 4 SETTINGS use_query_cache = 1, query_cache_ttl = 1")
    node.query("SELECT 5 SETTINGS use_query_cache = 1, query_cache_ttl = 1")

    time.sleep(2)
    node.query("SYSTEM RELOAD ASYNCHRONOUS METRICS")
    res = node.query(
        "SELECT value FROM system.asynchronous_metrics WHERE metric = 'QueryCacheEntries'",
    )
    assert res == "1\n"

    # restore the original config
    node.copy_file_to_container(
        os.path.join(CONFIG_DIR, "default.xml"),
        "/etc/clickhouse-server/config.d/default.xml",
    )
