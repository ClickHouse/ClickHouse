import os
import subprocess
import uuid
import time
import logging
import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)


def _is_sanitizer_build():
    """Detect sanitizer builds before starting the cluster, to avoid creating
    containers that are slow to shut down under sanitizers."""
    binary = os.environ.get(
        "CLICKHOUSE_TESTS_SERVER_BIN_PATH", "/usr/bin/clickhouse"
    )
    try:
        result = subprocess.run(
            [
                binary,
                "local",
                "--query",
                "SELECT value FROM system.build_options WHERE name = 'CXX_FLAGS'",
            ],
            capture_output=True,
            text=True,
            timeout=30,
        )
        return "-fsanitize=" in result.stdout
    except Exception:
        return False


is_sanitizer = _is_sanitizer_build()


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.add_instance(
            "node1",
            main_configs=["configs/conf.xml"],
            with_minio=True,
            mem_limit="20g",
            cpu_limit=7,
        )
        # Don't create node_smol in sanitizer builds: the ASan leak checker at
        # exit is very slow with high max_server_memory_usage, causing Docker
        # teardown to hang for minutes.
        if not is_sanitizer:
            cluster.add_instance(
                "node_smol",
                main_configs=["configs/conf.xml", "configs/smol.xml"],
                with_minio=True,
                mem_limit="20g",
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


def test_size_adjustment(started_cluster):
    if is_sanitizer:
        pytest.skip("sanitizer build has higher memory consumption; also it is slow")

    node = cluster.instances["node_smol"]

    # Clean up from a possible previous failed run.
    node.query("drop table if exists a;" "system drop page cache;")

    # A few GB, see configs/smol.xml
    memory_limit = int(node.query("select value from system.server_settings where name='max_server_memory_usage'"))

    try:
        # Insert more data than max_server_memory_usage.
        node.query(
            "create table a (k Int64 CODEC(NONE)) engine MergeTree order by k settings storage_policy = 's3';"
            "system stop merges a;"
            f"insert into a select * from numbers({int(memory_limit * 1.1 // 8)});"
        )

        # Make sure asynchronous metrics update.
        time.sleep(3)

        def get_metrics():
            tsv = node.query("select metric, toInt64(value) as value from system.asynchronous_metrics where metric in ('CGroupMemoryTotal', 'CGroupMemoryUsed', 'OSMemoryTotal', 'MemoryResident', 'PageCacheMaxBytes') UNION ALL select metric, value from system.metrics where metric in ('PageCacheBytes', 'PageCacheCells')")
            pairs = map(lambda p: p.split('\t'), tsv.strip().split('\n'))
            return {k: int(v) for [k, v] in pairs}

        metrics = get_metrics()
        logging.info(f"server metrics before reads: {metrics}")
        memory_used = metrics['MemoryResident']
        os_memory_limit = metrics['OSMemoryTotal']
        if 'CGroupMemoryUsed' in metrics:
            memory_used = max(memory_used, metrics['CGroupMemoryUsed'])
        if 'CGroupMemoryTotal' in metrics:
            os_memory_limit = min(os_memory_limit, metrics['CGroupMemoryTotal'])

        # Check that the test is run with a high enough memory limit in cgroups.
        assert os_memory_limit >= memory_limit

        # Check there's at least some free memory for page cache. If this fails, maybe server's memory
        # usage bloated enough that max_server_memory_usage needs to be increased in configs/smol.xml
        memory_free = min(os_memory_limit, memory_limit) - memory_used
        assert memory_free > 100e6

        assert metrics['PageCacheMaxBytes'] > 60e6
        assert metrics['PageCacheBytes'] < 10e6

        # Read with cache enabled.
        node.query(
            "select sum(k) from a settings use_page_cache_for_disks_without_file_cache=1;"
        )

        metrics = get_metrics()
        logging.info(f"server metrics after first read: {metrics}")

        assert metrics['PageCacheBytes'] > 50e6

        # Do a query that uses lots of memory (and fails), check that the cache was shrunk to ~page_cache_min_size.
        err = node.query_and_get_error(
            "select groupArray(number) from numbers(10000000000)"
        )
        assert "MEMORY_LIMIT_EXCEEDED" in err

        # (There used to be a check here that system.query_log shows high enough memory usage for the previous
        #  query, but it was flaky because log flush sometimes hits memory limit and fails.)

        metrics = get_metrics()
        logging.info(f"server metrics after second read: {metrics}")
        assert metrics['PageCacheBytes'] < 50e6
    finally:
        node.query("drop table if exists a;" "system drop page cache;")
