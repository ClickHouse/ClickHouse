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
    node.query("SYSTEM RELOAD ASYNCHRONOUS METRICS")

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

    node.query("SYSTEM RELOAD ASYNCHRONOUS METRICS")

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

    node.query("SYSTEM RELOAD ASYNCHRONOUS METRICS")

    # Verify cache arena pages decreased after clearing
    final_pactive = int(
        node.query(
            "SELECT value FROM system.asynchronous_metrics "
            "WHERE metric = 'jemalloc.cache_arena.pactive'"
        )
    )
    assert final_pactive < current_pactive, (
        f"Expected cache arena pactive to decrease after drop + purge: "
        f"before={current_pactive}, after={final_pactive}"
    )

    node.query("DROP TABLE t_arena_page_cache SYNC")


def test_text_index_lookahead(started_cluster):
    """Text index readers visit blocks out of file order, so `use_page_cache_lookahead_for_text_index`
    keeps their page cache lookahead at one block by default. A longer lookahead is abandoned at the
    next seek, and the S3 connection is reset. A plain sequential scan keeps the normal lookahead."""
    node = cluster.instances["node1"]

    node.query(
        "drop table if exists t_text_index_lookahead;"
        "drop table if exists t_text_index_lookahead_seq;"
    )

    # 40 tokens, 8 of them (searched below) with much larger cardinalities than the 32 filler tokens.
    # A hash shuffle interleaves every token's rows, so its own postings cannot compress into one short run.
    node.query(
        """
        create table t_text_index_lookahead
        (
            s String,
            index idx s type text(tokenizer = 'splitByNonAlpha', dictionary_block_size = 4, posting_list_block_size = 256)
        )
        engine = MergeTree order by tuple()
        settings storage_policy = 's3', min_bytes_for_wide_part = 0,
            min_compress_block_size = 256, max_compress_block_size = 256;

        insert into t_text_index_lookahead (s)
        select concat('kwneedle', leftPad(toString(token_idx_and_rows.1), 2, '0'))
        from
        (
            select arrayJoin(arrayZip(
                range(40),
                [2500, 1000, 1000, 1000, 1000, 15000, 1000, 1000, 1000, 1000,
                 7500, 1000, 1000, 1000, 1000, 20000, 1000, 1000, 1000, 1000,
                 12500, 1000, 1000, 1000, 1000, 5000, 1000, 1000, 1000, 1000,
                 17500, 1000, 1000, 1000, 1000, 10000, 1000, 1000, 1000, 1000]
            )) as token_idx_and_rows
        ) as tokens
        array join range(token_idx_and_rows.2) as row_in_token
        order by sipHash64(row_in_token, token_idx_and_rows.1);

        optimize table t_text_index_lookahead final;

        create table t_text_index_lookahead_seq (n UInt64)
        engine = MergeTree order by tuple()
        settings storage_policy = 's3', min_bytes_for_wide_part = 0,
            min_compress_block_size = 256, max_compress_block_size = 256;

        insert into t_text_index_lookahead_seq select sipHash64(number) from numbers(150000);

        optimize table t_text_index_lookahead_seq final;
        """
    )

    # The 8 tokens are listed in file order, but are actually read rarest-first; the *_cache settings
    # disable the server-wide text index caches, so the query reads through the page cache.
    search_query = """
        select count() from t_text_index_lookahead
        where hasAnyTokens(s, ['kwneedle00', 'kwneedle05', 'kwneedle10', 'kwneedle15', 'kwneedle20', 'kwneedle25', 'kwneedle30', 'kwneedle35'])
        settings
            use_page_cache_for_disks_without_file_cache = 1,
            page_cache_block_size = 1024,
            max_threads = 1,
            use_text_index_tokens_cache = 0,
            use_text_index_negative_tokens_cache = 0,
            use_text_index_pattern_bypass_cache = 0,
            use_text_index_header_cache = 0,
            use_text_index_postings_cache = 0,
            use_query_condition_cache = 0,
            use_page_cache_lookahead_for_text_index = {}
        """

    def run_cold(query):
        node.query("system drop page cache")
        query_id = uuid.uuid4().hex
        result = node.query(query, query_id=query_id)
        return result, query_id

    default_result, default_query_id = run_cold(search_query.format(0))
    setting_on_result, setting_on_query_id = run_cold(search_query.format(1))
    seq_result, seq_query_id = run_cold(
        "select sum(n) from t_text_index_lookahead_seq settings "
        "use_page_cache_for_disks_without_file_cache = 1, page_cache_block_size = 1024, max_threads = 1"
    )

    assert default_result == "90000\n"
    assert setting_on_result == "90000\n"
    assert seq_result == "15975038870141670131\n"

    node.query("system flush logs query_log")

    def get_events(query_id):
        tsv = node.query(
            "select ProfileEvents['PageCacheMisses'], ProfileEvents['DiskConnectionsReset'], ProfileEvents['S3GetObject'] "
            f"from system.query_log where query_id = '{query_id}' and type = 'QueryFinish'"
        )
        misses, resets, gets = map(int, tsv.split())
        return {"misses": misses, "resets": resets, "gets": gets}

    # Default: many misses, but almost nothing is abandoned.
    events = get_events(default_query_id)
    logging.info(f"default: {events}")
    assert events["misses"] >= 100
    assert events["resets"] <= 2

    # With the normal lookahead, many more connections are abandoned.
    events = get_events(setting_on_query_id)
    logging.info(f"setting on: {events}")
    assert events["resets"] > 2

    # Sequential scan: many misses, still grouped into few requests by the normal lookahead.
    events = get_events(seq_query_id)
    logging.info(f"sequential: {events}")
    assert events["misses"] >= 100
    assert events["gets"] * 8 < events["misses"]

    node.query(
        "drop table t_text_index_lookahead sync;"
        "drop table t_text_index_lookahead_seq sync;"
        "system drop page cache;"
    )


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
