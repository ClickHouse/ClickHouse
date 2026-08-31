#!/usr/bin/env bash
# The query cache on disk (setting `query_cache_on_disk_cache_name`) serializes result chunks keeping their column
# representations: a Const column stays Const (the entry on disk stays small), and enabling the on-disk cache must not
# change what the in-memory query cache stores (a Const result must not be materialized and trip the entry size limit).
# Sparse columns must survive the round trip through the on-disk cache as well.
# Uses the preconfigured filesystem cache 'cache_for_query_results' (see tests/config/config.d/query_result_cache_on_disk.xml).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `max_block_size` is pinned: the size of a cached Const result is dominated by the per-chunk padding of the single-row
# data columns, so a randomized (small) block size would multiply the number of chunks and blow up the entry size.
# `query_cache_tag` disambiguates the entries in system.query_cache: the flaky check runs this test many times against
# one server, the in-memory query cache is server-global and keeps listing stale entries, and the tag (unlike the query
# text with a short random number in it) is guaranteed unique per test instance.
settings="use_query_cache = 1, query_cache_on_disk_cache_name = 'cache_for_query_results', query_cache_squash_partial_results = 0, max_block_size = 65409, query_cache_tag = '${CLICKHOUSE_DATABASE}'"

rnd=$(tr -dc 1-9 </dev/urandom | head -c 5) # disambiguates the queries in system.query_log below

echo "-- A Const result stays Const in both backends (the default entry size limit is 1 MiB, materialized it would be 2 MB)"
query_const="SELECT 1 AS v FROM numbers(2000000) WHERE ${rnd} > 0 SETTINGS ${settings}"
${CLICKHOUSE_CLIENT} --query "${query_const}" | uniq -c | sed 's/^ *//'

echo "-- The entry was accepted by the in-memory cache and stayed small (materialized it would be at least 2 MB)"
${CLICKHOUSE_CLIENT} --query "SELECT result_size < 1000000 FROM system.query_cache WHERE tag = '${CLICKHOUSE_DATABASE}' AND query LIKE 'SELECT 1 AS v FROM numbers(2000000) WHERE ${rnd} > 0%'"

echo "-- The result is served from disk when reads from the in-memory cache are disabled"
${CLICKHOUSE_CLIENT} --query "${query_const}, enable_reads_from_query_cache = 0" | uniq -c | sed 's/^ *//'

echo "-- The entry on disk is small, and the second run read it (from system.query_log)"
${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS query_log"
${CLICKHOUSE_CLIENT} --query "
    SELECT ProfileEvents['QueryCacheOnDiskWrittenBytes'] < 100000, ProfileEvents['QueryCacheOnDiskHits']
    FROM system.query_log
    WHERE event_date >= yesterday() AND event_time >= now() - 600 AND type = 'QueryFinish'
        AND current_database = currentDatabase() AND query LIKE 'SELECT 1 AS v FROM numbers(2000000) WHERE ${rnd} > 0%'
    ORDER BY event_time_microseconds"

echo "-- A Sparse column survives the round trip through the on-disk cache"
${CLICKHOUSE_CLIENT} --query "
    DROP TABLE IF EXISTS t_05025;
    CREATE TABLE t_05025 (k UInt64, s UInt64) ENGINE = MergeTree ORDER BY k
        SETTINGS ratio_of_defaults_for_sparse_serialization = 0.5;
    INSERT INTO t_05025 SELECT number, if(number = 500000, 777, 0) FROM numbers(1000000);"
query_sparse="SELECT s FROM t_05025 SETTINGS ${settings}, max_threads = 1"
${CLICKHOUSE_CLIENT} --query "${query_sparse}" | uniq -c | sed 's/^ *//'
${CLICKHOUSE_CLIENT} --query "${query_sparse}, enable_reads_from_query_cache = 0" | uniq -c | sed 's/^ *//'

${CLICKHOUSE_CLIENT} --query "DROP TABLE t_05025"
