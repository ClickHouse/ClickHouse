#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: needs Parquet

# Tests that the Query Condition Cache works for local Parquet files backed by the
# `File` table engine (previously the cache was populated only for object storage).
# The first query with a given predicate is a cache miss and records the row groups
# that contain no matching rows; a second query with the same predicate is a cache
# hit and reuses that information.
#
# We assert on the QueryConditionCacheMisses / QueryConditionCacheHits profile events,
# which is the reliable signal that the cache was populated and reused. `read_rows`
# alone does not distinguish here: Parquet bloom-filter pruning already skips the
# absent (odd) value on the very first query, so both queries read the same number of
# rows regardless of the cache.
#
# The file version token (sub-second mtime + inode + size) is folded into the cache
# key via `QueryConditionCache::makeFilePartName`, and it is trusted only after the
# file has settled: filesystem timestamps are coarser than the wall clock, so a
# rewrite that lands in the same timestamp tick as the previous write could otherwise
# produce an identical token and reuse a stale entry. The table therefore points at
# an explicit file under `user_files`, and the test pins the mtime explicitly:
#   - a settled (past) mtime engages the cache: miss, then hit;
#   - a fresh (future) mtime fails close: correct results, no cache reads or writes;
#   - an in-place rewrite with a different settled mtime is a fresh cache miss - the
#     stale "nothing matches" decision for the old file version must not be reused.
#
# The table gets a unique UUID per test run and a per-database file path, so the
# cache keys are isolated and the test is parallel-safe without a global cache reset.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA_FILE="${USER_FILES_PATH:?}/${CLICKHOUSE_DATABASE}/04498_query_condition_cache.parquet"

# The row group size must be a table-level setting: the `File` sink writes with the
# format settings captured on the table, so a query-level SETTINGS clause on the
# INSERT would not reach the Parquet writer. Multiple row groups matter for the
# rewrite step below: a row group that matches the predicate is never recorded in the
# cache, so a single-row-group file with a matching row would leave nothing to cache.
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t_query_condition_cache"
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE t_query_condition_cache (b UInt64)
    ENGINE = File(Parquet, '${CLICKHOUSE_DATABASE}/04498_query_condition_cache.parquet')
    SETTINGS output_format_parquet_row_group_size = 100000
"

# All values are even, so the odd predicate value 3 never matches - the first query
# scans the surviving row groups, records them as non-matching, and the second query
# then skips them via the cache.
${CLICKHOUSE_CLIENT} --query "
    INSERT INTO t_query_condition_cache
    SELECT number * 2 FROM numbers(1000000)
"

# Backdate the file so its version token has settled and the cache engages.
touch -d '2020-01-01 00:00:00' "$DATA_FILE"

qid_first="${CLICKHOUSE_TEST_UNIQUE_NAME}_first"
qid_second="${CLICKHOUSE_TEST_UNIQUE_NAME}_second"

echo "first query result (expect 0):"
${CLICKHOUSE_CLIENT} --query_id="$qid_first" --query "
    SELECT count() FROM t_query_condition_cache WHERE b = 3 SETTINGS use_query_condition_cache = 1
"
echo "second query result (expect 0):"
${CLICKHOUSE_CLIENT} --query_id="$qid_second" --query "
    SELECT count() FROM t_query_condition_cache WHERE b = 3 SETTINGS use_query_condition_cache = 1
"

${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS query_log"

echo "first query was a cache miss (expect 1):"
${CLICKHOUSE_CLIENT} --query "
    SELECT ProfileEvents['QueryConditionCacheMisses'] > 0
    FROM system.query_log
    WHERE query_id = '$qid_first' AND current_database = currentDatabase() AND type = 'QueryFinish'
    ORDER BY event_time_microseconds DESC LIMIT 1
"
echo "first query was not a cache hit (expect 0):"
${CLICKHOUSE_CLIENT} --query "
    SELECT ProfileEvents['QueryConditionCacheHits'] > 0
    FROM system.query_log
    WHERE query_id = '$qid_first' AND current_database = currentDatabase() AND type = 'QueryFinish'
    ORDER BY event_time_microseconds DESC LIMIT 1
"
echo "second query was a cache hit (expect 1):"
${CLICKHOUSE_CLIENT} --query "
    SELECT ProfileEvents['QueryConditionCacheHits'] > 0
    FROM system.query_log
    WHERE query_id = '$qid_second' AND current_database = currentDatabase() AND type = 'QueryFinish'
    ORDER BY event_time_microseconds DESC LIMIT 1
"

# Correctness: a predicate that actually matches must still return the right rows
# once the cache is populated (no over-skipping of matching row groups).
echo "matching predicate, run 1 (expect 1):"
${CLICKHOUSE_CLIENT} --query "
    SELECT count() FROM t_query_condition_cache WHERE b = 400000 SETTINGS use_query_condition_cache = 1
"
echo "matching predicate, run 2 (expect 1):"
${CLICKHOUSE_CLIENT} --query "
    SELECT count() FROM t_query_condition_cache WHERE b = 400000 SETTINGS use_query_condition_cache = 1
"

# Cache invalidation on in-place rewrite. At this point the cache holds, for the current
# file version, the skip decision "no row group contains the value 3" (all data is even).
# Overwrite the table in place with data that now DOES contain 3.
${CLICKHOUSE_CLIENT} --query "
    INSERT INTO t_query_condition_cache
    SELECT number FROM numbers(1000000)
    SETTINGS engine_file_truncate_on_insert = 1
"

# Fail-close path: while the version token cannot be trusted (the mtime is too recent
# to prove that a same-tick rewrite is impossible - pinned here with a future mtime so
# the test is deterministic on any machine), the cache must be bypassed entirely: the
# query returns the correct (rewritten) data and records neither a hit nor a miss.
touch -d '2100-01-01 00:00:00' "$DATA_FILE"

qid_fresh="${CLICKHOUSE_TEST_UNIQUE_NAME}_fresh"

echo "rewritten file with an unsettled mtime, matching row is found (expect 1):"
${CLICKHOUSE_CLIENT} --query_id="$qid_fresh" --query "
    SELECT count() FROM t_query_condition_cache WHERE b = 3 SETTINGS use_query_condition_cache = 1
"

${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS query_log"

echo "unsettled-mtime query bypassed the cache, no miss recorded (expect 0):"
${CLICKHOUSE_CLIENT} --query "
    SELECT ProfileEvents['QueryConditionCacheMisses'] > 0
    FROM system.query_log
    WHERE query_id = '$qid_fresh' AND current_database = currentDatabase() AND type = 'QueryFinish'
    ORDER BY event_time_microseconds DESC LIMIT 1
"
echo "unsettled-mtime query bypassed the cache, no hit recorded (expect 0):"
${CLICKHOUSE_CLIENT} --query "
    SELECT ProfileEvents['QueryConditionCacheHits'] > 0
    FROM system.query_log
    WHERE query_id = '$qid_fresh' AND current_database = currentDatabase() AND type = 'QueryFinish'
    ORDER BY event_time_microseconds DESC LIMIT 1
"

# Once the rewritten file settles (distinct mtime from the first version), the same
# predicate must be a fresh cache miss and must scan the rewritten file, returning the
# matching row. A stale cache hit here would reuse the old "nothing matches" decision
# and wrongly return 0 - this is the regression this step guards against.
touch -d '2020-01-02 00:00:00' "$DATA_FILE"

qid_rewrite="${CLICKHOUSE_TEST_UNIQUE_NAME}_rewrite"
qid_recached="${CLICKHOUSE_TEST_UNIQUE_NAME}_recached"

echo "after the rewrite settles, matching row is found (expect 1):"
${CLICKHOUSE_CLIENT} --query_id="$qid_rewrite" --query "
    SELECT count() FROM t_query_condition_cache WHERE b = 3 SETTINGS use_query_condition_cache = 1
"
echo "settled rewritten file is served from the cache again (expect 1):"
${CLICKHOUSE_CLIENT} --query_id="$qid_recached" --query "
    SELECT count() FROM t_query_condition_cache WHERE b = 3 SETTINGS use_query_condition_cache = 1
"

${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS query_log"

echo "post-rewrite query was a cache miss (expect 1):"
${CLICKHOUSE_CLIENT} --query "
    SELECT ProfileEvents['QueryConditionCacheMisses'] > 0
    FROM system.query_log
    WHERE query_id = '$qid_rewrite' AND current_database = currentDatabase() AND type = 'QueryFinish'
    ORDER BY event_time_microseconds DESC LIMIT 1
"
echo "post-rewrite query was not a cache hit (expect 0):"
${CLICKHOUSE_CLIENT} --query "
    SELECT ProfileEvents['QueryConditionCacheHits'] > 0
    FROM system.query_log
    WHERE query_id = '$qid_rewrite' AND current_database = currentDatabase() AND type = 'QueryFinish'
    ORDER BY event_time_microseconds DESC LIMIT 1
"
echo "repeated post-rewrite query was a cache hit (expect 1):"
${CLICKHOUSE_CLIENT} --query "
    SELECT ProfileEvents['QueryConditionCacheHits'] > 0
    FROM system.query_log
    WHERE query_id = '$qid_recached' AND current_database = currentDatabase() AND type = 'QueryFinish'
    ORDER BY event_time_microseconds DESC LIMIT 1
"

${CLICKHOUSE_CLIENT} --query "DROP TABLE t_query_condition_cache"
rm -f "$DATA_FILE"
