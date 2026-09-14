#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel
# Tag no-fasttest: needs Parquet
# Tag no-parallel: asserts query condition cache profile events. A parallel sibling test dropping or
# renaming any UUID-less (path-keyed) `MergeTree` table calls `Context::clearCaches`, which wipes the
# query condition cache and turns an expected hit into a miss.

# Companion of 03229_query_condition_cache_join_runtime_filter.sql for the second writer of the same
# cache: the format path (`FormatFilterInfo`), where a join runtime filter also reaches the read's
# PREWHERE. A row group that the PREWHERE leaves without rows is recorded as matching nothing, but
# the entry is keyed on `filter_actions_dag`, which the runtime filter is added to PREWHERE after -
# so a later plain read of the same file loses the rows only the runtime filter had removed.
#
# Shell rather than SQL because the cache engages only once the file's version token has settled,
# which needs an explicit mtime - as in 04498_query_condition_cache_local_files.sh.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `enable_analyzer` has to be a session value, not a per-query one: a subquery may not change it
# relative to the top level, and the plan assertions below read `EXPLAIN` from a subquery, so a
# query-level pin fails outright wherever the session value is 0 (as in the `old analyzer` lane).
CLICKHOUSE_CLIENT="${CLICKHOUSE_CLIENT} --enable_analyzer=1"

DATA_FILE="${USER_FILES_PATH:?}/${CLICKHOUSE_DATABASE}/03229_qcc_join_runtime_filter.parquet"

# The row group size must be a table-level setting: the `File` sink writes with the format settings
# captured on the table, so a query-level SETTINGS clause would not reach the Parquet writer. Ten row
# groups matter - a row group the predicate matches is never recorded, so a single-row-group file
# would leave nothing to cache.
#
# `k = 5000` sits in row group 5 and `5000 % 7 = 2`, so the one row the runtime filter keeps is then
# rejected by the probe-side predicate and every row group ends up empty for that read, while the
# hashed predicate alone matches 1429 rows spread over all ten.
${CLICKHOUSE_CLIENT} --query "
    DROP TABLE IF EXISTS t_qcc_jrf_file;
    DROP TABLE IF EXISTS t_qcc_jrf_dim;
    CREATE TABLE t_qcc_jrf_file (k UInt64, val UInt64)
    ENGINE = File(Parquet, '${CLICKHOUSE_DATABASE}/03229_qcc_join_runtime_filter.parquet')
    SETTINGS output_format_parquet_row_group_size = 1000;
    CREATE TABLE t_qcc_jrf_dim (k UInt64) ENGINE = MergeTree ORDER BY k;
    INSERT INTO t_qcc_jrf_dim VALUES (5000);
    INSERT INTO t_qcc_jrf_file SELECT number, number FROM numbers(10000);
"

# Backdate the file so its version token has settled and the cache engages at all.
touch -d '2020-01-01 00:00:00' "$DATA_FILE"

JOIN_QUERY="SELECT count() FROM t_qcc_jrf_file AS p, t_qcc_jrf_dim AS d WHERE p.k = d.k AND p.val % 7 = 3"
JOIN_SETTINGS="use_query_condition_cache = 1, enable_join_runtime_filters = 1,
    join_runtime_filter_min_probe_rows = 0, join_algorithm = 'hash,parallel_hash',
    query_plan_join_swap_table = 0, optimize_move_to_prewhere = 1, query_plan_optimize_prewhere = 1"
NO_RF_SETTINGS="use_query_condition_cache = 1, enable_join_runtime_filters = 0,
    join_algorithm = 'hash,parallel_hash', query_plan_join_swap_table = 0,
    optimize_move_to_prewhere = 1, query_plan_optimize_prewhere = 1"

# The plan assertions pin the shape the row counts depend on, so that a decline (no runtime filter
# planted, or one not moved into PREWHERE) cannot pass as a fix. `pretty = 0` is required because the
# pretty renderer replaces the filter's column name with an annotation, and that name is the anchor.
echo "runtime filter reaches the file read's PREWHERE (expect 1):"
${CLICKHOUSE_CLIENT} --query "
    SELECT count() > 0 FROM (EXPLAIN actions = 1, pretty = 0 $JOIN_QUERY
        SETTINGS $JOIN_SETTINGS, query_plan_max_step_description_length = 1000)
    WHERE explain ILIKE '%prewhere filter column: %__applyfilter(%'"

echo "without runtime filters the same anchor is absent (expect 0):"
${CLICKHOUSE_CLIENT} --query "
    SELECT count() > 0 FROM (EXPLAIN actions = 1, pretty = 0 $JOIN_QUERY
        SETTINGS $NO_RF_SETTINGS, query_plan_max_step_description_length = 1000)
    WHERE explain ILIKE '%prewhere filter column: %__applyfilter(%'"

# Liveness of the fixture itself: without this, an unsettled version token or a Parquet reader that
# reports no buckets would make every assertion below pass for the wrong reason.
qid_live_miss="${CLICKHOUSE_TEST_UNIQUE_NAME}_live_miss"
qid_live_hit="${CLICKHOUSE_TEST_UNIQUE_NAME}_live_hit"

echo "cache engages on this file, run 1 (expect 1):"
${CLICKHOUSE_CLIENT} --query_id="$qid_live_miss" --query "
    SELECT count() FROM t_qcc_jrf_file WHERE k = 5000 SETTINGS use_query_condition_cache = 1"
echo "cache engages on this file, run 2 (expect 1):"
${CLICKHOUSE_CLIENT} --query_id="$qid_live_hit" --query "
    SELECT count() FROM t_qcc_jrf_file WHERE k = 5000 SETTINGS use_query_condition_cache = 1"

# The probe above uses a different predicate and no join, so it leaves open whether the shape the arm
# below reads with reaches the cache at all. The same join with runtime filters off settles it: a read
# that never computes a cache key would otherwise be indistinguishable from one the gate declined.
qid_norf_join="${CLICKHOUSE_TEST_UNIQUE_NAME}_norf_join"

echo "the same join without runtime filters (expect 0):"
${CLICKHOUSE_CLIENT} --query_id="$qid_norf_join" --query "$JOIN_QUERY SETTINGS $NO_RF_SETTINGS"

# Everything below asserts cache state, so start it from an empty cache instead of from whatever the
# reads above recorded.
${CLICKHOUSE_CLIENT} --query "SYSTEM CLEAR QUERY CONDITION CACHE"

# The runtime-filter query must not populate the cache. Its own read records neither a miss nor a
# hit, because a read that may not write an entry never computes the key it would probe with.
qid_join="${CLICKHOUSE_TEST_UNIQUE_NAME}_join"

echo "join with a runtime filter (expect 0):"
${CLICKHOUSE_CLIENT} --query_id="$qid_join" --query "$JOIN_QUERY SETTINGS $JOIN_SETTINGS"

# Both counts are printed rather than a boolean, so a reference diff shows which way it broke.
echo "plain read of the same predicate, cache on (expect 1429):"
${CLICKHOUSE_CLIENT} --query "
    SELECT count() FROM t_qcc_jrf_file WHERE val % 7 = 3 SETTINGS use_query_condition_cache = 1"
echo "plain read of the same predicate, cache off (expect 1429):"
${CLICKHOUSE_CLIENT} --query "
    SELECT count() FROM t_qcc_jrf_file WHERE val % 7 = 3 SETTINGS use_query_condition_cache = 0"

# A PREWHERE the hash does cover must keep populating the cache, or the gate above is simply
# switching file-level caching off. `val` is the only column the predicate names and `k` is read on
# top of it, which is what lets the condition move into the read's PREWHERE at all.
qid_det_miss="${CLICKHOUSE_TEST_UNIQUE_NAME}_det_miss"
qid_det_hit="${CLICKHOUSE_TEST_UNIQUE_NAME}_det_hit"
DET_QUERY="SELECT sum(k) FROM t_qcc_jrf_file WHERE val = 5001"
DET_SETTINGS="use_query_condition_cache = 1, optimize_move_to_prewhere = 1, query_plan_optimize_prewhere = 1"

echo "deterministic PREWHERE reaches the file read (expect 1):"
${CLICKHOUSE_CLIENT} --query "
    SELECT count() > 0 FROM (EXPLAIN actions = 1, pretty = 0 $DET_QUERY
        SETTINGS $DET_SETTINGS, query_plan_max_step_description_length = 1000)
    WHERE explain ILIKE '%prewhere filter column: %equals(%val, 5001%'"
echo "deterministic PREWHERE, run 1 (expect 5001):"
${CLICKHOUSE_CLIENT} --query_id="$qid_det_miss" --query "$DET_QUERY SETTINGS $DET_SETTINGS"
echo "deterministic PREWHERE, run 2 (expect 5001):"
${CLICKHOUSE_CLIENT} --query_id="$qid_det_hit" --query "$DET_QUERY SETTINGS $DET_SETTINGS"

${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS query_log"

profile_event() {
    ${CLICKHOUSE_CLIENT} --query "
        SELECT ProfileEvents['$2'] > 0
        FROM system.query_log
        WHERE query_id = '$1' AND current_database = currentDatabase() AND type = 'QueryFinish'
        ORDER BY event_time_microseconds DESC LIMIT 1"
}

echo "first plain read was a cache miss (expect 1):"
profile_event "$qid_live_miss" QueryConditionCacheMisses
echo "second plain read was a cache hit (expect 1):"
profile_event "$qid_live_hit" QueryConditionCacheHits
echo "the join's own predicate is cache-eligible without a runtime filter (expect 1):"
profile_event "$qid_norf_join" QueryConditionCacheMisses
echo "the runtime-filter query recorded no cache miss (expect 0):"
profile_event "$qid_join" QueryConditionCacheMisses
echo "the runtime-filter query recorded no cache hit (expect 0):"
profile_event "$qid_join" QueryConditionCacheHits
# `FunctionApplyFilter` passes every row while the filter is unbuilt, and in that state no row group is
# emptied, so nothing above can fail. These counters pin that premise: the filter was consulted and it
# did remove rows. The same join without runtime filters is the negative control for the first of them.
echo "the runtime filter was consulted (expect 1):"
profile_event "$qid_join" RuntimeFilterRowsChecked
echo "the runtime filter removed rows (expect 1):"
${CLICKHOUSE_CLIENT} --query "
    SELECT ProfileEvents['RuntimeFilterRowsPassed'] < ProfileEvents['RuntimeFilterRowsChecked']
    FROM system.query_log
    WHERE query_id = '$qid_join' AND current_database = currentDatabase() AND type = 'QueryFinish'
    ORDER BY event_time_microseconds DESC LIMIT 1"
echo "without runtime filters the same counter is absent (expect 0):"
profile_event "$qid_norf_join" RuntimeFilterRowsChecked
echo "deterministic PREWHERE run 1 was a cache miss (expect 1):"
profile_event "$qid_det_miss" QueryConditionCacheMisses
echo "deterministic PREWHERE run 2 was a cache hit (expect 1):"
profile_event "$qid_det_hit" QueryConditionCacheHits

${CLICKHOUSE_CLIENT} --query "DROP TABLE t_qcc_jrf_file; DROP TABLE t_qcc_jrf_dim"
rm -f "$DATA_FILE"
