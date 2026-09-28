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
# The same goes for a condition that a join's ON clause adds to that PREWHERE, for two PREWHEREs that differ
# only in a lambda's body, and for `formatRowNoNewline`, which writes its arguments' names into its result.
#
# Shell rather than SQL because the cache engages only once the file's version token has settled,
# which needs an explicit mtime - as in 04498_query_condition_cache_local_files.sh.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

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

# Through `f.v = a.v`, the ON condition `f.v + a.v >= 2048` becomes `v + v >= 2048` in the subquery's
# PREWHERE, which the subquery's WHERE does not contain. The rows are contiguous on purpose: a row
# group counts as matched once one row survives PREWHERE, so the row group holding every row of the
# plain read below must be emptied by PREWHERE alone.
LATE_FILE="${USER_FILES_PATH:?}/${CLICKHOUSE_DATABASE}/03229_qcc_late_prewhere.parquet"
${CLICKHOUSE_CLIENT} --query "
    DROP TABLE IF EXISTS t_qcc_late_file;
    CREATE TABLE t_qcc_late_file (k UInt64, v Int64, w Int64)
    ENGINE = File(Parquet, '${CLICKHOUSE_DATABASE}/03229_qcc_late_prewhere.parquet')
    SETTINGS output_format_parquet_row_group_size = 64;
    INSERT INTO t_qcc_late_file SELECT number, number, number FROM numbers(2000);
"
touch -d '2020-01-01 00:00:00' "$LATE_FILE"

LATE_JOIN="SELECT count() FROM t_qcc_late_file AS f RIGHT JOIN (SELECT * FROM t_qcc_late_file WHERE v + w < 100) AS a
    ON f.v = a.v AND f.v + a.v >= 2048 WHERE f.v > 5"
LATE_SETTINGS="$JOIN_SETTINGS, query_plan_convert_outer_join_to_inner_join = 1"
qid_late_plain="${CLICKHOUSE_TEST_UNIQUE_NAME}_late_plain"

echo "a condition derived from ON reaches the subquery's PREWHERE (expect 1):"
${CLICKHOUSE_CLIENT} --query "
    SELECT count() > 0 FROM (EXPLAIN actions = 1, pretty = 0 $LATE_JOIN
        SETTINGS $LATE_SETTINGS, query_plan_max_step_description_length = 1000)
    WHERE explain ILIKE '%prewhere filter column: %greaterOrEquals(plus(%v, %v), 2048\_%'
      AND explain NOT ILIKE '%\_\_applyFilter%'"
${CLICKHOUSE_CLIENT} --query "SYSTEM CLEAR QUERY CONDITION CACHE"
echo "join with the derived condition (expect 0):"
${CLICKHOUSE_CLIENT} --query "$LATE_JOIN SETTINGS $LATE_SETTINGS"
echo "plain read of the subquery's WHERE and v > 5, cache on (expect 44):"
${CLICKHOUSE_CLIENT} --query_id="$qid_late_plain" --query "
    SELECT count() FROM t_qcc_late_file WHERE v + w < 100 AND v > 5 SETTINGS use_query_condition_cache = 1"
echo "plain read of the subquery's WHERE and v > 5, cache off (expect 44):"
${CLICKHOUSE_CLIENT} --query "
    SELECT count() FROM t_qcc_late_file WHERE v + w < 100 AND v > 5 SETTINGS use_query_condition_cache = 0"

# A PREWHERE with `IN` that the hash covers keeps populating the cache, like the `=` above.
qid_in_miss="${CLICKHOUSE_TEST_UNIQUE_NAME}_in_miss"
qid_in_hit="${CLICKHOUSE_TEST_UNIQUE_NAME}_in_hit"
IN_QUERY="SELECT sum(k) FROM t_qcc_jrf_file WHERE val IN (5001, 7001)"

echo "PREWHERE with IN reaches the file read (expect 1):"
${CLICKHOUSE_CLIENT} --query "
    SELECT count() > 0 FROM (EXPLAIN actions = 1, pretty = 0 $IN_QUERY
        SETTINGS $DET_SETTINGS, query_plan_max_step_description_length = 1000)
    WHERE explain ILIKE '%prewhere filter column: %in(%val, %'"
echo "PREWHERE with IN, run 1 (expect 12002):"
${CLICKHOUSE_CLIENT} --query_id="$qid_in_miss" --query "$IN_QUERY SETTINGS $DET_SETTINGS"
echo "PREWHERE with IN, run 2 (expect 12002):"
${CLICKHOUSE_CLIENT} --query_id="$qid_in_hit" --query "$IN_QUERY SETTINGS $DET_SETTINGS"

# A PREWHERE that applies a function to another function's result keeps populating the cache, like the
# `=` and `IN` above.
qid_nested_miss="${CLICKHOUSE_TEST_UNIQUE_NAME}_nested_miss"
qid_nested_hit="${CLICKHOUSE_TEST_UNIQUE_NAME}_nested_hit"
NESTED_QUERY="SELECT sum(k) FROM t_qcc_jrf_file WHERE startsWith(toString(val), '5001')"

echo "PREWHERE with a nested function reaches the file read (expect 1):"
${CLICKHOUSE_CLIENT} --query "
    SELECT count() > 0 FROM (EXPLAIN actions = 1, pretty = 0 $NESTED_QUERY
        SETTINGS $DET_SETTINGS, query_plan_max_step_description_length = 1000)
    WHERE explain ILIKE '%prewhere filter column: %startsWith(toString(%val), %'"
echo "PREWHERE with a nested function, run 1 (expect 5001):"
${CLICKHOUSE_CLIENT} --query_id="$qid_nested_miss" --query "$NESTED_QUERY SETTINGS $DET_SETTINGS"
echo "PREWHERE with a nested function, run 2 (expect 5001):"
${CLICKHOUSE_CLIENT} --query_id="$qid_nested_hit" --query "$NESTED_QUERY SETTINGS $DET_SETTINGS"

# A PREWHERE with a constant lambda keeps populating the cache, like the `=` and `IN` above.
qid_const_lambda_miss="${CLICKHOUSE_TEST_UNIQUE_NAME}_const_lambda_miss"
qid_const_lambda_hit="${CLICKHOUSE_TEST_UNIQUE_NAME}_const_lambda_hit"
CONST_LAMBDA_QUERY="SELECT sum(k) FROM t_qcc_jrf_file WHERE arrayExists(x -> x > 5000 AND x < 5002, [val])"

echo "PREWHERE with a constant lambda reaches the file read (expect 1):"
${CLICKHOUSE_CLIENT} --query "
    SELECT count() > 0 FROM (EXPLAIN actions = 1, pretty = 0 $CONST_LAMBDA_QUERY
        SETTINGS $DET_SETTINGS, query_plan_max_step_description_length = 1000)
    WHERE explain ILIKE '%prewhere filter column: %arrayExists(x UInt64 -> %'"
echo "PREWHERE with a constant lambda, run 1 (expect 5001):"
${CLICKHOUSE_CLIENT} --query_id="$qid_const_lambda_miss" --query "$CONST_LAMBDA_QUERY SETTINGS $DET_SETTINGS"
echo "PREWHERE with a constant lambda, run 2 (expect 5001):"
${CLICKHOUSE_CLIENT} --query_id="$qid_const_lambda_hit" --query "$CONST_LAMBDA_QUERY SETTINGS $DET_SETTINGS"

# formatRowNoNewline writes its arguments' names into its result, so the ON condition's copy in the
# subquery's PREWHERE removes every row although it reads the same as the subquery's own condition.
FMT_JOIN="SELECT count() FROM t_qcc_late_file AS f RIGHT JOIN
    (SELECT * FROM t_qcc_late_file WHERE v + w < 100 AND formatRowNoNewline('JSONEachRow', v + v) LIKE '%\"plus(v, v)\"%') AS a
    ON f.v = a.v AND formatRowNoNewline('JSONEachRow', f.v + a.v) LIKE '%\"plus(v, v)\"%' WHERE f.v > 5"
FMT_PLAIN="SELECT count() FROM t_qcc_late_file
    WHERE (v + w < 100 AND formatRowNoNewline('JSONEachRow', v + v) LIKE '%\"plus(v, v)\"%') AND v > 5"

echo "formatRowNoNewline from ON reaches the subquery's PREWHERE (expect 1):"
${CLICKHOUSE_CLIENT} --query "
    SELECT count() > 0 FROM (EXPLAIN actions = 1, pretty = 0 $FMT_JOIN
        SETTINGS $LATE_SETTINGS, query_plan_max_step_description_length = 1000)
    WHERE explain ILIKE '%prewhere filter column: %formatRowNoNewline(%formatRowNoNewline(%'"
${CLICKHOUSE_CLIENT} --query "SYSTEM CLEAR QUERY CONDITION CACHE"
echo "join with formatRowNoNewline in ON (expect 0):"
${CLICKHOUSE_CLIENT} --query "$FMT_JOIN SETTINGS $LATE_SETTINGS"
echo "plain read of the subquery's WHERE with formatRowNoNewline and v > 5, cache on (expect 44):"
${CLICKHOUSE_CLIENT} --query "$FMT_PLAIN SETTINGS use_query_condition_cache = 1"
echo "plain read of the subquery's WHERE with formatRowNoNewline and v > 5, cache off (expect 44):"
${CLICKHOUSE_CLIENT} --query "$FMT_PLAIN SETTINGS use_query_condition_cache = 0"

# Two reads whose PREWHERE differs only in a lambda's body must not share an entry: the first keeps two row
# groups, the second keeps every row.
LAMBDA_EQ="SELECT sum(k) FROM t_qcc_jrf_file WHERE arrayExists(x -> x = val, [5001, 7001])"
LAMBDA_NE="SELECT sum(k) FROM t_qcc_jrf_file WHERE arrayExists(x -> x != val, [5001, 7001])"
echo "PREWHERE with a lambda reaches the file read (expect 1):"
${CLICKHOUSE_CLIENT} --query "
    SELECT count() > 0 FROM (EXPLAIN actions = 1, pretty = 0 $LAMBDA_NE
        SETTINGS $DET_SETTINGS, query_plan_max_step_description_length = 1000)
    WHERE explain ILIKE '%prewhere filter column: %arrayExists(%'"
${CLICKHOUSE_CLIENT} --query "SYSTEM CLEAR QUERY CONDITION CACHE"
echo "lambda x = val (expect 12002):"
${CLICKHOUSE_CLIENT} --query "$LAMBDA_EQ SETTINGS $DET_SETTINGS"
echo "lambda x != val after it, cache on (expect 49995000):"
${CLICKHOUSE_CLIENT} --query "$LAMBDA_NE SETTINGS $DET_SETTINGS"
echo "lambda x != val, cache off (expect 49995000):"
${CLICKHOUSE_CLIENT} --query "$LAMBDA_NE SETTINGS use_query_condition_cache = 0, optimize_move_to_prewhere = 1, query_plan_optimize_prewhere = 1"

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
# The join recorded nothing under this key, so the plain read had nothing to hit.
echo "plain read after the join with the derived condition was a cache miss (expect 1):"
profile_event "$qid_late_plain" QueryConditionCacheMisses
echo "PREWHERE with IN run 1 was a cache miss (expect 1):"
profile_event "$qid_in_miss" QueryConditionCacheMisses
echo "PREWHERE with IN run 2 was a cache hit (expect 1):"
profile_event "$qid_in_hit" QueryConditionCacheHits
echo "PREWHERE with a nested function run 1 was a cache miss (expect 1):"
profile_event "$qid_nested_miss" QueryConditionCacheMisses
echo "PREWHERE with a nested function run 2 was a cache hit (expect 1):"
profile_event "$qid_nested_hit" QueryConditionCacheHits
echo "PREWHERE with a constant lambda run 1 was a cache miss (expect 1):"
profile_event "$qid_const_lambda_miss" QueryConditionCacheMisses
echo "PREWHERE with a constant lambda run 2 was a cache hit (expect 1):"
profile_event "$qid_const_lambda_hit" QueryConditionCacheHits

${CLICKHOUSE_CLIENT} --query "DROP TABLE t_qcc_jrf_file; DROP TABLE t_qcc_jrf_dim; DROP TABLE t_qcc_late_file"
rm -f "$DATA_FILE" "$LATE_FILE"
