-- Tags: no-parallel
-- Tag no-parallel: Messes with internal cache

-- Tests that a join runtime filter sitting in a read step's PREWHERE does not populate the query
-- condition cache, and that the two filters which legitimately may sit there still do.

SET enable_parallel_replicas = 0;
SET parallel_replicas_local_plan = 1;
SET use_query_condition_cache = 1;

DROP VIEW IF EXISTS v_tab;
DROP TABLE IF EXISTS tab;
DROP TABLE IF EXISTS tab_topk;
DROP TABLE IF EXISTS tab_widened;
DROP TABLE IF EXISTS dim;

CREATE TABLE tab (k Int32, val Int64) ENGINE = MergeTree ORDER BY k
SETTINGS index_granularity = 8192, auto_statistics_types = 'uniq,tdigest',
         add_minmax_index_for_numeric_columns = 0;
CREATE TABLE dim (k Int32) ENGINE = MergeTree ORDER BY k;

INSERT INTO tab SELECT number, number FROM numbers(500000) SETTINGS materialize_statistics_on_insert = 1;
-- `k = 1` does not satisfy the view's filter, so the join below returns nothing. That is the point:
-- the runtime filter leaves exactly that one row of the granule, the view's filter then rejects it,
-- and the granule looks empty to a filter which never saw the rows the runtime filter removed.
INSERT INTO dim VALUES (1);

-- The filter references every column the read produces, which is what stops
-- `MergeTreeWhereOptimizer` from moving it into PREWHERE next to the runtime filter.
CREATE VIEW v_tab AS SELECT k, val FROM tab HAVING (k + val) % 7 = 3;

SELECT '-- a join runtime filter in PREWHERE must not populate the cache';
-- The two plan assertions pin the shape the row counts below depend on: the runtime filter in the
-- read's PREWHERE, and a residual filter still above the read. `pretty = 0` is required because the
-- pretty renderer replaces a filter's column name with an annotation, and these anchors are that
-- name. `trimLeft` anchors the second one at the start of its line, so `Prewhere filter column:`
-- cannot satisfy it. They run before the CLEAR because an EXPLAIN may write cache entries of its own.
SELECT count() > 0 FROM (EXPLAIN actions = 1, pretty = 0 SELECT count() FROM v_tab, dim WHERE v_tab.k = dim.k
    SETTINGS enable_join_runtime_filters = 1, enable_join_runtime_filters_index_analysis = 0,
             join_runtime_filter_min_probe_rows = 0,
             join_algorithm = 'hash,parallel_hash', use_statistics = 1, query_plan_join_swap_table = 0,
             optimize_move_to_prewhere = 1, query_plan_optimize_prewhere = 1,
             query_plan_max_step_description_length = 1000)
WHERE explain ILIKE '%prewhere filter column: %__applyfilter(%';
SELECT count() > 0 FROM (EXPLAIN actions = 1, pretty = 0 SELECT count() FROM v_tab, dim WHERE v_tab.k = dim.k
    SETTINGS enable_join_runtime_filters = 1, enable_join_runtime_filters_index_analysis = 0,
             join_runtime_filter_min_probe_rows = 0,
             join_algorithm = 'hash,parallel_hash', use_statistics = 1, query_plan_join_swap_table = 0,
             optimize_move_to_prewhere = 1, query_plan_optimize_prewhere = 1,
             query_plan_max_step_description_length = 1000)
WHERE trimLeft(explain) ILIKE 'Filter column: %modulo(plus(%k, %val), 7\_%';
SYSTEM CLEAR QUERY CONDITION CACHE;
SELECT count() FROM v_tab, dim WHERE v_tab.k = dim.k
SETTINGS enable_join_runtime_filters = 1, enable_join_runtime_filters_index_analysis = 0,
         join_runtime_filter_min_probe_rows = 0,
         join_algorithm = 'hash,parallel_hash', use_statistics = 1, query_plan_join_swap_table = 0,
         optimize_move_to_prewhere = 1, query_plan_optimize_prewhere = 1, log_comment = 'qcc_jrf_join';
SELECT count() FROM system.query_condition_cache;
-- Print both counts rather than a boolean, so a reference diff shows which way it broke.
SELECT count() FROM v_tab SETTINGS use_query_condition_cache = 1;
SELECT count() FROM v_tab SETTINGS use_query_condition_cache = 0;
-- `FunctionApplyFilter` passes every row while the filter is unbuilt, and in that state nothing is
-- emptied and nothing is poisoned, yet every assertion above still holds. These counters pin that
-- premise: the filter was consulted (`Checked > 0`) and did remove rows (`Passed < Checked`).
SYSTEM FLUSH LOGS query_log;
SELECT ProfileEvents['RuntimeFilterRowsChecked'] > 0
FROM system.query_log
WHERE current_database = currentDatabase() AND log_comment = 'qcc_jrf_join' AND type = 'QueryFinish'
ORDER BY event_time_microseconds DESC LIMIT 1;
SELECT ProfileEvents['RuntimeFilterRowsPassed'] < ProfileEvents['RuntimeFilterRowsChecked']
FROM system.query_log
WHERE current_database = currentDatabase() AND log_comment = 'qcc_jrf_join' AND type = 'QueryFinish'
ORDER BY event_time_microseconds DESC LIMIT 1;

SELECT '-- control: without runtime filters the same query leaves the view readable';
-- Same anchor as arm 1, runtime filters off: it must not match. This is what makes that assertion an
-- oracle rather than a constant, and it is why the equal row counts below mean anything.
SELECT count() > 0 FROM (EXPLAIN actions = 1, pretty = 0 SELECT count() FROM v_tab, dim WHERE v_tab.k = dim.k
    SETTINGS enable_join_runtime_filters = 0,
             join_algorithm = 'hash,parallel_hash', use_statistics = 1, query_plan_join_swap_table = 0,
             optimize_move_to_prewhere = 1, query_plan_optimize_prewhere = 1,
             query_plan_max_step_description_length = 1000)
WHERE explain ILIKE '%prewhere filter column: %__applyfilter(%';
SYSTEM CLEAR QUERY CONDITION CACHE;
SELECT count() FROM v_tab, dim WHERE v_tab.k = dim.k
SETTINGS enable_join_runtime_filters = 0, enable_join_runtime_filters_index_analysis = 0,
         join_algorithm = 'hash,parallel_hash', use_statistics = 1, query_plan_join_swap_table = 0,
         optimize_move_to_prewhere = 1, query_plan_optimize_prewhere = 1, log_comment = 'qcc_jrf_join_norf';
SELECT count() FROM v_tab SETTINGS use_query_condition_cache = 1;
SELECT count() FROM v_tab SETTINGS use_query_condition_cache = 0;
-- No runtime filter here, so the counters arm 1 asserts on must be absent. This is what makes those
-- two assertions an oracle rather than a pair of constants.
SYSTEM FLUSH LOGS query_log;
SELECT ProfileEvents['RuntimeFilterRowsChecked'] > 0
FROM system.query_log
WHERE current_database = currentDatabase() AND log_comment = 'qcc_jrf_join_norf' AND type = 'QueryFinish'
ORDER BY event_time_microseconds DESC LIMIT 1;

SELECT '-- control: a runtime filter left above the read is rejected as before';
SYSTEM CLEAR QUERY CONDITION CACHE;
SELECT count() FROM v_tab, dim WHERE v_tab.k = dim.k
SETTINGS enable_join_runtime_filters = 1, join_runtime_filter_min_probe_rows = 0,
         join_algorithm = 'hash,parallel_hash', use_statistics = 1, query_plan_join_swap_table = 0,
         optimize_move_to_prewhere = 0, query_plan_optimize_prewhere = 0;
SELECT count() FROM system.query_condition_cache;
SELECT count() FROM v_tab SETTINGS use_query_condition_cache = 1;
SELECT count() FROM v_tab SETTINGS use_query_condition_cache = 0;

SELECT '-- control: a runtime filter driving index analysis on data read is rejected too';
-- `enable_join_runtime_filters_index_analysis` prunes marks in the readers chain rather than through
-- PREWHERE, so the rows it removes are invisible to every filter the arms above inspect. The two
-- counters are what stop the cache count from being the output of a query that pruned nothing.
SYSTEM CLEAR QUERY CONDITION CACHE;
SELECT count() FROM v_tab, dim WHERE v_tab.k = dim.k
SETTINGS enable_join_runtime_filters = 1, enable_join_runtime_filters_index_analysis = 1,
         use_skip_indexes_on_data_read = 1, join_runtime_filter_min_probe_rows = 0,
         join_algorithm = 'hash,parallel_hash', use_statistics = 1, query_plan_join_swap_table = 0,
         optimize_move_to_prewhere = 1, query_plan_optimize_prewhere = 1, log_comment = 'qcc_jrf_index_scan';
SELECT count() FROM system.query_condition_cache;
SELECT count() FROM v_tab SETTINGS use_query_condition_cache = 1;
SELECT count() FROM v_tab SETTINGS use_query_condition_cache = 0;
SYSTEM FLUSH LOGS query_log;
SELECT toInt32(ProfileEvents['SelectedMarks']) < toInt32(ProfileEvents['SelectedMarksTotal']),
       ProfileEvents['RuntimeFilterRowsChecked'] > 0
FROM system.query_log
WHERE current_database = currentDatabase() AND log_comment = 'qcc_jrf_index_scan' AND type = 'QueryFinish'
ORDER BY event_time_microseconds DESC LIMIT 1;

-- The PREWHERE below matches every row, so the only thing that can prune marks on the second run is
-- the cache entry the WHERE filter wrote. Both runs return 1 row; only the second one prunes.
SELECT '-- a deterministic PREWHERE must not stop the WHERE filter from populating the cache';
SYSTEM CLEAR QUERY CONDITION CACHE;
SELECT count() FROM tab PREWHERE k < 500000 WHERE (k + val) = 24690
SETTINGS optimize_move_to_prewhere = 1, query_plan_optimize_prewhere = 1, log_comment = 'qcc_jrf_prewhere';
SELECT count() FROM tab PREWHERE k < 500000 WHERE (k + val) = 24690
SETTINGS optimize_move_to_prewhere = 1, query_plan_optimize_prewhere = 1, log_comment = 'qcc_jrf_prewhere';
SYSTEM FLUSH LOGS query_log;
SELECT toInt32(ProfileEvents['SelectedMarks']) < toInt32(ProfileEvents['SelectedMarksTotal'])
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600 AND type = 'QueryFinish'
  AND current_database = currentDatabase() AND log_comment = 'qcc_jrf_prewhere'
-- The unpruned run selected more marks, which orders the two rows even if they share a microsecond.
ORDER BY event_time_microseconds, ProfileEvents['SelectedMarks'] DESC;

-- `__topKFilter` reaches PREWHERE only for a read that has none yet, hence a table of its own. The
-- read must also produce more than one block, because the threshold that fills that PREWHERE exists
-- only once an earlier block has been sorted; `max_block_size` is therefore pinned well below the row
-- count, since a read the size of the whole part is filtered by nothing and records nothing.
SELECT '-- `__topKFilter` in PREWHERE must still populate the cache';
CREATE TABLE tab_topk (k UInt32, v1 UInt32, v2 UInt32) ENGINE = MergeTree ORDER BY k
SETTINGS index_granularity = 64, min_bytes_for_wide_part = 0, add_minmax_index_for_numeric_columns = 0;
INSERT INTO tab_topk SELECT number, number, number FROM numbers(100000);
-- `__topKFilter` actually reaching PREWHERE is what this arm exists to exercise: the cache count below
-- reads 1 either way, because without it the read has no PREWHERE and the WHERE filter is tagged as
-- before.
SELECT count() > 0 FROM (EXPLAIN actions = 1, pretty = 0 SELECT v1 FROM tab_topk WHERE v2 = 10000 ORDER BY v1 ASC LIMIT 5
    SETTINGS use_query_condition_cache_for_top_k = 1, use_top_k_dynamic_filtering = 1,
             use_skip_indexes_for_top_k = 1, query_plan_max_limit_for_top_k_optimization = 1000,
             optimize_move_to_prewhere = 0, max_block_size = 4096,
             query_plan_max_step_description_length = 1000)
WHERE explain ILIKE '%prewhere filter column: %__topkfilter(%';
SYSTEM CLEAR QUERY CONDITION CACHE;
SELECT v1 FROM tab_topk WHERE v2 = 10000 ORDER BY v1 ASC LIMIT 5
SETTINGS use_query_condition_cache_for_top_k = 1, use_top_k_dynamic_filtering = 1,
         use_skip_indexes_for_top_k = 1, query_plan_max_limit_for_top_k_optimization = 1000,
         optimize_move_to_prewhere = 0, max_block_size = 4096
FORMAT Null;
SELECT count() > 0 FROM system.query_condition_cache;

SELECT '-- a filter widened after index analysis must not populate the cache';
-- Push-down merges the self-join's always-false `ON` conjunct into the very filter the read's
-- `filter_actions_dag` was built from, so the granule that filter empties still holds the 194 rows that
-- `v > 5` matches. The plan assertion pins that merge on the build side, which is the side that carries
-- the key: the probe side also gets `__applyFilter`, and a filter holding it is rejected as
-- non-deterministic before any of this matters. `query_plan_convert_outer_join_to_inner_join` is pinned
-- because the push-down happens only once the `RIGHT JOIN` has become an `INNER` one: with it off the
-- filter is never widened, and the arm would pass without exercising anything.
CREATE TABLE tab_widened (v Int64) ENGINE = MergeTree ORDER BY v SETTINGS index_granularity = 8192;
INSERT INTO tab_widened SELECT number FROM numbers(200);
SELECT count() > 0 FROM (EXPLAIN actions = 1, pretty = 0
    SELECT count() FROM tab_widened LOCAL RIGHT JOIN tab_widened AS a
        ON and(equals(v, a.v), not(equals(v, a.v))) WHERE v > 5
    SETTINGS enable_join_runtime_filters = 1, enable_join_runtime_filters_index_analysis = 1,
             join_runtime_filter_min_probe_rows = 0, query_plan_convert_outer_join_to_inner_join = 1,
             query_plan_max_step_description_length = 1000)
WHERE trimLeft(explain) ILIKE 'Filter column: and(greater(%v, 5\_%), not(equals(%'
  AND explain NOT ILIKE '%\_\_applyFilter%';
SYSTEM CLEAR QUERY CONDITION CACHE;
SELECT count() FROM tab_widened WHERE v > 5;
-- Returns 0 legitimately, and must keep doing so: the fix withholds a cache entry, it does not change
-- what the join computes.
SELECT count() FROM tab_widened LOCAL RIGHT JOIN tab_widened AS a
    ON and(equals(v, a.v), not(equals(v, a.v))) WHERE v > 5
SETTINGS enable_join_runtime_filters = 1, enable_join_runtime_filters_index_analysis = 1,
         join_runtime_filter_min_probe_rows = 0, query_plan_convert_outer_join_to_inner_join = 1;
-- Print both counts rather than a boolean, so a reference diff shows which way it broke.
SELECT count() FROM tab_widened WHERE v > 5 SETTINGS use_query_condition_cache = 1;
SELECT count() FROM tab_widened WHERE v > 5 SETTINGS use_query_condition_cache = 0;
SELECT count() FROM system.query_condition_cache;

SELECT '-- control: with no runtime filter the same join leaves the table readable';
-- The re-walk that re-annotates a rebuilt filter step runs only when runtime filters were added, so
-- this arm reads 194 with or without the fix. That is what makes the arm above an oracle rather than a
-- pair of constants.
SYSTEM CLEAR QUERY CONDITION CACHE;
SELECT count() FROM tab_widened LOCAL RIGHT JOIN tab_widened AS a
    ON and(equals(v, a.v), not(equals(v, a.v))) WHERE v > 5
SETTINGS enable_join_runtime_filters = 0, query_plan_convert_outer_join_to_inner_join = 1;
SELECT count() FROM tab_widened WHERE v > 5 SETTINGS use_query_condition_cache = 1;
SELECT count() FROM system.query_condition_cache;

DROP VIEW v_tab;
DROP TABLE dim;
DROP TABLE tab_topk;
DROP TABLE tab_widened;
DROP TABLE tab;
