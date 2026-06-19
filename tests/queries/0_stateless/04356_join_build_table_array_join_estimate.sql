-- Tags: no-parallel-replicas

-- The build-side choice ('auto') estimates each input's row count from the underlying scan's
-- `selected_rows`. That number is only valid if every step between the scan and the join can
-- only keep or reduce the row count. An `arrayJoin` multiplies rows by an unknown factor, so
-- the scan count is neither an estimate nor an upper bound for the exploded subtree and must be
-- discarded -- otherwise the optimizer would under-count the exploded side and wrongly build the
-- hash table on it.
--
-- `arr_tbl` has 100 base rows, each holding a 1000-element array, so the `arrayJoin` explodes it
-- to 100000 rows -- larger than `big` (50000). The hash table must therefore be built on `big`,
-- not on the exploded side. Covered for both the `arrayJoin` function and the `ARRAY JOIN`
-- clause, since they are represented differently in the plan (an expression action vs a
-- dedicated `ArrayJoinStep`).

-- Build-side-choice determinism. `query_plan_join_swap_table = 'auto'` is under test, and
-- clickhouse-test randomizes several settings that change which table is built; pin them so the
-- assertions are stable. Keep this block identical in 04337 and 04356.
--  * enable_analyzer -- the build-side choice lives in the logical join optimizer;
--  * use_statistics -- off, else a statistics estimator replaces the metadata row counts;
--  * query_plan_optimize_join_order_limit -- a randomized 0 disables the reorder pass (where the
--    swap lives), leaving the tables in their written order;
--  * query_plan_optimize_join_order_randomize -- a non-zero seed replaces the estimates with
--    random values;
--  * collect/use hash-table stats -- a process-global cache that persists across test runs;
--  * enable_join_transitive_predicates -- changes which residual filters are inferred across keys;
--  * enable_join_runtime_filters -- off, to keep the plans simple.
SET enable_analyzer = 1;
SET query_plan_join_swap_table = 'auto';
SET use_statistics = 0;
SET query_plan_optimize_join_order_limit = 10;
SET query_plan_optimize_join_order_randomize = 0;
SET collect_hash_table_stats_during_joins = 0;
SET use_hash_table_stats_for_join_reordering = 0;
SET enable_join_transitive_predicates = 1;
SET enable_join_runtime_filters = 0;

DROP TABLE IF EXISTS arr_tbl;
DROP TABLE IF EXISTS big;

CREATE TABLE arr_tbl (k Int32, vals Array(Int32)) ENGINE = MergeTree ORDER BY k;
INSERT INTO arr_tbl SELECT number, range(1000) FROM numbers(100);

CREATE TABLE big (k Int32) ENGINE = MergeTree ORDER BY k;
INSERT INTO big SELECT number FROM numbers(50000);

-- arrayJoin function form (folded into expression actions)
SELECT * FROM (SELECT k, arrayJoin(vals) AS x FROM arr_tbl) AS a JOIN big ON a.k = big.k
SETTINGS log_comment = '04356_array_join_function' FORMAT Null;

-- ARRAY JOIN clause form (dedicated ArrayJoinStep)
SELECT * FROM (SELECT k, x FROM arr_tbl ARRAY JOIN vals AS x) AS a JOIN big ON a.k = big.k
SETTINGS log_comment = '04356_array_join_clause' FORMAT Null;

SYSTEM FLUSH LOGS query_log;

-- In both forms the build side must be `big` (50000), not the exploded side (100000).
SELECT
    log_comment,
    if(ProfileEvents['JoinBuildTableRowCount'] BETWEEN 45000 AND 55000, 'ok', format('fail({}): build={}', query_id, ProfileEvents['JoinBuildTableRowCount'])),
    if(ProfileEvents['JoinProbeTableRowCount'] BETWEEN 90000 AND 110000, 'ok', format('fail({}): probe={}', query_id, ProfileEvents['JoinProbeTableRowCount'])),
    if(ProfileEvents['JoinResultRowCount'] = 100000, 'ok', format('fail({}): result={}', query_id, ProfileEvents['JoinResultRowCount']))
FROM system.query_log
WHERE type = 'QueryFinish' AND event_date >= yesterday() AND event_time >= now() - 600
  AND query_kind = 'Select' AND current_database = currentDatabase()
  AND log_comment IN ('04356_array_join_function', '04356_array_join_clause')
ORDER BY log_comment;

DROP TABLE IF EXISTS arr_tbl;
DROP TABLE IF EXISTS big;
