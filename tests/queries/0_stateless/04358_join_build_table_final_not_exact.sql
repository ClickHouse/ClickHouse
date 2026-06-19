-- Tags: no-parallel-replicas

-- A MergeTree read can shed rows AFTER the mark-range selection that `selected_rows` counts --
-- here via FINAL (collapsing/replacing merges drop rows). Such a scan must be treated as an upper
-- bound, not an exact count, or the build-side choice could trust an over-count as a lower bound
-- and swap a larger input onto the build side.
--
-- `rep` is a ReplacingMergeTree with 10 unmerged parts of the same 100 keys: `selected_rows` is
-- 1000 (pre-merge), while `... FINAL` emits 100. If that 1000 were treated as an exact estimate,
-- the residual-filtered `lhs` (upper bound 500) would be swapped onto the build side (500 < 1000),
-- even though FINAL actually yields fewer rows than `lhs`. With FINAL recognized as a row reducer,
-- the scan stays an upper bound and cannot anchor the swap, so the small FINAL side stays the build.

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

DROP TABLE IF EXISTS rep;
DROP TABLE IF EXISTS lhs;

CREATE TABLE rep (k Int32, w Int32) ENGINE = ReplacingMergeTree ORDER BY k;
SYSTEM STOP MERGES rep;
-- 10 parts, each the same 100 keys -> selected_rows = 1000, but `FINAL` yields 100.
INSERT INTO rep SELECT number, 1 FROM numbers(100);
INSERT INTO rep SELECT number, 2 FROM numbers(100);
INSERT INTO rep SELECT number, 3 FROM numbers(100);
INSERT INTO rep SELECT number, 4 FROM numbers(100);
INSERT INTO rep SELECT number, 5 FROM numbers(100);
INSERT INTO rep SELECT number, 6 FROM numbers(100);
INSERT INTO rep SELECT number, 7 FROM numbers(100);
INSERT INTO rep SELECT number, 8 FROM numbers(100);
INSERT INTO rep SELECT number, 9 FROM numbers(100);
INSERT INTO rep SELECT number, 10 FROM numbers(100);

CREATE TABLE lhs (k Int32, v Int32) ENGINE = MergeTree ORDER BY k;
INSERT INTO lhs SELECT number, number FROM numbers(500);

SELECT * FROM lhs JOIN (SELECT k FROM rep FINAL) AS rf ON lhs.k = rf.k WHERE lhs.v != -1
SETTINGS log_comment = '04358_join_build_table_final_not_exact' FORMAT Null;

SYSTEM FLUSH LOGS query_log;

-- The FINAL side (100 rows) must stay the build side; the 500-row left input must not be swapped
-- onto it on the strength of the 1000-row pre-merge over-count.
SELECT
    if(ProfileEvents['JoinBuildTableRowCount'] BETWEEN 1 AND 200, 'ok', format('fail({}): build={}', query_id, ProfileEvents['JoinBuildTableRowCount'])),
    if(ProfileEvents['JoinProbeTableRowCount'] BETWEEN 400 AND 600, 'ok', format('fail({}): probe={}', query_id, ProfileEvents['JoinProbeTableRowCount'])),
    if(ProfileEvents['JoinResultRowCount'] = 100, 'ok', format('fail({}): result={}', query_id, ProfileEvents['JoinResultRowCount']))
FROM system.query_log
WHERE type = 'QueryFinish' AND event_date >= yesterday() AND event_time >= now() - 600
  AND query_kind = 'Select' AND current_database = currentDatabase()
  AND log_comment = '04358_join_build_table_final_not_exact'
ORDER BY event_time DESC
LIMIT 1;

DROP TABLE IF EXISTS rep;
DROP TABLE IF EXISTS lhs;
