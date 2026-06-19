-- Tags: no-parallel-replicas

-- A MergeTree read can shed rows AFTER the mark-range selection that `selected_rows` counts -- here
-- via a lightweight DELETE, whose `_row_exists` mask is applied while reading, after the marks are
-- chosen. Such a scan must be treated as an upper bound, not an exact count, or the build-side
-- choice could trust the pre-mask count as a lower bound and swap a larger input onto the build
-- side over a much smaller deleted-mask-filtered one.
--
-- `lw_r` has 1000000 rows; the lightweight DELETE leaves 10, but the mask does not change
-- `selected_rows` (still ~1000000). If that 1000000 were treated as an exact estimate, the
-- residual-filtered `lw_l` (upper bound 500) would be swapped onto the build side (500 < 1000000),
-- even though the right side actually yields only 10 rows. With the lightweight-delete mask
-- recognized as a row reducer, the scan stays an upper bound and cannot anchor the swap, so the
-- tiny right side stays the build.

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

DROP TABLE IF EXISTS lw_r;
DROP TABLE IF EXISTS lw_l;

CREATE TABLE lw_r (k Int64, w Int32) ENGINE = MergeTree ORDER BY k;
-- A single INSERT -> one part, so there is no merge partner: the lightweight-delete mask stays
-- unmaterialized (no merge physically removes the rows), keeping `selected_rows` at ~1000000.
INSERT INTO lw_r SELECT number, number FROM numbers(1000000);
DELETE FROM lw_r WHERE k % 100000 != 0;   -- leaves 10 rows; selected_rows stays ~1000000

CREATE TABLE lw_l (k Int64, x Int32) ENGINE = MergeTree ORDER BY k;
INSERT INTO lw_l SELECT number, number FROM numbers(500);

SELECT * FROM lw_l JOIN lw_r ON lw_l.k = lw_r.k WHERE lw_l.x != -1
SETTINGS log_comment = '04359_join_build_table_lightweight_delete_not_exact' FORMAT Null;

SYSTEM FLUSH LOGS query_log;

-- The lightweight-deleted side (10 rows) must stay the build side; the 500-row left input must not
-- be swapped onto it on the strength of the 1000000-row pre-mask over-count.
SELECT
    if(ProfileEvents['JoinBuildTableRowCount'] BETWEEN 1 AND 100, 'ok', format('fail({}): build={}', query_id, ProfileEvents['JoinBuildTableRowCount'])),
    if(ProfileEvents['JoinProbeTableRowCount'] BETWEEN 400 AND 600, 'ok', format('fail({}): probe={}', query_id, ProfileEvents['JoinProbeTableRowCount'])),
    if(ProfileEvents['JoinResultRowCount'] = 1, 'ok', format('fail({}): result={}', query_id, ProfileEvents['JoinResultRowCount']))
FROM system.query_log
WHERE type = 'QueryFinish' AND event_date >= yesterday() AND event_time >= now() - 600
  AND query_kind = 'Select' AND current_database = currentDatabase()
  AND log_comment = '04359_join_build_table_lightweight_delete_not_exact'
ORDER BY event_time DESC
LIMIT 1;

DROP TABLE IF EXISTS lw_r;
DROP TABLE IF EXISTS lw_l;
