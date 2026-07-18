-- Regression test for the old-analyzer read-in-order limit pushdown with a hidden reader-side
-- filter (here a row policy). The pushed-down `LIMIT` must not stop the storage before the
-- row-policy filter runs; otherwise `ORDER BY pk LIMIT N` under a policy that drops the leading
-- rows could return fewer than N rows.
--
-- The legacy paths (`InterpreterSelectQuery::executeFetchColumns` and the window shortcut in
-- `optimizeReadInOrder.cpp`) used to gate the limit pushdown only on AST-visible filtration
-- (`query.hasFiltration()`), which does not see a row policy. They now also fence on
-- `query_info.filter_asts` (row policy / additional filter / parallel-replicas custom-key filter),
-- matching the `filter_asts.empty()` check in `maxBlockSizeByLimit` and the query-plan fence in
-- `buildSortingDAG`.

DROP ROW POLICY IF EXISTS rp_04552 ON t_rp_04552;
DROP TABLE IF EXISTS t_rp_04552;

CREATE TABLE t_rp_04552 (pk UInt64) ENGINE = MergeTree ORDER BY pk SETTINGS index_granularity = 8;
INSERT INTO t_rp_04552 SELECT number FROM numbers(300);

-- Drops the first 100 rows (by the sort key `pk`); the first surviving row is pk = 100.
CREATE ROW POLICY rp_04552 ON t_rp_04552 FOR SELECT USING pk >= 100 TO ALL;

-- Must return the first three surviving rows in `pk` order (100, 101, 102), never fewer, on every
-- analyzer / read-in-order combination.
SELECT '-- read-in-order';
SELECT pk FROM t_rp_04552 ORDER BY pk LIMIT 3
    SETTINGS enable_analyzer = 0, optimize_read_in_order = 1, query_plan_read_in_order = 0, max_threads = 1, enable_parallel_replicas = 0;
SELECT '--';
SELECT pk FROM t_rp_04552 ORDER BY pk LIMIT 3
    SETTINGS enable_analyzer = 0, optimize_read_in_order = 1, query_plan_read_in_order = 1, max_threads = 1, enable_parallel_replicas = 0;
SELECT '--';
SELECT pk FROM t_rp_04552 ORDER BY pk LIMIT 3
    SETTINGS enable_analyzer = 1, optimize_read_in_order = 1, query_plan_read_in_order = 0, max_threads = 1, enable_parallel_replicas = 0;
SELECT '--';
SELECT pk FROM t_rp_04552 ORDER BY pk LIMIT 3
    SETTINGS enable_analyzer = 1, optimize_read_in_order = 1, query_plan_read_in_order = 1, max_threads = 1, enable_parallel_replicas = 0;

-- The legacy window read-in-order shortcut (`tryReuseStorageOrderingForWindowFunctions`) has the
-- same row-policy hole; it fires only with the old analyzer and the reuse setting on.
SELECT '-- window shortcut';
SELECT pk, row_number() OVER (ORDER BY pk) AS rn FROM t_rp_04552 ORDER BY pk LIMIT 3
    SETTINGS enable_analyzer = 0, optimize_read_in_order = 0, query_plan_read_in_order = 0,
             query_plan_reuse_storage_ordering_for_window_functions = 1, max_threads = 1, enable_parallel_replicas = 0;

DROP ROW POLICY rp_04552 ON t_rp_04552;
DROP TABLE t_rp_04552;
