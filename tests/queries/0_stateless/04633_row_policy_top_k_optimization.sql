-- Regression test for the top-K skip-index optimization (`tryOptimizeTopK` /
-- `perform_top_k_optimization`) with a hidden reader-side filter (here a row policy).
--
-- A row policy restricts rows inside the reader, just like a `WHERE` / `PREWHERE`. The optimization
-- used to gate on `where_clause = filter_step || getPrewhereInfo()`, which does not see a row policy,
-- so a query filtered only by a hidden policy stayed on the unfiltered fast path: `perform_top_k_optimization`
-- narrowed the read to the top-K marks before the policy ran, the policy discarded the rows in those
-- marks, and `ORDER BY key LIMIT N` could return fewer than N rows even though later marks hold rows the
-- policy keeps. `where_clause` now also counts `getRowLevelFilter()`, matching the visible-filter path.

DROP ROW POLICY IF EXISTS rp_04633 ON t_04633;
DROP TABLE IF EXISTS t_04633;

CREATE TABLE t_04633 (key UInt64, INDEX mm_key key TYPE minmax GRANULARITY 1)
    ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 8;
INSERT INTO t_04633 SELECT number FROM numbers(300);

-- Drops the first 100 rows (by the sort key `key`); the first surviving row is key = 100.
CREATE ROW POLICY rp_04633 ON t_04633 FOR SELECT USING key >= 100 TO ALL;

-- Must return the first three surviving rows in `key` order (100, 101, 102), never fewer, with the
-- skip-index top-k optimization enabled, on both analyzers.
SELECT key FROM t_04633 ORDER BY key LIMIT 3
    SETTINGS enable_analyzer = 0, use_skip_indexes_for_top_k = 1, use_top_k_dynamic_filtering = 0,
             max_threads = 1, enable_parallel_replicas = 0;
SELECT '--';
SELECT key FROM t_04633 ORDER BY key LIMIT 3
    SETTINGS enable_analyzer = 1, use_skip_indexes_for_top_k = 1, use_top_k_dynamic_filtering = 0,
             max_threads = 1, enable_parallel_replicas = 0;

DROP ROW POLICY rp_04633 ON t_04633;
DROP TABLE t_04633;
