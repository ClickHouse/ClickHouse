-- Regression test: `pushOrderByIntoView` must not push `ORDER BY`/`LIMIT` into a view whose own
-- query contains a stateful function (e.g. `neighbor`, `runningAccumulate`, `logTrace`). Injecting
-- the sort and the truncation into the view definition moves them to the same query level as the
-- stateful expression, so the inner query is planned as an ordered top-N query and the stateful
-- function can run under a different read strategy than in the original plan, where the view
-- executes in full and the sort and `LIMIT` stay above it.

-- The per-query `enable_analyzer = 1` below sits inside `FROM (EXPLAIN ...)` subqueries; pin the
-- analyzer at the session level too, or the old-analyzer CI configuration rejects the queries with
-- "Setting 'enable_analyzer' is changed in the subquery" (`INCORRECT_QUERY`).
SET enable_analyzer = 1;
SET allow_deprecated_error_prone_window_functions = 1;

DROP TABLE IF EXISTS t_04830;
DROP VIEW IF EXISTS v_04830_stateful;
DROP VIEW IF EXISTS v_04830_stateful_where;
DROP VIEW IF EXISTS v_04830_udf;
DROP VIEW IF EXISTS v_04830_control;
DROP FUNCTION IF EXISTS f_04830_neighbor;

CREATE TABLE t_04830 (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 8;
INSERT INTO t_04830 SELECT number, number * 10 FROM numbers(1000);

CREATE VIEW v_04830_stateful AS SELECT neighbor(v, 1) AS n, k FROM t_04830;
CREATE VIEW v_04830_stateful_where AS SELECT k, v FROM t_04830 WHERE neighbor(v, 1) >= 0;
CREATE FUNCTION f_04830_neighbor AS x -> neighbor(x, 1);
CREATE VIEW v_04830_udf AS SELECT f_04830_neighbor(v) AS n, k FROM t_04830;
CREATE VIEW v_04830_control AS SELECT v AS n, k FROM t_04830;

-- The plan must keep a single `Sorting` step: the pushdown would add a second one inside the view
-- subquery. Pin `enable_parallel_replicas = 0`: the parallel-replicas CI configuration enables it
-- at the session level, and a parallel-replicas plan legitimately suppresses the pushdown.

-- Stateful function in the view's projection.
SELECT if(countIf(explain LIKE '%Sorting%') = 1, 'not pushed', 'pushed')
FROM (EXPLAIN SELECT n FROM v_04830_stateful ORDER BY k ASC LIMIT 1
    SETTINGS max_threads = 1, enable_parallel_replicas = 0, enable_analyzer = 1);

-- Stateful function in the view's WHERE clause.
SELECT if(countIf(explain LIKE '%Sorting%') = 1, 'not pushed', 'pushed')
FROM (EXPLAIN SELECT k FROM v_04830_stateful_where ORDER BY k ASC LIMIT 1
    SETTINGS max_threads = 1, enable_parallel_replicas = 0, enable_analyzer = 1);

-- Stateful function hidden behind a SQL UDF wrapper in the view's projection.
SELECT if(countIf(explain LIKE '%Sorting%') = 1, 'not pushed', 'pushed')
FROM (EXPLAIN SELECT n FROM v_04830_udf ORDER BY k ASC LIMIT 1
    SETTINGS max_threads = 1, enable_parallel_replicas = 0, enable_analyzer = 1);

-- Control: without a stateful function the pushdown still fires (a second `Sorting` step inside
-- the view subquery), so the checks above stay meaningful.
SELECT if(countIf(explain LIKE '%Sorting%') = 2, 'pushed', 'not pushed')
FROM (EXPLAIN SELECT n FROM v_04830_control ORDER BY k ASC LIMIT 1
    SETTINGS max_threads = 1, enable_parallel_replicas = 0, enable_analyzer = 1);

-- The stateful view must still return correct values: `neighbor(v, 1)` over the full single-block
-- read of the view sees the row after `k = 0`, i.e. `v = 10`.
SELECT n FROM v_04830_stateful ORDER BY k ASC LIMIT 1
    SETTINGS max_threads = 1, max_block_size = 65536, enable_parallel_replicas = 0, enable_analyzer = 1;

DROP VIEW v_04830_stateful;
DROP VIEW v_04830_stateful_where;
DROP VIEW v_04830_udf;
DROP VIEW v_04830_control;
DROP TABLE t_04830;
DROP FUNCTION f_04830_neighbor;
