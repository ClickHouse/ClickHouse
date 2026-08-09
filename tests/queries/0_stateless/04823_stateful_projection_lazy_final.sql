-- The lazy FINAL optimization (query_plan_optimize_lazy_final) replaces the regular FINAL read
-- with InputSelector branches, and with mixed intersecting/non-intersecting parts it additionally
-- unions the split-off non-intersecting parts with the result. The union produces the same rows in
-- a different row/block stream, so a stateful expression above the read (`neighbor`,
-- `runningAccumulate`, `logTrace`) would observe a different stream than in the unoptimized query.
-- `optimizeLazyFinal` must keep the regular FINAL read when the projection is stateful.

-- The lazy FINAL optimization is a query-plan rewrite exercised through the analyzer plan shape;
-- pin the analyzer so the control keeps optimizing in the old-analyzer CI configuration.
SET enable_analyzer = 1;
SET query_plan_optimize_lazy_final = 1;
SET allow_deprecated_error_prone_window_functions = 1;
SET max_threads = 1;
SET max_block_size = 65536;
SET optimize_move_to_prewhere = 0;
SET enable_parallel_replicas = 0;

DROP TABLE IF EXISTS t_04823;
CREATE TABLE t_04823 (k UInt64, v UInt64) ENGINE = ReplacingMergeTree ORDER BY k
    SETTINGS index_granularity = 4;
SYSTEM STOP MERGES t_04823;
-- Two intersecting parts (overlapping key ranges) plus one non-intersecting part,
-- so the optimization takes the mixed path: InputSelector branches + a union with
-- the split-off non-intersecting part.
INSERT INTO t_04823 SELECT number, number FROM numbers(0, 10);
INSERT INTO t_04823 SELECT number, number * 10 FROM numbers(5, 10);
INSERT INTO t_04823 SELECT number, number FROM numbers(100, 10);

-- Control: with a non-stateful projection the optimization applies (the plan contains
-- an InputSelector and the union with the non-intersecting part).
SELECT if(countIf(explain LIKE '%InputSelector%') > 0, 'optimized', 'not optimized')
FROM (EXPLAIN SELECT v FROM t_04823 FINAL WHERE v < 1000000);

-- A stateful function in the projection must keep the regular FINAL read.
SELECT if(countIf(explain LIKE '%InputSelector%') > 0, 'optimized', 'not optimized')
FROM (EXPLAIN SELECT neighbor(v, 1) FROM t_04823 FINAL WHERE v < 1000000);

SELECT if(countIf(explain LIKE '%InputSelector%') > 0, 'optimized', 'not optimized')
FROM (EXPLAIN SELECT logTrace('t_04823'), v FROM t_04823 FINAL WHERE v < 1000000);

-- The stateful projection gets the same plan with the setting enabled and disabled.
-- Note: this deliberately compares the plans, not the query results. The regular FINAL read
-- over mixed parts emits the merged intersecting stream and the split-off non-intersecting
-- stream as separate chunks, and the chunk order is not deterministic run-to-run, so comparing
-- the values of the chunk-boundary-sensitive `neighbor` between two independent executions
-- is flaky even though both run the identical plan.
SELECT (
    SELECT groupArray(explain) FROM (EXPLAIN SELECT neighbor(v, 1) FROM t_04823 FINAL WHERE v < 1000000
        SETTINGS query_plan_optimize_lazy_final = 1)
) = (
    SELECT groupArray(explain) FROM (EXPLAIN SELECT neighbor(v, 1) FROM t_04823 FINAL WHERE v < 1000000
        SETTINGS query_plan_optimize_lazy_final = 0)
);

DROP TABLE t_04823;
