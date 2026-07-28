-- https://github.com/ClickHouse/ClickHouse/issues/78761
-- A non-equi JOIN ON predicate over a Nothing-typed column (e.g. from ARRAY JOIN [])
-- used to abort with LOGICAL_ERROR "Unexpected expression in JOIN ON section. Expected boolean (UInt8), got 'Nothing'".

-- The bug and its fix live in the analyzer's planner; the old analyzer cannot resolve
-- an ARRAY JOIN alias inside JOIN ON at all, so pin the analyzer to exercise the fixed path.
SET enable_analyzer = 1;

DROP TABLE IF EXISTS t1;
CREATE TABLE t1 (c0 Int32) ENGINE = MergeTree ORDER BY c0;
INSERT INTO t1 VALUES (1), (2), (3);

-- single residual predicate
SELECT 1 AS x FROM t1 tx ARRAY JOIN [] AS a0 LEFT JOIN t1 ON t1.c0 = a0 AND t1.c0 <> a0;
-- multiple residual predicates
SELECT 1 AS x FROM t1 tx ARRAY JOIN [] AS a0 LEFT JOIN t1 ON t1.c0 = a0 AND t1.c0 <> a0 AND t1.c0 > a0;
-- residual mixed with a normal predicate
SELECT 1 AS x FROM t1 tx ARRAY JOIN [] AS a0 LEFT JOIN t1 ON t1.c0 = a0 AND t1.c0 <> a0 AND t1.c0 > 0;
-- INNER join
SELECT 1 AS x FROM t1 tx ARRAY JOIN [] AS a0 INNER JOIN t1 ON t1.c0 = a0 AND t1.c0 <> a0;
-- ConcurrentHashJoin path (issue #78761 stack trace)
SELECT 1 AS x FROM t1 tx ARRAY JOIN [] AS a0 LEFT JOIN t1 ON t1.c0 = a0 AND t1.c0 <> a0 SETTINGS max_threads = 4;

-- non-empty ARRAY JOIN keeps producing correct results
SELECT t1.c0, a0 FROM t1 tx ARRAY JOIN [2, 3] AS a0 LEFT JOIN t1 ON t1.c0 = a0 AND t1.c0 <> a0 + 100 ORDER BY t1.c0, a0;

DROP TABLE t1;
