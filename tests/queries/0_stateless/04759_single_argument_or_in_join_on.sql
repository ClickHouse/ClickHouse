-- A single-argument `or` used to trip `chassert(or_argument_nodes.size() > 1)` in
-- `tryExtractCommonExpressions` (an exception with a logical error in debug builds). Such a node can
-- reach the pass because an argument of type `Nothing` (here: the alias of an empty array from
-- `ARRAY JOIN`) short-circuits function resolution before the "at least 2 arguments" check runs.
-- Found by AST fuzzer.

-- `LogicalExpressionOptimizerPass` only runs with the analyzer, and its common-expression
-- extraction for JOIN expressions is gated on `optimize_extract_common_expressions`; enable both
-- explicitly so the test targets the pass regardless of the defaults.
SET enable_analyzer = 1;
SET optimize_extract_common_expressions = 1;

DROP TABLE IF EXISTS t_04759;
CREATE TABLE t_04759 (c0 Int32) ENGINE = MergeTree ORDER BY c0;
INSERT INTO t_04759 VALUES (1), (2), (3);

-- The assertion tripped during analysis (query tree passes), so `EXPLAIN QUERY TREE` with
-- `run_passes = 1` exercises exactly the fixed code path and succeeds after the fix, without
-- depending on this join shape being unimplemented at execution time.
SELECT count() > 0 FROM
(
    EXPLAIN QUERY TREE run_passes = 1
    SELECT 9223372036854775807 AS x
    FROM t_04759 AS tx
    LEFT ARRAY JOIN [] AS a0
    GLOBAL ANTI RIGHT JOIN t_04759 ON or(t_04759.c0 = a0)
);

DROP TABLE t_04759;
