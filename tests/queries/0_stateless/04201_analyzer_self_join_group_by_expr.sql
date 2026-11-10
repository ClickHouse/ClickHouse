-- Test: exercises `areColumnSourcesEqual` recursive descent through `FunctionNode` children
--        when GROUP BY key is a complex expression with mismatched column sources.
-- Covers: src/Analyzer/ValidationUtils.cpp:117-129 — the children iteration loop.
-- PR tests only compare bare `ColumnNode` (no children pushed). This test pushes
-- arguments_list of `FunctionNode` and recurses to detect alias-source mismatch deep in tree.

DROP TABLE IF EXISTS t1_04201;
CREATE TABLE t1_04201 (x Int32) ENGINE = MergeTree ORDER BY x;
INSERT INTO t1_04201 VALUES (1), (2), (3);

SET enable_analyzer = 1;

-- SELECT expression `t1.x + t2.x` does NOT match GROUP BY `t1.x + t1.x` because the
-- second argument has a different column source. Without recursive source-equality check,
-- the parent FunctionNode would be incorrectly considered "in GROUP BY".
SELECT t1.x + t2.x FROM t1_04201 AS t1 JOIN t1_04201 AS t2 ON t1.x = t2.x GROUP BY t1.x + t1.x; -- { serverError NOT_AN_AGGREGATE }

-- Same gap inside a tuple expression.
SELECT (t1.x, t2.x) FROM t1_04201 AS t1 JOIN t1_04201 AS t2 ON t1.x = t2.x GROUP BY (t1.x, t1.x); -- { serverError NOT_AN_AGGREGATE }

-- Function applied to mismatched column.
SELECT abs(t2.x) FROM t1_04201 AS t1 JOIN t1_04201 AS t2 ON t1.x = t2.x GROUP BY abs(t1.x); -- { serverError NOT_AN_AGGREGATE }

-- Success: matching alias structure in GROUP BY expression.
SELECT t1.x + t2.x FROM t1_04201 AS t1 JOIN t1_04201 AS t2 ON t1.x = t2.x GROUP BY t1.x + t2.x ORDER BY ALL;
SELECT (t1.x, t2.x) FROM t1_04201 AS t1 JOIN t1_04201 AS t2 ON t1.x = t2.x GROUP BY (t1.x, t2.x) ORDER BY ALL;

DROP TABLE t1_04201;
