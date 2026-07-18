-- A stateful function (or `arrayJoin`) inside a correlated subquery in the projection is an
-- execution-time expression evaluated in the context of the outer rows, so the planner-side
-- trivial-`LIMIT` / `ORDER BY` fences (`mainQueryNodeBlockSizeByLimit`, `pushOrderByIntoView`)
-- must treat such a projection as stateful and not cap or push the outer `LIMIT` below it. This
-- guards `hasStatefulFunctionNode` / `hasArrayJoinFunctionNode` descending into correlated
-- subqueries. Correlated subqueries require the analyzer.

SET enable_analyzer = 1;
SET enable_parallel_replicas = 0;
SET allow_deprecated_error_prone_window_functions = 1;

DROP TABLE IF EXISTS t_outer_04557;
DROP TABLE IF EXISTS t_inner_04557;

CREATE TABLE t_outer_04557 (k UInt64) ENGINE = Memory;
INSERT INTO t_outer_04557 SELECT number FROM numbers(5);

CREATE TABLE t_inner_04557 (g UInt64, v UInt64) ENGINE = Memory;
INSERT INTO t_inner_04557 SELECT number, number * 10 FROM numbers(5);

-- `logTrace` (the function this PR is about) hidden in a correlated subquery under a trivial `LIMIT`.
-- Correlation is trivially satisfied for every outer row, so the result is order-independent.
SELECT (SELECT logTrace('probe_04557') FROM system.one WHERE dummy = t_outer_04557.k * 0) AS c
FROM t_outer_04557
LIMIT 3;

-- A stateful function (`neighbor`) reading a real inner value inside a correlated subquery under a
-- trivial `LIMIT`. Every outer row matches the single inner row, so the answer is `7` regardless of
-- which three rows the `LIMIT` keeps.
SELECT (SELECT neighbor(v, 0) FROM (SELECT 7 AS v) AS s WHERE 1 = 1 OR s.v = t_outer_04557.k) AS c
FROM t_outer_04557
LIMIT 3;

-- A stateful function inside a correlated subquery combined with `ORDER BY ... LIMIT`: the per-key
-- values must stay correct.
SELECT k, (SELECT neighbor(v, 0) FROM t_inner_04557 WHERE t_inner_04557.g = t_outer_04557.k) AS c
FROM t_outer_04557
ORDER BY k
LIMIT 3;

DROP TABLE t_outer_04557;
DROP TABLE t_inner_04557;
