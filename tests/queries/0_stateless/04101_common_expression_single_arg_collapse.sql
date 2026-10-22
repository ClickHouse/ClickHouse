SET enable_analyzer = 1;
SET optimize_extract_common_expressions = 1;

-- Tests for single-argument collapse after common-expression extraction in
-- LogicalExpressionOptimizerPass. When a rewrite collapses to a single argument whose
-- ResultType differs from the original `and`/`or`, it must be wrapped as `and(arg, 1)`
-- so the `and` function performs the `!= 0` boolean coercion internally. A bare argument
-- followed by `_CAST(arg, UInt8)` would truncate non-integer values like 0.5 to 0 instead,
-- causing wrong result.

-- { echo }

DROP TABLE IF EXISTS t0;
CREATE TABLE t0 (c0 Int32) ENGINE = Memory;
INSERT INTO t0 VALUES (1), (2), (3);

-- Case 1: Full degeneration (issue #99832). ((c0 >= c0) OR (c0 >= c0)) AND (c0 >= c0)
-- collapses to a single (c0 >= c0) after common-expression extraction; types already
-- match the UInt8 AND result, so the bare argument is used directly.
SELECT c0 FROM t0 WHERE ((c0 >= c0) OR (c0 >= c0)) AND (c0 >= c0) ORDER BY c0;
EXPLAIN SYNTAX run_query_tree_passes = 1
SELECT c0 FROM t0 WHERE ((c0 >= c0) OR (c0 >= c0)) AND (c0 >= c0) ORDER BY c0;

-- Case 2: Partial degeneration - AND(OR(A,A), A, B) should become AND(A, B).
SELECT c0 FROM t0 WHERE ((c0 > 0) OR (c0 > 0)) AND (c0 > 0) AND (c0 < 10) ORDER BY c0;
EXPLAIN SYNTAX run_query_tree_passes = 1
SELECT c0 FROM t0 WHERE ((c0 > 0) OR (c0 > 0)) AND (c0 > 0) AND (c0 < 10) ORDER BY c0;

-- Case 3: No degeneration - normal AND expression should be unaffected.
SELECT c0 FROM t0 WHERE (c0 > 0) AND (c0 < 10) ORDER BY c0;
EXPLAIN SYNTAX run_query_tree_passes = 1
SELECT c0 FROM t0 WHERE (c0 > 0) AND (c0 < 10) ORDER BY c0;

DROP TABLE t0;

-- Case 4: AND single-arg collapse over Float64 must preserve `!= 0` semantics —
-- `and(x, 1)` keeps 0.5 truthy rather than truncating it to 0 via `_CAST` to UInt8.
SELECT x FROM (SELECT arrayJoin([0., 0.5, 1., 2.]) AS x) WHERE (x OR x) AND x ORDER BY x;
EXPLAIN SYNTAX run_query_tree_passes = 1
SELECT x FROM (SELECT arrayJoin([0., 0.5, 1., 2.]) AS x) WHERE (x OR x) AND x ORDER BY x;

-- Case 5: OR single-arg collapse over Float64 must preserve `!= 0` semantics.
SELECT x FROM (SELECT arrayJoin([0., 0.5, 1., 2.]) AS x) WHERE (x AND x) OR (x AND x) ORDER BY x;
EXPLAIN SYNTAX run_query_tree_passes = 1
SELECT x FROM (SELECT arrayJoin([0., 0.5, 1., 2.]) AS x) WHERE (x AND x) OR (x AND x) ORDER BY x;

-- Case 6: OR single-arg collapse where the collapsed type already matches the UInt8 OR result.
-- The bare argument is used directly; no `and(arg, 1)` wrap is needed.
SELECT n FROM (SELECT arrayJoin([1, 2, 3]) AS n) WHERE (n >= n) OR (n >= n) ORDER BY n;
EXPLAIN SYNTAX run_query_tree_passes = 1
SELECT n FROM (SELECT arrayJoin([1, 2, 3]) AS n) WHERE (n >= n) OR (n >= n) ORDER BY n;

-- Case 7: OR multi-arg - common factor extraction with a leftover disjunction.
-- ((n > 0) AND (n < 5)) OR ((n > 0) AND (n < 10)) factors out `(n > 0)`, producing
-- `and((n > 0), or((n < 5), (n < 10)))`.
SELECT n FROM (SELECT arrayJoin([1, 2, 3]) AS n) WHERE ((n > 0) AND (n < 5)) OR ((n > 0) AND (n < 10)) ORDER BY n;
EXPLAIN SYNTAX run_query_tree_passes = 1
SELECT n FROM (SELECT arrayJoin([1, 2, 3]) AS n) WHERE ((n > 0) AND (n < 5)) OR ((n > 0) AND (n < 10)) ORDER BY n;

-- Case 8: AND single-arg collapse over Int64 must preserve `!= 0` semantics —
-- `and(x, 1)` keeps 256 (truthy) rather than wrapping it mod 256 to 0 via `_CAST` to UInt8.
SELECT x FROM (SELECT arrayJoin([0::Int64, 1::Int64, 256::Int64, 257::Int64]) AS x) WHERE (x OR x) AND x ORDER BY x;
EXPLAIN SYNTAX run_query_tree_passes = 1
SELECT x FROM (SELECT arrayJoin([0::Int64, 1::Int64, 256::Int64, 257::Int64]) AS x) WHERE (x OR x) AND x ORDER BY x;

-- Case 9: AND single-arg collapse over Nullable(Float64) must preserve three-valued logic.
-- NULL is treated as false in WHERE, 0 is false, 0.5/1 are truthy; the wrap result type
-- becomes `Nullable(UInt8)` matching the original Nullable AND, so no `_CAST` is needed.
SELECT x FROM (SELECT arrayJoin([NULL, 0., 0.5, 1.]::Array(Nullable(Float64))) AS x) WHERE (x OR x) AND x ORDER BY x;
EXPLAIN SYNTAX run_query_tree_passes = 1
SELECT x FROM (SELECT arrayJoin([NULL, 0., 0.5, 1.]::Array(Nullable(Float64))) AS x) WHERE (x OR x) AND x ORDER BY x;
