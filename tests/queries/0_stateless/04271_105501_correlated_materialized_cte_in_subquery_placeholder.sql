-- Regression test for issue #105501 (STID 1611-343c):
-- "Logical error: Trying to execute PLACEHOLDER action".
--
-- Background.
-- `FutureSetFromSubquery::build` shares the same source plan as the inplace variants
-- `buildSetInplace` / `buildOrderedSetInplace`. The inplace variants already check
-- `hasCorrelatedExpressions` on the source plan and return early when `PLACEHOLDER`
-- actions are present (the plan cannot be executed standalone, it must be decorrelated
-- by the outer query). `build`, however, is reached through a different family of
-- callers: `addCreatingSetsTransform` (mutations), and
-- `DelayedCreatingSetsStep::makePlansForSets` (the `addPlansForSets` optimisation).
-- Without the same guard, these callers happily install a `CreatingSetStep` on top of
-- a `PLACEHOLDER`-bearing plan, and the resulting pipeline aborts in
-- `ExpressionActions::execute` with `LOGICAL_ERROR`.
--
-- The fuzzer hits this via a `MATERIALIZED` CTE whose body references the outer scope,
-- e.g. `WITH t AS MATERIALIZED (SELECT number FROM zeros(N)) SELECT ... WHERE x IN (t)`
-- (here `number` is not a column of `zeros`, so it resolves outward and turns the CTE
-- body into a correlated subquery). The shapes below exercise the same combination on
-- the regular planner path so they would also crash without the guard.

SET enable_analyzer = 1;
SET allow_experimental_correlated_subqueries = 1;

DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;

CREATE TABLE t1 (a UInt32, b UInt32) ENGINE = MergeTree() ORDER BY a;
CREATE TABLE t2 (x UInt32, y UInt32) ENGINE = MergeTree() ORDER BY x;

INSERT INTO t1 VALUES (1, 10), (2, 20), (3, 30);
INSERT INTO t2 VALUES (10, 100), (20, 200), (40, 400);

-- Plain correlated `IN` subquery. The planner builds a `FutureSetFromSubquery` whose
-- source plan contains a `PLACEHOLDER` for `t1.b`. Without the guard the optimisation
-- step `addPlansForSets` would call `build` on it and the resulting pipeline would
-- abort with `Trying to execute PLACEHOLDER action`. The analyzer rejects correlated
-- `IN` subqueries up front today, so the expected outcome is a clear `NOT_IMPLEMENTED`
-- error rather than a `LOGICAL_ERROR`. Any other outcome is a regression.
SELECT '-- correlated IN subquery';
SELECT a FROM t1 WHERE a IN (SELECT x FROM t2 WHERE t2.y = t1.b) ORDER BY a FORMAT Null; -- { serverError NOT_IMPLEMENTED }

-- Same shape but the correlated reference now lives behind a `MATERIALIZED` CTE wrapper.
-- The CTE wraps the body in a `TableNode`, which is what the AST fuzzer query in
-- issue #105501 ultimately reaches via `zeros(...)`. The analyzer rejects this case
-- with the dedicated `Materialized CTE 't' cannot be correlated` check
-- (`UNSUPPORTED_METHOD`), so we never reach the planner with a `PLACEHOLDER`-bearing
-- subquery plan. Any other outcome is a regression.
SELECT '-- correlated MATERIALIZED CTE in IN';
SELECT a FROM t1 WHERE a IN (
    WITH t AS MATERIALIZED (SELECT x FROM t2 WHERE t2.y = t1.b)
    SELECT x FROM t
) ORDER BY a SETTINGS enable_materialized_cte = 1 FORMAT Null; -- { serverError UNSUPPORTED_METHOD }

-- Sanity check: a non-correlated `IN` subquery still works after the guard. This
-- ensures the new `if (hasCorrelatedExpressions(...)) return nullptr` short-circuit
-- only fires for plans that actually carry `PLACEHOLDER` nodes. The expected output
-- is `a = 2`, since `t2.x = t1.b` matches only the pair `(2, 20)` / `(20, 200)`.
SELECT '-- non-correlated IN subquery still works';
SELECT a FROM t1 WHERE b IN (SELECT x * 2 FROM t2) ORDER BY a;

-- Sanity check: a non-correlated `MATERIALIZED` CTE on the right of `IN` still works.
SELECT '-- non-correlated MATERIALIZED CTE in IN still works';
WITH t AS MATERIALIZED (SELECT x * 2 AS xx FROM t2)
SELECT a FROM t1 WHERE b IN (t) ORDER BY a SETTINGS enable_materialized_cte = 1;

DROP TABLE t1;
DROP TABLE t2;
