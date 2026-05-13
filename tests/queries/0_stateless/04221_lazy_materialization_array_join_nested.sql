-- Regression test for the lazy-materialization variant of issue #82279,
-- specifically issue #101608 ("Bad arrayJoin result with optimize_read_in_order
-- and query_plan_execute_functions_after_sorting disabled").
--
-- Pulled in from PR #101644 (Vladimir Cherkasov / @vdimir) and consolidated
-- here so a single test file covers both lazy-materialization-only repros and
-- the broader `arrayJoin`-below-sort-with-`LIMIT` repros that PR #104558
-- adds for issue #82279.
--
-- Without the guard in `optimizeLazyMaterialization2` the `ExpressionStep`
-- containing `arrayJoin` would be split into a main half (run before
-- `JoinLazyColumnsStep`, i.e. before `LIMIT`) and a lazy half (run after
-- `JoinLazyColumnsStep`). The lazy half would then re-expand the already
-- limited rows and produce extra output that the `LIMIT` was supposed to
-- truncate.

SET query_plan_optimize_lazy_materialization = 1;
SET flatten_nested = 0;

DROP TABLE IF EXISTS t0;
CREATE TABLE t0
(
    id UInt32,
    col1 Nested(a UInt32, n Nested(s String, b UInt32))
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO t0 SELECT number, arrayMap(x -> (x, arrayMap(y -> (toString(number), number), range(number))), range(number)) FROM numbers(10);

SELECT arrayJoin(col1.n) AS e FROM t0 ORDER BY id LIMIT 2 OFFSET 2;

SELECT '----------------------';

-- This combination disables the `liftUpFunctions` guard added in this PR
-- (via `query_plan_execute_functions_after_sorting = 0`) and forces the
-- non-read-in-order path, isolating the `optimizeLazyMaterialization`
-- guard as the only thing that can keep the result correct.
SELECT arrayJoin(col1.n) AS e FROM t0 ORDER BY id LIMIT 2 OFFSET 2 SETTINGS optimize_read_in_order = 0, query_plan_execute_functions_after_sorting = 0;

DROP TABLE t0;
