-- Test for issue #103730: `replaceToConstants` in `ConvertQueryToCNFPass` must not
-- recurse into correlated subqueries.
--
-- Root cause: `replaceToConstants` (a free function in `ConvertQueryToCNFPass.cpp`) walked
-- `term->getChildren()` unconditionally, descending into `QueryNode`/`UnionNode` subtrees
-- and into their `correlated_columns_list`. When a correlated column had a constant
-- equivalence in the constraint comparison graph (e.g. via `ASSUME a = 5`), the
-- `ColumnNode` in `correlated_columns_list` was replaced with a `ConstantNode`, and a
-- subsequent `CollectTopLevelColumnIdentifiersVisitor` raised
-- `Bad cast from type DB::ConstantNode to DB::ColumnNode` (LOGICAL_ERROR).
--
-- This is the third unguarded tree-walker in this pass. PR #100756 added the same
-- `QUERY`/`UNION` skip guard to `ComponentCollectorVisitor` and `SubstituteColumnVisitor`
-- but did not touch `replaceToConstants`. This test exercises the `replaceToConstants`
-- path specifically by setting `optimize_substitute_columns = 0`, which bypasses
-- `SubstituteColumnVisitor`.

DROP TABLE IF EXISTS t_replace_const_corr;

CREATE TABLE t_replace_const_corr
(
    a Int32,
    b Int32,
    CONSTRAINT c1 ASSUME a = 5
)
ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t_replace_const_corr VALUES (5, 10);

-- Case 1: original reproduction from the issue. Without the fix this aborts with
-- `LOGICAL_ERROR: Bad cast from type DB::ConstantNode to DB::ColumnNode`.
-- With the fix the query executes and returns 1.
SELECT count() FROM t_replace_const_corr
WHERE exists((SELECT 1 PREWHERE a > 0))
SETTINGS enable_analyzer = 1, convert_query_to_cnf = 1, optimize_using_constraints = 1, optimize_substitute_columns = 0;

-- Case 2: same code path with a `UNION` subquery — `replaceToConstants` should also skip
-- `UnionNode` children, not just `QueryNode`.
SELECT count() FROM t_replace_const_corr
WHERE exists((SELECT 1 WHERE a > 0 UNION ALL SELECT 1 WHERE a < 0))
SETTINGS enable_analyzer = 1, convert_query_to_cnf = 1, optimize_using_constraints = 1, optimize_substitute_columns = 0;

-- Case 3: NOT EXISTS variant — a different correlated-subquery shape that still produces
-- the same `correlated_columns_list` corruption pattern in the buggy code.
SELECT count() FROM t_replace_const_corr
WHERE NOT exists((SELECT 1 PREWHERE a > 100))
SETTINGS enable_analyzer = 1, convert_query_to_cnf = 1, optimize_using_constraints = 1, optimize_substitute_columns = 0;

-- Case 4: direct correlated scalar subquery. Here the atom passed to `replaceToConstants`
-- is the `QUERY` node itself, not a child, so the recursion-time child guard is not enough;
-- the early return at the top of `replaceToConstants` is what prevents the bad cast.
SELECT count() FROM t_replace_const_corr
WHERE (SELECT a = 5)
SETTINGS enable_analyzer = 1, allow_experimental_correlated_subqueries = 1, convert_query_to_cnf = 1, optimize_using_constraints = 1, optimize_substitute_columns = 0;

DROP TABLE t_replace_const_corr;
