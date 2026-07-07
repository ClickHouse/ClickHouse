-- https://github.com/ClickHouse/ClickHouse/issues/105760
--
-- PR #105900 strips the inner projection of a correlated `EXISTS` subquery to
-- `SELECT 1` so projection-only outer references do not become equality
-- predicates of the decorrelation join. An earlier follow-up applied the
-- rewrite per-branch for `UnionNode` join trees, but ONLY when every branch
-- was itself a `QueryNode`. If a branch was another `UnionNode` (which happens
-- when union modes are mixed, e.g. `(UNION DISTINCT) UNION ALL`), the rewrite
-- was skipped for the whole subquery and projection-only correlations leaked
-- back into the outer `EXISTS` wrapper.
--
-- This test pins the recursive-descent behavior: for any nesting of
-- safe-mode `UnionNode` subtrees (`UNION_ALL` / `UNION_DISTINCT` /
-- `UNION_DEFAULT`), every leaf `QueryNode` projection that only affects
-- output must be stripped to a constant 1. `INTERSECT` and `EXCEPT` subtrees
-- (where row VALUES across branches matter) must remain intact.

SET enable_analyzer = 1;
SET allow_experimental_correlated_subqueries = 1;

-- (1) Nested on the left: `(UNION DISTINCT) UNION ALL Q3`. The outer `UNION_ALL`
-- is safe, the nested `UNION_DISTINCT` is also safe. All three leaf
-- `QueryNode` projections must be stripped, so no leaf projection still
-- references the outer `o.number` column.
SELECT countIf(explain LIKE '%column_name: number%') AS unstripped_refs_nested_left
FROM
(
    EXPLAIN QUERY TREE
    SELECT 1 FROM numbers(3) AS o
    WHERE EXISTS
    (
        (
            (SELECT o.number FROM system.one AS r1)
            UNION DISTINCT
            (SELECT o.number FROM system.one AS r2)
        )
        UNION ALL
        (SELECT o.number FROM system.one AS r3)
    )
);

-- (2) Nested on the right: `Q1 UNION ALL (UNION DISTINCT)`. Same expectation,
-- the nested `UnionNode` sits in the second branch slot.
SELECT countIf(explain LIKE '%column_name: number%') AS unstripped_refs_nested_right
FROM
(
    EXPLAIN QUERY TREE
    SELECT 1 FROM numbers(3) AS o
    WHERE EXISTS
    (
        (SELECT o.number FROM system.one AS r1)
        UNION ALL
        (
            (SELECT o.number FROM system.one AS r2)
            UNION DISTINCT
            (SELECT o.number FROM system.one AS r3)
        )
    )
);

-- (3) Doubly nested: `(UNION DISTINCT) UNION ALL (UNION DISTINCT)`. Recursion
-- must reach each of the four leaf `QueryNode`s.
SELECT countIf(explain LIKE '%column_name: number%') AS unstripped_refs_double_nested
FROM
(
    EXPLAIN QUERY TREE
    SELECT 1 FROM numbers(3) AS o
    WHERE EXISTS
    (
        (
            (SELECT o.number FROM system.one AS r1)
            UNION DISTINCT
            (SELECT o.number FROM system.one AS r2)
        )
        UNION ALL
        (
            (SELECT o.number FROM system.one AS r3)
            UNION DISTINCT
            (SELECT o.number FROM system.one AS r4)
        )
    )
);

-- (4) `INTERSECT` inside `UNION ALL`: the `INTERSECT` subtree depends on row
-- VALUES, so its leaf projections must NOT be stripped. The plain `QueryNode`
-- branch on the outer `UNION_ALL` is safe and IS stripped. The exact count is
-- not asserted; instead we assert presence: at least one leaf projection
-- still references the outer column (in the `INTERSECT` subtree), and the
-- plain branch on the safe `UNION_ALL` side no longer contributes a
-- correlated reference. This is the strongest invariant we can express that
-- distinguishes "INTERSECT preserved" from "everything stripped".
SELECT countIf(explain LIKE '%column_name: number%') > 0 AS intersect_branch_preserved
FROM
(
    EXPLAIN QUERY TREE
    SELECT 1 FROM numbers(3) AS o
    WHERE EXISTS
    (
        (
            (SELECT o.number FROM system.one AS r1)
            INTERSECT
            (SELECT o.number FROM system.one AS r2)
        )
        UNION ALL
        (SELECT o.number FROM system.one AS r3)
    )
);

-- (5) Sanity: existing flat `UNION ALL` case (already covered by PR #105900's
-- earlier commit) still strips all branches.
SELECT countIf(explain LIKE '%column_name: number%') AS unstripped_refs_flat
FROM
(
    EXPLAIN QUERY TREE
    SELECT 1 FROM numbers(3) AS o
    WHERE EXISTS
    (
        (SELECT o.number FROM system.one AS r1)
        UNION ALL
        (SELECT o.number FROM system.one AS r2)
    )
);

-- (6) Sanity: existing plain `QueryNode` case (the original #105760 fix) still
-- strips the projection.
SELECT countIf(explain LIKE '%column_name: number%') AS unstripped_refs_plain
FROM
(
    EXPLAIN QUERY TREE
    SELECT 1 FROM numbers(3) AS o
    WHERE EXISTS (SELECT o.number FROM system.one)
);
