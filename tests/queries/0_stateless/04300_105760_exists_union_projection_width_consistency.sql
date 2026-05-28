-- https://github.com/ClickHouse/ClickHouse/issues/105760
--
-- PR #105900 strips an `EXISTS` subquery's projection to `SELECT 1` so
-- projection-only outer correlations do not leak into the decorrelation JOIN.
-- A follow-up extended the rewrite per-branch inside a `UnionNode` for safe
-- modes (`UNION_ALL` / `UNION_DISTINCT` / `UNION_DEFAULT`) and recursed
-- through nested `UnionNode`s. The recursion applied the strip independently
-- to each leaf `QueryNode`. When ONE branch was strippable + correlated and a
-- SIBLING branch was NOT (either uncorrelated, or carrying `arrayJoin` /
-- aggregates / `INTERSECT` / `EXCEPT`), only the first was rewritten to
-- `SELECT 1`. The surviving `UnionNode` then had mismatched projection
-- widths and `UnionNode::computeProjectionColumns` raised `UNION different
-- number of columns` at planning time. See the inline bot review on
-- PR #105900.
--
-- This test pins the all-or-nothing strategy: a `UnionNode` subtree is
-- mutated ONLY when every branch (recursively through nested safe-mode
-- `UnionNode`s) is strippable. Mixed-eligibility subtrees are left intact so
-- branch projection widths stay consistent. The cost is missing the
-- projection-stripping optimization for the strippable siblings, but the
-- alternative (mid-planning `UNION different number of columns`) is fatal.

SET enable_analyzer = 1;
SET allow_experimental_correlated_subqueries = 1;

-- (1) Bot's exact reproducer. Branch 1 is correlated + strippable, branch 2
-- is uncorrelated + strippable, both with 2 projection columns. Before the
-- all-or-nothing gate, only the correlated branch was stripped to `SELECT 1`
-- (1 col) while the uncorrelated sibling kept 2 columns, raising
-- `UNION different number of columns`. With the gate the entire subtree is
-- strippable, so both branches are stripped uniformly. Both branches
-- contribute at least one row (`system.one` has exactly 1 row), so `EXISTS`
-- is true for every outer row and the result is 3.
SELECT count() AS bot_repro
FROM numbers(3) AS o
WHERE EXISTS
(
    (SELECT o.number, 1 FROM system.one)
    UNION ALL
    (SELECT 1, 2 FROM system.one)
);

-- (2) Structural: mixed-eligibility subtree must be left intact. Branch 1
-- has `arrayJoin` in the projection so the all-or-nothing gate flips the
-- subtree to NOT strippable. With the gate the rewrite must NOT mutate any
-- leaf projection, so branch 2's PROJECTION list still contains the
-- correlated `column_name: number` node (rather than being collapsed to a
-- single `CONSTANT 1`). Branch 1's PROJECTION list still contains
-- `function_name: arrayJoin`. Both invariants are asserted via
-- `EXPLAIN QUERY TREE` and the counts on the test reference. The assertion
-- is robust against AST refactors that add or remove unrelated nodes: we
-- assert `>= 1` rather than an exact count.
SELECT
    countIf(explain LIKE '%function_name: arrayJoin%') >= 1 AS branch1_arrayjoin_preserved,
    countIf(explain LIKE '%column_name: number%') >= 1 AS branch2_correlated_ref_preserved
FROM
(
    EXPLAIN QUERY TREE passes=1
    SELECT 1 FROM numbers(3) AS o
    WHERE EXISTS
    (
        (SELECT arrayJoin([10, 20]), 1 FROM system.one)
        UNION ALL
        (SELECT o.number, 1 FROM system.one)
    )
);

-- (3) Sanity. All-correlated, all-strippable. The rewrite uniformly strips
-- both branches to `SELECT 1`, the surviving `UnionNode` is `(SELECT 1) UNION
-- ALL (SELECT 1)`, and `EXISTS` is true for every outer row. Pins that the
-- refactor of `tryStripExistsProjection` into `isExistsQueryNodeStrippable` +
-- `stripExistsQueryNodeProjection` did not change the all-correlated path.
SELECT count() AS all_correlated
FROM numbers(3) AS o
WHERE EXISTS
(
    (SELECT o.number, 1 FROM system.one)
    UNION ALL
    (SELECT 1, o.number FROM system.one)
);

-- (4) Sanity. Plain `QueryNode` body (the original #105760 path). The
-- refactor extracted `isExistsQueryNodeStrippable` and
-- `stripExistsQueryNodeProjection` from the old `tryStripExistsProjection`,
-- so this confirms the single-`QueryNode` case is unchanged.
SELECT count() AS plain_query
FROM numbers(3) AS o
WHERE EXISTS (SELECT o.number, 1 FROM system.one);
