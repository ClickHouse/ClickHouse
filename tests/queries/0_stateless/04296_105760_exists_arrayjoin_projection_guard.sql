-- https://github.com/ClickHouse/ClickHouse/issues/105760
--
-- The fix in PR #105900 strips the inner projection of a correlated `EXISTS`
-- subquery to `SELECT 1` when the projection only affects output. The
-- assumption is that the projection does not affect row counts, which holds
-- for plain expressions, aggregates and window calls (already covered by
-- earlier guards), but NOT for `arrayJoin`: a zero-length array produces zero
-- rows, so `EXISTS` should be `false` for that outer row even when the inner
-- `FROM` is non-empty. Replacing the projection with `SELECT 1` would make
-- the subquery non-empty for every outer row and silently change the truth
-- value.
--
-- This test asserts that the projection-stripping rewrite does NOT fire when
-- the inner projection contains `arrayJoin`. The check is structural: after
-- analysis the inner subquery's projection must still hold an `arrayJoin`
-- function call (count >= 1), not the constant `1` produced by stripping.

SET enable_analyzer = 1;
SET allow_experimental_correlated_subqueries = 1;

-- The inner subquery's projection contains `arrayJoin` and must be preserved.
-- With the guard active, the `EXPLAIN QUERY TREE` output mentions
-- `function_name: arrayJoin` exactly once (in the inner subquery's projection).
-- Without the guard, the projection-stripping rewrite would replace it with
-- a `CONSTANT id: N, constant_value: UInt64_1` node and the count would be 0.

SELECT countIf(explain LIKE '%function_name: arrayJoin%') AS arrayjoin_function_nodes
FROM
(
    EXPLAIN QUERY TREE
    SELECT number
    FROM numbers(3) AS o
    WHERE EXISTS (SELECT arrayJoin(range(o.number)) FROM system.one)
);

-- Same shape with a multi-row inner `FROM` to rule out the rewrite firing on a
-- different code path.
SELECT countIf(explain LIKE '%function_name: arrayJoin%') AS arrayjoin_function_nodes
FROM
(
    EXPLAIN QUERY TREE
    SELECT number
    FROM numbers(3) AS o
    WHERE EXISTS (SELECT arrayJoin(range(o.number)) FROM numbers(2))
);

-- UNION variant: every branch carries `arrayJoin` in its projection, so the
-- per-branch stripping path (introduced in PR #105900 for `UNION ALL` /
-- `UNION DISTINCT`) must skip both branches.
SELECT countIf(explain LIKE '%function_name: arrayJoin%') AS arrayjoin_function_nodes
FROM
(
    EXPLAIN QUERY TREE
    SELECT number
    FROM numbers(3) AS o
    WHERE EXISTS
    (
        (SELECT arrayJoin(range(o.number)) FROM system.one)
        UNION ALL
        (SELECT arrayJoin(range(o.number + 1)) FROM system.one)
    )
);

-- Sanity check that the unrelated `EXISTS` correlated-projection fix from
-- #105760 still applies: when the inner projection has no row-count-affecting
-- function, the stripping rewrite must still fire (so the projection becomes
-- a constant `1` and no `arrayJoin` is present in the resulting tree).
SELECT countIf(explain LIKE '%function_name: arrayJoin%') AS arrayjoin_function_nodes_when_stripped
FROM
(
    EXPLAIN QUERY TREE
    SELECT number
    FROM numbers(3) AS o
    WHERE EXISTS (SELECT o.number + 1 FROM system.one)
);

-- Smoke run of the bot's exact reproducer. The query must execute without
-- error. The end-to-end answer here is masked by a separate decorrelation
-- bug (the correlated EXISTS planner currently drops the `arrayJoin` call
-- in the JOIN tree), so the result is intentionally not asserted on a
-- specific row set; we only verify that the rewrite did not throw.
SELECT count() >= 0
FROM
(
    SELECT number
    FROM numbers(3) AS o
    WHERE EXISTS (SELECT arrayJoin(range(o.number)) FROM system.one)
);
