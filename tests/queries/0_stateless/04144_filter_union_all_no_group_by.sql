-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/65630
--
-- A WHERE filter applied on top of a UNION ALL whose branches have a constant
-- column alias and no explicit GROUP BY used to return one row per non-matching
-- branch with the aggregate evaluated over an empty set (count = 0). The cause
-- was the planner pushing the WHERE predicate down into each UNION ALL branch:
-- inside a branch the predicate evaluates against the branch's constant alias
-- and becomes false, which filters out the source rows; the implicit (no
-- GROUP BY) aggregation then still emits a single row with count = 0 per
-- non-matching branch. The expected behaviour is that WHERE is applied on top
-- of the UNION, so non-matching branches are dropped entirely.

-- Direct outer WHERE on UNION ALL: should return only the matching branch.
SELECT * FROM
(
    SELECT 'number1' AS id, count(number) AS cnt FROM numbers(5)
    UNION ALL
    SELECT 'number2', count(number) FROM numbers(4)
    UNION ALL
    SELECT 'number3', count(number) FROM numbers(3)
)
WHERE id = 'number1';

SELECT '--- with LIMIT (pushdown blocker) ---';

-- Wrapping in a LIMIT was originally suggested as a workaround because LIMIT
-- blocks predicate pushdown. It must keep producing the same single row.
SELECT * FROM
(
    SELECT * FROM
    (
        SELECT 'number1' AS id, count(number) AS cnt FROM numbers(5)
        UNION ALL
        SELECT 'number2', count(number) FROM numbers(4)
        UNION ALL
        SELECT 'number3', count(number) FROM numbers(3)
    )
    LIMIT 100000000
)
WHERE id = 'number1';

SELECT '--- filter on aggregate value ---';

-- Filtering by the aggregate column (post-aggregation) was never affected by
-- the pushdown bug, but is included so that any future change to predicate
-- pushdown rules cannot silently regress it.
SELECT * FROM
(
    SELECT 'number1' AS id, count(number) AS cnt FROM numbers(5)
    UNION ALL
    SELECT 'number2', count(number) FROM numbers(4)
    UNION ALL
    SELECT 'number3', count(number) FROM numbers(3)
)
WHERE cnt = 3
ORDER BY id;

SELECT '--- no match returns empty ---';

-- A predicate that excludes every branch must produce an empty result, not a
-- row of zeroed aggregates per branch.
SELECT * FROM
(
    SELECT 'number1' AS id, count(number) AS cnt FROM numbers(5)
    UNION ALL
    SELECT 'number2', count(number) FROM numbers(4)
    UNION ALL
    SELECT 'number3', count(number) FROM numbers(3)
)
WHERE id = 'nope';
