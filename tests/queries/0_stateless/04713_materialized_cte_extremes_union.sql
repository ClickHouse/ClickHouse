SET enable_analyzer = 1;
SET enable_materialized_cte = 1;

-- `extremes` is pinned per statement: a runner-injected `extremes = 0` removes the extremes
-- plumbing entirely and would make every query below pass without exercising the bug.
-- The UNION ALL arms below deliberately produce identical row sets, because the order in which
-- arms reach the output is not deterministic.

-- the reported shape: UNION ALL + IN-subquery over a materialized CTE
WITH a AS MATERIALIZED (SELECT number AS id FROM numbers(10))
SELECT id FROM a WHERE id IN (SELECT id FROM a)
UNION ALL
SELECT id FROM a
ORDER BY id
SETTINGS extremes = 1;

-- INTERSECT sibling (same unitePipes path via IntersectOrExceptStep)
WITH a AS MATERIALIZED (SELECT number AS id FROM numbers(10))
SELECT id FROM a WHERE id IN (SELECT id FROM a)
INTERSECT
SELECT id FROM a
ORDER BY id
SETTINGS extremes = 1;

-- EXCEPT sibling
WITH a AS MATERIALIZED (SELECT number AS id FROM numbers(20))
SELECT id FROM a WHERE id IN (SELECT id FROM a)
EXCEPT
SELECT id FROM a WHERE id < 5
ORDER BY id
SETTINGS extremes = 1;

-- extremes-of-extremes over a 3-branch union (exercises Resize with more than 2 inputs)
WITH a AS MATERIALIZED (SELECT number AS id FROM numbers(10))
SELECT id FROM a WHERE id IN (SELECT id FROM a)
UNION ALL
SELECT id FROM a
UNION ALL
SELECT id FROM a
ORDER BY id
SETTINGS extremes = 1;

-- aggregating arms: extremes over a narrower value range than the CTE rows
WITH a AS MATERIALIZED (SELECT number AS id FROM numbers(10))
SELECT count() FROM a WHERE id IN (SELECT id FROM a)
UNION ALL
SELECT count() FROM a
SETTINGS extremes = 1;

-- The arms above have coinciding extremes, so they only prove the united extremes port exists.
-- The two below assert the merge ACROSS arms: their extremes are unreachable from any one arm.
-- The set operation must stay top level, or an outer ExtremesStep recomputes over final rows.

-- max comes only from the excluded second arm: arms (0,4) and (15,19), rows {0..4}
WITH a AS MATERIALIZED (SELECT number AS id FROM numbers(20))
SELECT id FROM a WHERE id IN (SELECT id FROM a) AND id < 5
EXCEPT
SELECT id FROM a WHERE id >= 15
ORDER BY id
SETTINGS extremes = 1;

-- three arms, min and max each from a different one: (5,9), (0,9), (5,19), rows {5..9}
WITH a AS MATERIALIZED (SELECT number AS id FROM numbers(20))
SELECT id FROM a WHERE id IN (SELECT id FROM a) AND id >= 5 AND id < 10
INTERSECT
SELECT id FROM a WHERE id < 10
INTERSECT
SELECT id FROM a WHERE id >= 5
ORDER BY id
SETTINGS extremes = 1;
