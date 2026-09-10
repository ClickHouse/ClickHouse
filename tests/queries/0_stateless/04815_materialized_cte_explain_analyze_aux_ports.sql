SET enable_analyzer = 1;
SET enable_materialized_cte = 1;

-- A materialized CTE read behind an IN-subquery sits under a DelayedPortsProcessor gate.
-- Completing such a pipeline with a sink used to discard the totals/extremes ports with a
-- node that has no outputs; such a node is seeded before the pipeline runs and closed the
-- gate's paired input, so the CTE was read before it had been materialized.
--
-- EXPLAIN ANALYZE is wrapped in viewExplain because its own output carries timings.

-- Each carrier asserts results, which are equal whether the CTE is materialized or inlined,
-- so each also asserts that its own shape is planned with materialization. An EXPLAIN ANALYZE
-- carrier does that in place: its report names the MaterializingCTEs step it ran.

SELECT 'explain analyze with extremes';
SELECT countIf(explain LIKE '%MaterializingCTEs%') > 0 FROM viewExplain('EXPLAIN ANALYZE', 'processors = 1', (
    WITH a AS MATERIALIZED (SELECT number AS id FROM numbers(10))
    SELECT id FROM a WHERE id IN (SELECT id FROM a)
)) SETTINGS extremes = 1;

SELECT 'explain analyze with totals';
SELECT countIf(explain LIKE '%MaterializingCTEs%') > 0 FROM viewExplain('EXPLAIN ANALYZE', 'processors = 1', (
    WITH a AS MATERIALIZED (SELECT number AS id FROM numbers(10))
    SELECT id FROM a WHERE id IN (SELECT id FROM a) GROUP BY id WITH TOTALS
));

SELECT 'explain analyze with totals and extremes';
SELECT countIf(explain LIKE '%MaterializingCTEs%') > 0 FROM viewExplain('EXPLAIN ANALYZE', 'processors = 1', (
    WITH a AS MATERIALIZED (SELECT number AS id FROM numbers(10))
    SELECT id FROM a WHERE id IN (SELECT id FROM a) GROUP BY id WITH TOTALS
)) SETTINGS extremes = 1;

SELECT 'explain analyze with limit and extremes';
SELECT countIf(explain LIKE '%MaterializingCTEs%') > 0 FROM viewExplain('EXPLAIN ANALYZE', 'processors = 1', (
    WITH a AS MATERIALIZED (SELECT number AS id FROM numbers(10))
    SELECT id FROM a WHERE id IN (SELECT id FROM a) ORDER BY id LIMIT 1
)) SETTINGS extremes = 1;

-- Results must be unchanged: the totals/extremes streams are still discarded, and the
-- streams that are kept are still produced in full. These carriers return rows rather than a
-- plan, so each is preceded by an assertion that its own shape is planned with materialization.
SELECT 'select with extremes';
SELECT countIf(explain LIKE '%MaterializingCTETransform%') > 0 FROM viewExplain('EXPLAIN PIPELINE', '', (
    WITH a AS MATERIALIZED (SELECT number AS id FROM numbers(10))
    SELECT id FROM a WHERE id IN (SELECT id FROM a) ORDER BY id
)) SETTINGS extremes = 1;
WITH a AS MATERIALIZED (SELECT number AS id FROM numbers(10))
SELECT id FROM a WHERE id IN (SELECT id FROM a) ORDER BY id
SETTINGS extremes = 1;

SELECT 'select with totals';
SELECT countIf(explain LIKE '%MaterializingCTETransform%') > 0 FROM viewExplain('EXPLAIN PIPELINE', '', (
    WITH a AS MATERIALIZED (SELECT number AS id FROM numbers(5))
    SELECT id FROM a WHERE id IN (SELECT id FROM a) GROUP BY id WITH TOTALS ORDER BY id
));
WITH a AS MATERIALIZED (SELECT number AS id FROM numbers(5))
SELECT id FROM a WHERE id IN (SELECT id FROM a) GROUP BY id WITH TOTALS ORDER BY id;

SELECT 'select with limit 0 and extremes';
SELECT countIf(explain LIKE '%MaterializingCTETransform%') > 0 FROM viewExplain('EXPLAIN PIPELINE', '', (
    WITH a AS MATERIALIZED (SELECT number AS id FROM numbers(10))
    SELECT id FROM a WHERE id IN (SELECT id FROM a) ORDER BY id LIMIT 0
)) SETTINGS extremes = 1;
WITH a AS MATERIALIZED (SELECT number AS id FROM numbers(10))
SELECT id FROM a WHERE id IN (SELECT id FROM a) ORDER BY id LIMIT 0
SETTINGS extremes = 1;

SELECT 'select with limit 1 and extremes';
SELECT countIf(explain LIKE '%MaterializingCTETransform%') > 0 FROM viewExplain('EXPLAIN PIPELINE', '', (
    WITH a AS MATERIALIZED (SELECT number AS id FROM numbers(10))
    SELECT id FROM a WHERE id IN (SELECT id FROM a) ORDER BY id LIMIT 1
)) SETTINGS extremes = 1;
WITH a AS MATERIALIZED (SELECT number AS id FROM numbers(10))
SELECT id FROM a WHERE id IN (SELECT id FROM a) ORDER BY id LIMIT 1
SETTINGS extremes = 1;

SELECT 'in subquery without a materialized cte';
SELECT number FROM numbers(5) WHERE number IN (SELECT number FROM numbers(3)) ORDER BY number
SETTINGS extremes = 1;
