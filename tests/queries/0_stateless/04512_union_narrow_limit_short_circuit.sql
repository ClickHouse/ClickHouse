-- Regression test for issue #110382: max_streams_for_union_step must not narrow a UNION
-- that sits directly under a short-circuiting LIMIT. Narrowing routes the branches through
-- a ConcatProcessor that drains branch 0 fully before branch 1; with an infinite branch 0 the
-- outer LIMIT can never short-circuit and the query hangs. The narrowing is disabled in that
-- case, so the query below returns immediately instead of hanging.

SELECT number FROM
(
    SELECT number FROM system.numbers WHERE sipHash64(number) = 0
    UNION ALL
    SELECT 1 AS number
)
LIMIT 1
SETTINGS max_rows_to_read = 0, max_threads = 1, max_streams_for_union_step = 1;

-- The narrowing pipeline must not contain a Concat when a LIMIT can short-circuit the union.
SELECT count() FROM
(
    EXPLAIN PIPELINE
    SELECT number FROM (SELECT 1 AS number UNION ALL SELECT 2 UNION ALL SELECT 3) LIMIT 1
    SETTINGS max_threads = 4, max_streams_for_union_step = 1
)
WHERE explain ILIKE '%Concat%';

-- Without a short-circuiting LIMIT the memory cap still applies: narrowing produces a Concat.
SELECT count() > 0 FROM
(
    EXPLAIN PIPELINE
    SELECT number FROM (SELECT 1 AS number UNION ALL SELECT 2 UNION ALL SELECT 3)
    SETTINGS max_threads = 4, max_streams_for_union_step = 1
)
WHERE explain ILIKE '%Concat%';
