-- Regression tests for LIMIT AFTER/UNTIL side effects:
--   1. IN (subquery) conditions must work.
--   2. rows_before_limit_at_least must count all chunks including those drained after done_outputting.
--   3. extremes are computed on the pre-range stream.
--   4. AFTER/UNTIL conditions may reference non-selected columns, because the range runs before
--      projection.

-- 1. IN (subquery) must work in AFTER/UNTIL conditions.
SELECT number FROM numbers(8) ORDER BY number LIMIT AFTER number IN (SELECT 3);
SELECT number FROM numbers(8) ORDER BY number LIMIT AFTER number IN (SELECT 2) UNTIL number IN (SELECT 6);

-- 2. rows_before_limit_at_least: all rows must be counted across multiple chunks.
--    max_block_size=1 ensures drained-after-done_outputting chunks are counted too.
SET output_format_write_statistics = 0;
SELECT number FROM numbers(10) ORDER BY number LIMIT 3 AFTER number >= 5
FORMAT JSONCompact
SETTINGS exact_rows_before_limit = 1, max_block_size = 1;

-- 3. extremes: min/max reflect the full pre-range stream.
SELECT number FROM numbers(10) ORDER BY number LIMIT 3 AFTER number >= 5
FORMAT JSONCompact
SETTINGS extremes = 1;

-- 4. AFTER/UNTIL reference column y not in the SELECT list (the range runs before projection, which
--    would otherwise drop y). y must stay available for the boundary conditions.
SELECT x FROM (SELECT number AS x, number * 2 AS y FROM numbers(10)) ORDER BY x LIMIT 3 AFTER y >= 10 SETTINGS enable_analyzer = 1;
SELECT x FROM (SELECT number AS x, number * 2 AS y FROM numbers(10)) ORDER BY x LIMIT AFTER y >= 10 UNTIL y >= 14 SETTINGS enable_analyzer = 1;
