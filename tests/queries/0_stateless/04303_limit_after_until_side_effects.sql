-- Regression tests for LIMIT AFTER/UNTIL side effects:
--   1. IN (subquery) conditions must work in both paths.
--   2. rows_before_limit_at_least must count all chunks including those drained after done_outputting.
--   3. extremes are computed on the pre-range stream (both analyzer and legacy paths).
--   4. AFTER/UNTIL conditions may reference non-selected columns (legacy path, runs before projection).

SET allow_experimental_limit_after = 1;

-- 1. IN (subquery) must work in AFTER/UNTIL conditions (analyzer + legacy).
SELECT number FROM numbers(8) ORDER BY number LIMIT AFTER number IN (SELECT 3);
SELECT number FROM numbers(8) ORDER BY number LIMIT AFTER number IN (SELECT 3) SETTINGS enable_analyzer = 0;
SELECT number FROM numbers(8) ORDER BY number LIMIT AFTER number IN (SELECT 2) UNTIL number IN (SELECT 6);
SELECT number FROM numbers(8) ORDER BY number LIMIT AFTER number IN (SELECT 2) UNTIL number IN (SELECT 6) SETTINGS enable_analyzer = 0;

-- 2. rows_before_limit_at_least: all rows must be counted across multiple chunks.
--    max_block_size=1 ensures drained-after-done_outputting chunks are counted too.
SET output_format_write_statistics = 0;
SELECT number FROM numbers(10) ORDER BY number LIMIT 3 AFTER number >= 5
FORMAT JSONCompact
SETTINGS exact_rows_before_limit = 1, max_block_size = 1;

SELECT number FROM numbers(10) ORDER BY number LIMIT 3 AFTER number >= 5
FORMAT JSONCompact
SETTINGS exact_rows_before_limit = 1, max_block_size = 1, enable_analyzer = 0;

-- 3. extremes: min/max reflect the full pre-range stream (analyzer path).
SELECT number FROM numbers(10) ORDER BY number LIMIT 3 AFTER number >= 5
FORMAT JSONCompact
SETTINGS extremes = 1;

-- 3b. extremes on the legacy path: min/max must also reflect the full pre-range stream,
--     i.e. extremes are computed before the LIMIT ... AFTER range, not over the emitted rows.
SELECT number FROM numbers(10) ORDER BY number LIMIT 3 AFTER number >= 5
FORMAT JSONCompact
SETTINGS extremes = 1, enable_analyzer = 0;

-- 4. AFTER/UNTIL reference column y not in the SELECT list (the range runs before projection, which
--    would otherwise drop y). Both paths must keep y available for the boundary conditions.
SELECT x FROM (SELECT number AS x, number * 2 AS y FROM numbers(10)) ORDER BY x LIMIT 3 AFTER y >= 10 SETTINGS enable_analyzer = 0;
SELECT x FROM (SELECT number AS x, number * 2 AS y FROM numbers(10)) ORDER BY x LIMIT AFTER y >= 10 UNTIL y >= 14 SETTINGS enable_analyzer = 0;
SELECT x FROM (SELECT number AS x, number * 2 AS y FROM numbers(10)) ORDER BY x LIMIT 3 AFTER y >= 10 SETTINGS enable_analyzer = 1;
SELECT x FROM (SELECT number AS x, number * 2 AS y FROM numbers(10)) ORDER BY x LIMIT AFTER y >= 10 UNTIL y >= 14 SETTINGS enable_analyzer = 1;
