-- Test: exercises `RecursiveCTEChunkGenerator` constructor column-count check
-- Covers: src/Processors/Sources/RecursiveCTESource.cpp:110-113 — SIZES_OF_COLUMNS_DOESNT_MATCH
-- when recursive subquery projection has different number of columns than non-recursive subquery.

SET enable_analyzer = 1;

-- Recursive subquery has more columns than non-recursive subquery.
WITH RECURSIVE x AS (SELECT 1 AS n UNION ALL SELECT n + 1, n + 2 FROM x WHERE n < 3)
SELECT * FROM x; -- { serverError SIZES_OF_COLUMNS_DOESNT_MATCH }

-- Non-recursive subquery has more columns than recursive subquery.
WITH RECURSIVE x AS (SELECT 1 AS n, 2 AS m UNION ALL SELECT n + 1 FROM x WHERE n < 3)
SELECT * FROM x; -- { serverError SIZES_OF_COLUMNS_DOESNT_MATCH }
