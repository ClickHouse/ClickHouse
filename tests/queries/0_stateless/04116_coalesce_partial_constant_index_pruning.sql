-- Tags: no-replicated-database, no-parallel-replicas
-- no-replicated-database: EXPLAIN output differs for replicated database.
-- no-parallel-replicas: EXPLAIN output differs for parallel replicas.
-- `coalesce(...) <op> const` with partial-constant arguments (NULL literals, middle/trailing constants).
-- See `src/Storages/MergeTree/KeyCondition.cpp::tryRewriteCoalesceComparison`.

SET parallel_replicas_local_plan = 1;
SET use_query_condition_cache = 0;
SET use_skip_indexes_on_data_read = 0;
SET use_skip_indexes = 1;
SET use_skip_indexes_for_disjunctions = 1;
SET allow_key_condition_coalesce_rewrite = 1;

DROP TABLE IF EXISTS tab;

CREATE TABLE tab
(
    id UInt32,
    a Nullable(UInt32),
    b Nullable(UInt32),
    c UInt32,
    INDEX a_idx a TYPE minmax GRANULARITY 1,
    INDEX b_idx b TYPE minmax GRANULARITY 1,
    INDEX c_idx c TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 64, index_granularity_bytes = 0,
    min_bytes_for_wide_part = 0,
    min_bytes_for_full_part_storage = 0,
    max_bytes_to_merge_at_max_space_in_pool = 1,
    add_minmax_index_for_numeric_columns = 0;

INSERT INTO tab
SELECT number,
       if(number % 3 = 0, NULL, toUInt32(intDiv(number, 1024))),
       if(number % 5 = 0, NULL, toUInt32(intDiv(number, 1024) + 100)),
       toUInt32(intDiv(number, 1024) + 1000000)
FROM numbers(65536);

SELECT '--- N=3 middle constant: `coalesce(a, 42, b)` -> `coalesce(a, 42)` after normalization ---';

-- With the rewrite: `(a = 42) OR (isNull(a) AND (42 = 42))` collapses to `a = 42 OR a IS NULL`.
-- The `b` column is unreachable and its skip index must not be consulted.
SELECT trimLeft(explain) FROM (
    EXPLAIN indexes = 1
    SELECT count() FROM tab WHERE coalesce(a, 42, b) = 42
) WHERE explain ILIKE '%Granules:%' OR explain ILIKE '%Skip%' OR explain ILIKE '%Name:%';

SELECT count() FROM tab WHERE coalesce(a, 42, b) = 42;
SELECT count() FROM tab WHERE (a = 42) OR (a IS NULL);

-- Off-baseline: no pruning when the rewrite is disabled.
SELECT trimLeft(explain) FROM (
    EXPLAIN indexes = 1
    SELECT count() FROM tab WHERE coalesce(a, 42, b) = 42
    SETTINGS allow_key_condition_coalesce_rewrite = 0
) WHERE explain ILIKE '%Granules:%' OR explain ILIKE '%Skip%' OR explain ILIKE '%Name:%';

SELECT '--- N=3 NULL literal in middle: `coalesce(a, NULL, b)` -> `coalesce(a, b)` ---';

-- The DAG still has three args; normalization drops the NULL literal so the rewrite becomes
-- `(a < 50) OR (isNull(a) AND b < 50)`. Cross-check equals the 2-arg form.
SELECT trimLeft(explain) FROM (
    EXPLAIN indexes = 1
    SELECT count() FROM tab WHERE coalesce(a, NULL, b) < 50
) WHERE explain ILIKE '%Granules:%' OR explain ILIKE '%Skip%' OR explain ILIKE '%Name:%';

SELECT count() FROM tab WHERE coalesce(a, NULL, b) < 50;
SELECT count() FROM tab WHERE coalesce(a, b) < 50;
SELECT count() FROM tab WHERE (a < 50) OR (a IS NULL AND b < 50);

SELECT '--- N=3 trailing constant, FALSE tail branch: `(isNull(a) AND isNull(b) AND 1000000 < 42)` drops ---';

-- Rewrite: `(a < 42) OR (isNull(a) AND b < 42) OR (isNull(a) AND isNull(b) AND (1000000 < 42))`.
-- The last branch folds to FALSE; per-column skip indexes on `a` and `b` still prune.
SELECT trimLeft(explain) FROM (
    EXPLAIN indexes = 1
    SELECT count() FROM tab WHERE coalesce(a, b, 1000000) < 42
) WHERE explain ILIKE '%Granules:%' OR explain ILIKE '%Skip%' OR explain ILIKE '%Name:%';

SELECT count() FROM tab WHERE coalesce(a, b, 1000000) < 42;
SELECT count() FROM tab WHERE (a < 42) OR (a IS NULL AND b < 42) OR (a IS NULL AND b IS NULL AND (1000000 < 42));

SELECT '--- N=3 trailing constant, TRUE tail branch: `(isNull(a) AND isNull(b) AND 0 >= 0)` stays ---';

-- Rewrite: `(a >= 0) OR (isNull(a) AND b >= 0) OR (isNull(a) AND isNull(b) AND (0 >= 0))`.
-- The last branch collapses to `isNull(a) AND isNull(b)`; semantics = "all rows" (a UInt32
-- column is always >= 0 when non-null, and the tail branch covers the both-NULL case).
SELECT count() FROM tab WHERE coalesce(a, b, toUInt32(0)) >= 0;
SELECT count() FROM tab WHERE (a >= 0) OR (a IS NULL AND b >= 0) OR (a IS NULL AND b IS NULL);

SELECT '--- N=4 with constant at position 3: `coalesce(a, b, 42, d)` -> `coalesce(a, b, 42)` ---';

DROP TABLE IF EXISTS tab4;

CREATE TABLE tab4
(
    id UInt32,
    a Nullable(UInt32),
    b Nullable(UInt32),
    d UInt32,
    INDEX a_idx a TYPE minmax GRANULARITY 1,
    INDEX b_idx b TYPE minmax GRANULARITY 1,
    INDEX d_idx d TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 64, index_granularity_bytes = 0,
    min_bytes_for_wide_part = 0,
    min_bytes_for_full_part_storage = 0,
    max_bytes_to_merge_at_max_space_in_pool = 1,
    add_minmax_index_for_numeric_columns = 0;

INSERT INTO tab4
SELECT number,
       if(number % 3 = 0, NULL, toUInt32(intDiv(number, 1024))),
       if(number % 5 = 0, NULL, toUInt32(intDiv(number, 1024) + 100)),
       toUInt32(intDiv(number, 1024) + 1000000)
FROM numbers(65536);

-- The `d_idx` must NOT appear: `d` is unreachable after normalization truncates at `42`.
SELECT trimLeft(explain) FROM (
    EXPLAIN indexes = 1
    SELECT count() FROM tab4 WHERE coalesce(a, b, 42, d) = 42
) WHERE explain ILIKE '%Name:%';

SELECT count() FROM tab4 WHERE coalesce(a, b, 42, d) = 42;
SELECT count() FROM tab4 WHERE (a = 42) OR (a IS NULL AND b = 42) OR (a IS NULL AND b IS NULL AND (42 = 42));

SELECT '--- Flipped operand form: `42 = coalesce(a, 100, b)` ---';

-- Rewrite via mirror op: `(a = 42) OR (isNull(a) AND (100 = 42))` -> `a = 42` (tail drops).
SELECT trimLeft(explain) FROM (
    EXPLAIN indexes = 1
    SELECT count() FROM tab WHERE 42 = coalesce(a, 100, b)
) WHERE explain ILIKE '%Granules:%' OR explain ILIKE '%Skip%' OR explain ILIKE '%Name:%';

SELECT count() FROM tab WHERE 42 = coalesce(a, 100, b);
SELECT count() FROM tab WHERE a = 42;

SELECT '--- Each ordered operator with trailing constant and semantics cross-check ---';

SELECT count() FROM tab WHERE coalesce(a, b, 0) = 0;
SELECT count() FROM tab WHERE (a = 0) OR (a IS NULL AND b = 0) OR (a IS NULL AND b IS NULL);

SELECT count() FROM tab WHERE coalesce(a, b, toUInt32(42)) < 10;
SELECT count() FROM tab WHERE (a < 10) OR (a IS NULL AND b < 10) OR (a IS NULL AND b IS NULL AND (42 < 10));

SELECT count() FROM tab WHERE coalesce(a, b, toUInt32(42)) > 60;
SELECT count() FROM tab WHERE (a > 60) OR (a IS NULL AND b > 60) OR (a IS NULL AND b IS NULL AND (42 > 60));

SELECT count() FROM tab WHERE coalesce(a, b, toUInt32(42)) <= 10;
SELECT count() FROM tab WHERE (a <= 10) OR (a IS NULL AND b <= 10) OR (a IS NULL AND b IS NULL AND (42 <= 10));

SELECT count() FROM tab WHERE coalesce(a, b, toUInt32(42)) >= 60;
SELECT count() FROM tab WHERE (a >= 60) OR (a IS NULL AND b >= 60) OR (a IS NULL AND b IS NULL AND (42 >= 60));

SELECT '--- ifNull(a, 42) covered by the same rewrite ---';

-- Rewrite: `(a = 42) OR (isNull(a) AND (42 = 42))` -> same as coalesce(a, 42).
SELECT count() FROM tab WHERE ifNull(a, 42) = 42;
SELECT count() FROM tab WHERE (a = 42) OR (a IS NULL);

SELECT '--- Negative regressions: non-rewriteable ops must still return correct results ---';

SELECT count() FROM tab WHERE coalesce(a, 42, b) != 42;
SELECT count() FROM tab WHERE coalesce(a, 42, b) IS NULL;

DROP TABLE IF EXISTS tab;
DROP TABLE IF EXISTS tab4;
