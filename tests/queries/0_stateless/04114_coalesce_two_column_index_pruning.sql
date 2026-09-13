-- Tags: no-replicated-database, no-parallel-replicas
-- no-replicated-database: EXPLAIN output differs for replicated database.
-- no-parallel-replicas: EXPLAIN output differs for parallel replicas.
-- See `src/Storages/MergeTree/KeyCondition.cpp::tryRewriteCoalesceComparison`.

SET parallel_replicas_local_plan = 1;
SET use_query_condition_cache = 0;
SET use_skip_indexes_on_data_read = 0;
SET use_skip_indexes = 1;
SET use_skip_indexes_for_disjunctions = 1;
SET allow_key_condition_coalesce_rewrite = 1;

SELECT '--- both columns indexed: coalesce equals prunes via skip indexes ---';

DROP TABLE IF EXISTS tab_both;

CREATE TABLE tab_both
(
    id UInt32,
    a Nullable(UInt32),
    b UInt32,
    INDEX a_idx a TYPE minmax GRANULARITY 1,
    INDEX b_idx b TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 64, index_granularity_bytes = 0,
    min_bytes_for_wide_part = 0,
    min_bytes_for_full_part_storage = 0,
    max_bytes_to_merge_at_max_space_in_pool = 1,
    add_minmax_index_for_numeric_columns = 0;

INSERT INTO tab_both
SELECT number,
       if(number % 3 = 0, NULL, toUInt32(intDiv(number, 1024))),
       toUInt32(intDiv(number, 1024) + 1000000)
FROM numbers(65536);

SELECT trimLeft(explain) FROM (
    EXPLAIN indexes = 1
    SELECT count() FROM tab_both WHERE coalesce(a, b) = 42
) WHERE explain ILIKE '%Granules:%' OR explain ILIKE '%Skip%' OR explain ILIKE '%Name:%';

-- Result-count cross-check: the rewrite must not change query semantics.
SELECT count() FROM tab_both WHERE coalesce(a, b) = 42;
SELECT count() FROM tab_both WHERE (a = 42) OR (a IS NULL AND b = 42);

-- With the rewrite disabled, we expect no skip-index pruning on coalesce.
SELECT trimLeft(explain) FROM (
    EXPLAIN indexes = 1
    SELECT count() FROM tab_both WHERE coalesce(a, b) = 42
    SETTINGS allow_key_condition_coalesce_rewrite = 0
) WHERE explain ILIKE '%Granules:%' OR explain ILIKE '%Skip%' OR explain ILIKE '%Name:%';

SELECT '--- ifNull behaves the same as coalesce ---';

SELECT trimLeft(explain) FROM (
    EXPLAIN indexes = 1
    SELECT count() FROM tab_both WHERE ifNull(a, b) = 42
) WHERE explain ILIKE '%Granules:%' OR explain ILIKE '%Skip%' OR explain ILIKE '%Name:%';

SELECT count() FROM tab_both WHERE ifNull(a, b) = 42;

SELECT '--- only one column indexed: partial pruning still works ---';

DROP TABLE IF EXISTS tab_one;

CREATE TABLE tab_one
(
    id UInt32,
    a Nullable(UInt32),
    b UInt32,
    INDEX a_idx a TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 64, index_granularity_bytes = 0,
    min_bytes_for_wide_part = 0,
    min_bytes_for_full_part_storage = 0,
    max_bytes_to_merge_at_max_space_in_pool = 1,
    add_minmax_index_for_numeric_columns = 0;

INSERT INTO tab_one
SELECT number,
       if(number % 3 = 0, NULL, toUInt32(intDiv(number, 1024))),
       toUInt32(intDiv(number, 1024) + 1000000)
FROM numbers(65536);

SELECT trimLeft(explain) FROM (
    EXPLAIN indexes = 1
    SELECT count() FROM tab_one WHERE coalesce(a, b) = 42
) WHERE explain ILIKE '%Skip%' OR explain ILIKE '%Name:%';

SELECT count() FROM tab_one WHERE coalesce(a, b) = 42;

SELECT '--- PK form: ORDER BY (a, id) prunes via PrimaryKey on a ---';

DROP TABLE IF EXISTS tab_pk;

CREATE TABLE tab_pk
(
    id UInt32,
    a UInt32,
    b UInt32
)
ENGINE = MergeTree
ORDER BY (a, id)
SETTINGS index_granularity = 64,
    min_bytes_for_wide_part = 0,
    max_bytes_to_merge_at_max_space_in_pool = 1,
    add_minmax_index_for_numeric_columns = 0;

INSERT INTO tab_pk
SELECT number, toUInt32(intDiv(number, 1024)), toUInt32(intDiv(number, 512) + 1000000)
FROM numbers(65536);

SELECT trimLeft(explain) FROM (
    EXPLAIN indexes = 1
    SELECT count() FROM tab_pk WHERE coalesce(a, b) = 42
) WHERE explain ILIKE '%PrimaryKey%' OR explain ILIKE '%Condition:%' OR explain ILIKE '%Granules:%';

SELECT count() FROM tab_pk WHERE coalesce(a, b) = 42;

SELECT '--- ordered comparison: coalesce(a, b) > K ---';

SELECT trimLeft(explain) FROM (
    EXPLAIN indexes = 1
    SELECT count() FROM tab_both WHERE coalesce(a, b) > 60
) WHERE explain ILIKE '%Granules:%' OR explain ILIKE '%Skip%' OR explain ILIKE '%Name:%';

SELECT count() FROM tab_both WHERE coalesce(a, b) > 60;
SELECT count() FROM tab_both WHERE (a > 60) OR (a IS NULL AND b > 60);

SELECT '--- PR #94754 regression: coalesce(key, const) = X still prunes via PK ---';

DROP TABLE IF EXISTS tab_monotone;

CREATE TABLE tab_monotone
(
    a Nullable(UInt32)
)
ENGINE = MergeTree
ORDER BY a
SETTINGS index_granularity = 64, allow_nullable_key = 1,
    min_bytes_for_wide_part = 0,
    max_bytes_to_merge_at_max_space_in_pool = 1,
    add_minmax_index_for_numeric_columns = 0;

INSERT INTO tab_monotone SELECT toUInt32(intDiv(number, 128)) FROM numbers(65536);

-- Only check that PK analysis fired and pruned to a small set of granules. The exact textual
-- form of `Condition:` depends on whether the constant-FALSE branch from the coalesce rewrite
-- (`isNull(a) AND (0 = 42)`) gets folded, which varies under randomized settings.
SELECT trimLeft(explain) FROM (
    EXPLAIN indexes = 1
    SELECT count() FROM tab_monotone WHERE coalesce(a, toUInt32(0)) = 42
) WHERE explain ILIKE '%PrimaryKey%' OR explain ILIKE '%Granules:%';

SELECT count() FROM tab_monotone WHERE coalesce(a, toUInt32(0)) = 42;

SELECT '--- negative operators must not regress (no pruning, correct results) ---';

SELECT count() FROM tab_both WHERE coalesce(a, b) != 42;
SELECT count() FROM tab_both WHERE (coalesce(a, b) IS NULL);

DROP TABLE IF EXISTS tab_both;
DROP TABLE IF EXISTS tab_one;
DROP TABLE IF EXISTS tab_pk;
DROP TABLE IF EXISTS tab_monotone;
