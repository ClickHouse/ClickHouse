-- Tags: no-replicated-database, no-parallel-replicas
-- no-replicated-database: EXPLAIN output differs for replicated database.
-- no-parallel-replicas: EXPLAIN output differs for parallel replicas.
-- `coalesce(a_1, ..., a_N) <op> const` with N > 2.
-- See `src/Storages/MergeTree/KeyCondition.cpp::tryRewriteCoalesceComparison`.

SET parallel_replicas_local_plan = 1;
SET use_query_condition_cache = 0;
SET use_skip_indexes_on_data_read = 0;
SET use_skip_indexes = 1;
SET use_skip_indexes_for_disjunctions = 1;
SET allow_key_condition_coalesce_rewrite = 1;

SELECT '--- N=3: all columns indexed, equals prunes via each skip index ---';

DROP TABLE IF EXISTS tab3;

CREATE TABLE tab3
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

INSERT INTO tab3
SELECT number,
       if(number % 3 = 0, NULL, toUInt32(intDiv(number, 1024))),
       if(number % 5 = 0, NULL, toUInt32(intDiv(number, 1024) + 100)),
       toUInt32(intDiv(number, 1024) + 1000000)
FROM numbers(65536);

SELECT trimLeft(explain) FROM (
    EXPLAIN indexes = 1
    SELECT count() FROM tab3 WHERE coalesce(a, b, c) = 42
) WHERE explain ILIKE '%Granules:%' OR explain ILIKE '%Skip%' OR explain ILIKE '%Name:%';

-- Semantics cross-check: rewrite must not change the answer.
SELECT count() FROM tab3 WHERE coalesce(a, b, c) = 42;
SELECT count() FROM tab3 WHERE (a = 42) OR (a IS NULL AND b = 42) OR (a IS NULL AND b IS NULL AND c = 42);

-- With the rewrite disabled, there must be no skip-index pruning on coalesce.
SELECT trimLeft(explain) FROM (
    EXPLAIN indexes = 1
    SELECT count() FROM tab3 WHERE coalesce(a, b, c) = 42
    SETTINGS allow_key_condition_coalesce_rewrite = 0
) WHERE explain ILIKE '%Granules:%' OR explain ILIKE '%Skip%' OR explain ILIKE '%Name:%';

SELECT '--- N=3: flipped-argument form (const on the left) ---';

SELECT trimLeft(explain) FROM (
    EXPLAIN indexes = 1
    SELECT count() FROM tab3 WHERE 42 = coalesce(a, b, c)
) WHERE explain ILIKE '%Granules:%' OR explain ILIKE '%Skip%' OR explain ILIKE '%Name:%';

SELECT count() FROM tab3 WHERE 42 = coalesce(a, b, c);

SELECT '--- N=3: only middle column indexed, partial pruning still works ---';

DROP TABLE IF EXISTS tab3_mid;

CREATE TABLE tab3_mid
(
    id UInt32,
    a Nullable(UInt32),
    b Nullable(UInt32),
    c UInt32,
    INDEX b_idx b TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 64, index_granularity_bytes = 0,
    min_bytes_for_wide_part = 0,
    min_bytes_for_full_part_storage = 0,
    max_bytes_to_merge_at_max_space_in_pool = 1,
    add_minmax_index_for_numeric_columns = 0;

INSERT INTO tab3_mid
SELECT number,
       if(number % 3 = 0, NULL, toUInt32(intDiv(number, 1024))),
       if(number % 5 = 0, NULL, toUInt32(intDiv(number, 1024) + 100)),
       toUInt32(intDiv(number, 1024) + 1000000)
FROM numbers(65536);

SELECT trimLeft(explain) FROM (
    EXPLAIN indexes = 1
    SELECT count() FROM tab3_mid WHERE coalesce(a, b, c) = 142
) WHERE explain ILIKE '%Skip%' OR explain ILIKE '%Name:%';

SELECT count() FROM tab3_mid WHERE coalesce(a, b, c) = 142;

SELECT '--- N=3: ordered operators prune and preserve semantics ---';

SELECT count() FROM tab3 WHERE coalesce(a, b, c) < 10;
SELECT count() FROM tab3 WHERE (a < 10) OR (a IS NULL AND b < 10) OR (a IS NULL AND b IS NULL AND c < 10);

SELECT count() FROM tab3 WHERE coalesce(a, b, c) > 60;
SELECT count() FROM tab3 WHERE (a > 60) OR (a IS NULL AND b > 60) OR (a IS NULL AND b IS NULL AND c > 60);

SELECT count() FROM tab3 WHERE coalesce(a, b, c) <= 10;
SELECT count() FROM tab3 WHERE (a <= 10) OR (a IS NULL AND b <= 10) OR (a IS NULL AND b IS NULL AND c <= 10);

SELECT count() FROM tab3 WHERE coalesce(a, b, c) >= 60;
SELECT count() FROM tab3 WHERE (a >= 60) OR (a IS NULL AND b >= 60) OR (a IS NULL AND b IS NULL AND c >= 60);

SELECT '--- N=4: four columns indexed, equals prunes via combined skip indexes ---';

DROP TABLE IF EXISTS tab4;

CREATE TABLE tab4
(
    id UInt32,
    a Nullable(UInt32),
    b Nullable(UInt32),
    c Nullable(UInt32),
    d UInt32,
    INDEX a_idx a TYPE minmax GRANULARITY 1,
    INDEX b_idx b TYPE minmax GRANULARITY 1,
    INDEX c_idx c TYPE minmax GRANULARITY 1,
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
       if(number % 7 = 0, NULL, toUInt32(intDiv(number, 1024) + 200)),
       toUInt32(intDiv(number, 1024) + 1000000)
FROM numbers(65536);

SELECT trimLeft(explain) FROM (
    EXPLAIN indexes = 1
    SELECT count() FROM tab4 WHERE coalesce(a, b, c, d) = 42
) WHERE explain ILIKE '%<Combined skip indexes>%';

SELECT count() FROM tab4 WHERE coalesce(a, b, c, d) = 42;
SELECT count() FROM tab4
WHERE (a = 42)
   OR (a IS NULL AND b = 42)
   OR (a IS NULL AND b IS NULL AND c = 42)
   OR (a IS NULL AND b IS NULL AND c IS NULL AND d = 42);

SELECT '--- N=3: negative operators must not regress ---';

SELECT count() FROM tab3 WHERE coalesce(a, b, c) != 42;
SELECT count() FROM tab3 WHERE (coalesce(a, b, c) IS NULL);

DROP TABLE IF EXISTS tab3;
DROP TABLE IF EXISTS tab3_mid;
DROP TABLE IF EXISTS tab4;
