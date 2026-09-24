-- An enum label and its stored number identify different rows. Transforming the constant must
-- preserve its active type inside `Dynamic` and `Variant`, including the shared `Dynamic` variant.
-- Pin the range endpoints and selected granules, then enforce those granule counts with read limits.
-- The zero-valued enum member lets `accurateCastOrNull` prepare conversion to `String`; the tested
-- member has the label '37' and stored number 3.
SET explain_query_plan_default = 'legacy';
SET enable_parallel_replicas = 0;
SET use_query_condition_cache = 0;
SET optimize_use_projections = 0;
SET optimize_trivial_count_query = 0;
SET optimize_trivial_count_with_sparsity_filter = 0;
SET optimize_move_to_prewhere = 0;
SET use_primary_key = 1;
SET use_skip_indexes = 0;
SET max_threads = 1;

DROP TABLE IF EXISTS test_typed_constant_direct;
CREATE TABLE test_typed_constant_direct (v String) ENGINE = MergeTree ORDER BY v
SETTINGS index_granularity = 1, index_granularity_bytes = 0,
    min_rows_for_wide_part = 0, min_bytes_for_wide_part = 0,
    add_minmax_index_for_numeric_columns = 0;
INSERT INTO test_typed_constant_direct SELECT toString(number) FROM numbers(100);

SET analyze_index_with_multiple_key_columns_per_condition = 0;

SELECT 'direct, multiple_atoms=0, Enum8, Dynamic';
SELECT trimLeft(explain) FROM
(
    EXPLAIN indexes = 1
    SELECT v FROM test_typed_constant_direct
    WHERE v = CAST(CAST('37' AS Enum8('zero' = 0, '37' = 3)) AS Dynamic)
)
WHERE explain LIKE '%Condition:%' OR explain LIKE '%Granules:%/%';
SELECT v FROM test_typed_constant_direct
WHERE v = CAST(CAST('37' AS Enum8('zero' = 0, '37' = 3)) AS Dynamic)
SETTINGS max_rows_to_read = 100;

SET analyze_index_with_multiple_key_columns_per_condition = 1;

SELECT 'direct, multiple_atoms=1, Enum8, Dynamic';
SELECT trimLeft(explain) FROM
(
    EXPLAIN indexes = 1
    SELECT v FROM test_typed_constant_direct
    WHERE v = CAST(CAST('37' AS Enum8('zero' = 0, '37' = 3)) AS Dynamic)
)
WHERE explain LIKE '%Condition:%' OR explain LIKE '%Granules:%/%';
SELECT v FROM test_typed_constant_direct
WHERE v = CAST(CAST('37' AS Enum8('zero' = 0, '37' = 3)) AS Dynamic)
SETTINGS max_rows_to_read = 2;

SELECT 'direct, multiple_atoms=1, Enum8, Dynamic(max_types = 0)';
SELECT trimLeft(explain) FROM
(
    EXPLAIN indexes = 1
    SELECT v FROM test_typed_constant_direct
    WHERE v = CAST(CAST('37' AS Enum8('zero' = 0, '37' = 3)) AS Dynamic(max_types = 0))
)
WHERE explain LIKE '%Condition:%' OR explain LIKE '%Granules:%/%';
SELECT v FROM test_typed_constant_direct
WHERE v = CAST(CAST('37' AS Enum8('zero' = 0, '37' = 3)) AS Dynamic(max_types = 0))
SETTINGS max_rows_to_read = 2;

SELECT 'direct, multiple_atoms=1, Enum8, Variant';
SELECT trimLeft(explain) FROM
(
    EXPLAIN indexes = 1
    SELECT v FROM test_typed_constant_direct
    WHERE v = CAST(CAST('37' AS Enum8('zero' = 0, '37' = 3)) AS Variant(Array(UInt8), Enum8('zero' = 0, '37' = 3)))
)
WHERE explain LIKE '%Condition:%' OR explain LIKE '%Granules:%/%';
SELECT v FROM test_typed_constant_direct
WHERE v = CAST(CAST('37' AS Enum8('zero' = 0, '37' = 3)) AS Variant(Array(UInt8), Enum8('zero' = 0, '37' = 3)))
SETTINGS max_rows_to_read = 2;

SELECT 'direct, multiple_atoms=1, Enum16, Dynamic';
SELECT trimLeft(explain) FROM
(
    EXPLAIN indexes = 1
    SELECT v FROM test_typed_constant_direct
    WHERE v = CAST(CAST('37' AS Enum16('zero' = 0, '37' = 3)) AS Dynamic)
)
WHERE explain LIKE '%Condition:%' OR explain LIKE '%Granules:%/%';
SELECT v FROM test_typed_constant_direct
WHERE v = CAST(CAST('37' AS Enum16('zero' = 0, '37' = 3)) AS Dynamic)
SETTINGS max_rows_to_read = 2;

SELECT 'direct, multiple_atoms=1, Enum16, Dynamic(max_types = 0)';
SELECT trimLeft(explain) FROM
(
    EXPLAIN indexes = 1
    SELECT v FROM test_typed_constant_direct
    WHERE v = CAST(CAST('37' AS Enum16('zero' = 0, '37' = 3)) AS Dynamic(max_types = 0))
)
WHERE explain LIKE '%Condition:%' OR explain LIKE '%Granules:%/%';
SELECT v FROM test_typed_constant_direct
WHERE v = CAST(CAST('37' AS Enum16('zero' = 0, '37' = 3)) AS Dynamic(max_types = 0))
SETTINGS max_rows_to_read = 2;

SELECT 'direct, multiple_atoms=1, Enum16, Variant';
SELECT trimLeft(explain) FROM
(
    EXPLAIN indexes = 1
    SELECT v FROM test_typed_constant_direct
    WHERE v = CAST(CAST('37' AS Enum16('zero' = 0, '37' = 3)) AS Variant(Array(UInt8), Enum16('zero' = 0, '37' = 3)))
)
WHERE explain LIKE '%Condition:%' OR explain LIKE '%Granules:%/%';
SELECT v FROM test_typed_constant_direct
WHERE v = CAST(CAST('37' AS Enum16('zero' = 0, '37' = 3)) AS Variant(Array(UInt8), Enum16('zero' = 0, '37' = 3)))
SETTINGS max_rows_to_read = 2;

DROP TABLE test_typed_constant_direct;

DROP TABLE IF EXISTS test_typed_constant_derived;
CREATE TABLE test_typed_constant_derived (v String) ENGINE = MergeTree ORDER BY (reverse(v), v)
SETTINGS index_granularity = 1, index_granularity_bytes = 0,
    min_rows_for_wide_part = 0, min_bytes_for_wide_part = 0,
    add_minmax_index_for_numeric_columns = 0;
INSERT INTO test_typed_constant_derived SELECT toString(number) FROM numbers(100);

-- Derived-key cases put the constant on the left to exercise comparison normalization.
SET analyze_index_with_multiple_key_columns_per_condition = 0;

SELECT 'derived, multiple_atoms=0, Enum8, Dynamic';
SELECT trimLeft(explain) FROM
(
    EXPLAIN indexes = 1
    SELECT v FROM test_typed_constant_derived
    WHERE CAST(CAST('37' AS Enum8('zero' = 0, '37' = 3)) AS Dynamic) = v
)
WHERE explain LIKE '%Condition:%' OR explain LIKE '%Granules:%/%';
SELECT v FROM test_typed_constant_derived
WHERE CAST(CAST('37' AS Enum8('zero' = 0, '37' = 3)) AS Dynamic) = v
SETTINGS max_rows_to_read = 100;

SET analyze_index_with_multiple_key_columns_per_condition = 1;

SELECT 'derived, multiple_atoms=1, Enum8, Dynamic';
SELECT trimLeft(explain) FROM
(
    EXPLAIN indexes = 1
    SELECT v FROM test_typed_constant_derived
    WHERE CAST(CAST('37' AS Enum8('zero' = 0, '37' = 3)) AS Dynamic) = v
)
WHERE explain LIKE '%Condition:%' OR explain LIKE '%Granules:%/%';
SELECT v FROM test_typed_constant_derived
WHERE CAST(CAST('37' AS Enum8('zero' = 0, '37' = 3)) AS Dynamic) = v
SETTINGS max_rows_to_read = 2;

SELECT 'derived, multiple_atoms=1, Enum8, Dynamic(max_types = 0)';
SELECT trimLeft(explain) FROM
(
    EXPLAIN indexes = 1
    SELECT v FROM test_typed_constant_derived
    WHERE CAST(CAST('37' AS Enum8('zero' = 0, '37' = 3)) AS Dynamic(max_types = 0)) = v
)
WHERE explain LIKE '%Condition:%' OR explain LIKE '%Granules:%/%';
SELECT v FROM test_typed_constant_derived
WHERE CAST(CAST('37' AS Enum8('zero' = 0, '37' = 3)) AS Dynamic(max_types = 0)) = v
SETTINGS max_rows_to_read = 2;

SELECT 'derived, multiple_atoms=1, Enum8, Variant';
SELECT trimLeft(explain) FROM
(
    EXPLAIN indexes = 1
    SELECT v FROM test_typed_constant_derived
    WHERE CAST(CAST('37' AS Enum8('zero' = 0, '37' = 3)) AS Variant(Array(UInt8), Enum8('zero' = 0, '37' = 3))) = v
)
WHERE explain LIKE '%Condition:%' OR explain LIKE '%Granules:%/%';
SELECT v FROM test_typed_constant_derived
WHERE CAST(CAST('37' AS Enum8('zero' = 0, '37' = 3)) AS Variant(Array(UInt8), Enum8('zero' = 0, '37' = 3))) = v
SETTINGS max_rows_to_read = 2;

SELECT 'derived, multiple_atoms=1, Enum16, Dynamic';
SELECT trimLeft(explain) FROM
(
    EXPLAIN indexes = 1
    SELECT v FROM test_typed_constant_derived
    WHERE CAST(CAST('37' AS Enum16('zero' = 0, '37' = 3)) AS Dynamic) = v
)
WHERE explain LIKE '%Condition:%' OR explain LIKE '%Granules:%/%';
SELECT v FROM test_typed_constant_derived
WHERE CAST(CAST('37' AS Enum16('zero' = 0, '37' = 3)) AS Dynamic) = v
SETTINGS max_rows_to_read = 2;

SELECT 'derived, multiple_atoms=1, Enum16, Dynamic(max_types = 0)';
SELECT trimLeft(explain) FROM
(
    EXPLAIN indexes = 1
    SELECT v FROM test_typed_constant_derived
    WHERE CAST(CAST('37' AS Enum16('zero' = 0, '37' = 3)) AS Dynamic(max_types = 0)) = v
)
WHERE explain LIKE '%Condition:%' OR explain LIKE '%Granules:%/%';
SELECT v FROM test_typed_constant_derived
WHERE CAST(CAST('37' AS Enum16('zero' = 0, '37' = 3)) AS Dynamic(max_types = 0)) = v
SETTINGS max_rows_to_read = 2;

SELECT 'derived, multiple_atoms=1, Enum16, Variant';
SELECT trimLeft(explain) FROM
(
    EXPLAIN indexes = 1
    SELECT v FROM test_typed_constant_derived
    WHERE CAST(CAST('37' AS Enum16('zero' = 0, '37' = 3)) AS Variant(Array(UInt8), Enum16('zero' = 0, '37' = 3))) = v
)
WHERE explain LIKE '%Condition:%' OR explain LIKE '%Granules:%/%';
SELECT v FROM test_typed_constant_derived
WHERE CAST(CAST('37' AS Enum16('zero' = 0, '37' = 3)) AS Variant(Array(UInt8), Enum16('zero' = 0, '37' = 3))) = v
SETTINGS max_rows_to_read = 2;

DROP TABLE test_typed_constant_derived;

-- An inequality exercises the monotonic transform without considering deterministic equality atoms.
-- The relaxed bound includes '37', so pruning reads 32 one-row granules and filtering returns 31 rows.
DROP TABLE IF EXISTS test_typed_constant_monotonic;
CREATE TABLE test_typed_constant_monotonic (v String) ENGINE = MergeTree ORDER BY (toString(v), v)
SETTINGS index_granularity = 1, index_granularity_bytes = 0,
    min_rows_for_wide_part = 0, min_bytes_for_wide_part = 0,
    add_minmax_index_for_numeric_columns = 0;
INSERT INTO test_typed_constant_monotonic SELECT toString(number) FROM numbers(100);

SELECT 'monotonic, Enum8, Dynamic';
SELECT trimLeft(explain) FROM
(
    EXPLAIN indexes = 1
    SELECT count() FROM test_typed_constant_monotonic
    WHERE v < CAST(CAST('37' AS Enum8('zero' = 0, '37' = 3)) AS Dynamic)
)
WHERE explain LIKE '%Condition:%' OR explain LIKE '%Granules:%/%';
SELECT count() FROM test_typed_constant_monotonic
WHERE v < CAST(CAST('37' AS Enum8('zero' = 0, '37' = 3)) AS Dynamic)
SETTINGS max_rows_to_read = 32;

SELECT 'monotonic, Enum8, Dynamic(max_types = 0)';
SELECT trimLeft(explain) FROM
(
    EXPLAIN indexes = 1
    SELECT count() FROM test_typed_constant_monotonic
    WHERE v < CAST(CAST('37' AS Enum8('zero' = 0, '37' = 3)) AS Dynamic(max_types = 0))
)
WHERE explain LIKE '%Condition:%' OR explain LIKE '%Granules:%/%';
SELECT count() FROM test_typed_constant_monotonic
WHERE v < CAST(CAST('37' AS Enum8('zero' = 0, '37' = 3)) AS Dynamic(max_types = 0))
SETTINGS max_rows_to_read = 32;

SELECT 'monotonic, Enum8, Variant';
SELECT trimLeft(explain) FROM
(
    EXPLAIN indexes = 1
    SELECT count() FROM test_typed_constant_monotonic
    WHERE v < CAST(CAST('37' AS Enum8('zero' = 0, '37' = 3)) AS Variant(Array(UInt8), Enum8('zero' = 0, '37' = 3)))
)
WHERE explain LIKE '%Condition:%' OR explain LIKE '%Granules:%/%';
SELECT count() FROM test_typed_constant_monotonic
WHERE v < CAST(CAST('37' AS Enum8('zero' = 0, '37' = 3)) AS Variant(Array(UInt8), Enum8('zero' = 0, '37' = 3)))
SETTINGS max_rows_to_read = 32;

SELECT 'monotonic, Enum16, Dynamic';
SELECT trimLeft(explain) FROM
(
    EXPLAIN indexes = 1
    SELECT count() FROM test_typed_constant_monotonic
    WHERE v < CAST(CAST('37' AS Enum16('zero' = 0, '37' = 3)) AS Dynamic)
)
WHERE explain LIKE '%Condition:%' OR explain LIKE '%Granules:%/%';
SELECT count() FROM test_typed_constant_monotonic
WHERE v < CAST(CAST('37' AS Enum16('zero' = 0, '37' = 3)) AS Dynamic)
SETTINGS max_rows_to_read = 32;

SELECT 'monotonic, Enum16, Dynamic(max_types = 0)';
SELECT trimLeft(explain) FROM
(
    EXPLAIN indexes = 1
    SELECT count() FROM test_typed_constant_monotonic
    WHERE v < CAST(CAST('37' AS Enum16('zero' = 0, '37' = 3)) AS Dynamic(max_types = 0))
)
WHERE explain LIKE '%Condition:%' OR explain LIKE '%Granules:%/%';
SELECT count() FROM test_typed_constant_monotonic
WHERE v < CAST(CAST('37' AS Enum16('zero' = 0, '37' = 3)) AS Dynamic(max_types = 0))
SETTINGS max_rows_to_read = 32;

SELECT 'monotonic, Enum16, Variant';
SELECT trimLeft(explain) FROM
(
    EXPLAIN indexes = 1
    SELECT count() FROM test_typed_constant_monotonic
    WHERE v < CAST(CAST('37' AS Enum16('zero' = 0, '37' = 3)) AS Variant(Array(UInt8), Enum16('zero' = 0, '37' = 3)))
)
WHERE explain LIKE '%Condition:%' OR explain LIKE '%Granules:%/%';
SELECT count() FROM test_typed_constant_monotonic
WHERE v < CAST(CAST('37' AS Enum16('zero' = 0, '37' = 3)) AS Variant(Array(UInt8), Enum16('zero' = 0, '37' = 3)))
SETTINGS max_rows_to_read = 32;

DROP TABLE test_typed_constant_monotonic;
