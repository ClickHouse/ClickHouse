DROP TABLE IF EXISTS test_coalescing_basic;
DROP TABLE IF EXISTS test_coalescing_nested_before_key;
DROP TABLE IF EXISTS test_coalescing_explicit_col;
DROP TABLE IF EXISTS test_coalescing_tuple_partition;

-- Test 1: CoalescingMergeTree - Basic Tuple with Nullable fields
SELECT '=== Test 1: CoalescingMergeTree Basic Tuple ===';

CREATE TABLE test_coalescing_basic (
    id UInt32,
    metrics Tuple(
        sum_val Nullable(UInt64),
        max_val Nullable(UInt64),
        str_val Nullable(String)
    )
) ENGINE = CoalescingMergeTree() ORDER BY id
SETTINGS allow_tuple_element_aggregation = 1;

INSERT INTO test_coalescing_basic VALUES (1, (100, 50, 'first'));
INSERT INTO test_coalescing_basic VALUES (2, (200, 80, 'second'));
INSERT INTO test_coalescing_basic VALUES (1, (NULL, 60, 'updated'));
INSERT INTO test_coalescing_basic VALUES (2, (250, NULL, NULL));

SELECT 'Coalescing Basic - with FINAL:';
SELECT id, metrics FROM test_coalescing_basic FINAL ORDER BY id;

OPTIMIZE TABLE test_coalescing_basic FINAL;

SELECT 'Coalescing Basic - after OPTIMIZE:';
SELECT id, metrics FROM test_coalescing_basic ORDER BY id;

DROP TABLE test_coalescing_basic;

-- Test 2: Nested Tuple placed before sorting key column
-- Covers: nested tuple coalescing + column index shift when tuple precedes sort key
SELECT '=== Test 2: Nested Tuple before sorting key ===';

CREATE TABLE test_coalescing_nested_before_key (
    data Tuple(
        val Nullable(UInt64),
        nested Tuple(
            inner_val Nullable(UInt64),
            deep_nested Tuple(
                deep_val Nullable(UInt64)
            )
        )
    ),
    id UInt32
) ENGINE = CoalescingMergeTree() ORDER BY id
SETTINGS allow_tuple_element_aggregation = 1;

INSERT INTO test_coalescing_nested_before_key VALUES ((100, (200, (300))), 1), ((150, (250, (350))), 2);
INSERT INTO test_coalescing_nested_before_key VALUES ((NULL, (210, (NULL))), 1), ((160, (NULL, (360))), 2);

SELECT 'Nested before key - with FINAL:';
SELECT id, data FROM test_coalescing_nested_before_key FINAL ORDER BY id;

OPTIMIZE TABLE test_coalescing_nested_before_key FINAL;

SELECT 'Nested before key - after OPTIMIZE:';
SELECT id, data FROM test_coalescing_nested_before_key ORDER BY id;

DROP TABLE test_coalescing_nested_before_key;

-- Test 3: Explicit column_names_to_sum - only specified Tuple is coalesced
-- Covers: nested tuple in explicit list + unspecified tuple/plain columns keep first value
SELECT '=== Test 3: Explicit column_names_to_sum ===';

CREATE TABLE test_coalescing_explicit_col (
    key UInt32,
    coalesced Tuple(
        x Nullable(UInt64),
        inner Tuple(
            y Nullable(UInt64),
            z Nullable(UInt64)
        )
    ),
    not_coalesced Tuple(c Nullable(UInt64), d Nullable(UInt64)),
    plain_val UInt64
) ENGINE = CoalescingMergeTree(coalesced) ORDER BY key
SETTINGS allow_tuple_element_aggregation = 1;

INSERT INTO test_coalescing_explicit_col VALUES (1, (10, (20, 30)), (100, 200), 1000);
INSERT INTO test_coalescing_explicit_col VALUES (1, (NULL, (50, NULL)), (300, NULL), 2000);
INSERT INTO test_coalescing_explicit_col VALUES (2, (5, (6, 7)), (50, 60), 500);
INSERT INTO test_coalescing_explicit_col VALUES (2, (8, (NULL, 10)), (NULL, 80), 600);

SELECT 'Explicit col - with FINAL:';
SELECT key, coalesced, not_coalesced, plain_val FROM test_coalescing_explicit_col FINAL ORDER BY key;

OPTIMIZE TABLE test_coalescing_explicit_col FINAL;

SELECT 'Explicit col - after OPTIMIZE:';
SELECT key, coalesced, not_coalesced, plain_val FROM test_coalescing_explicit_col ORDER BY key;

DROP TABLE test_coalescing_explicit_col;

-- Test 4: Tuple column used as partition key must NOT be coalesced
-- pk is a nested Tuple used as PARTITION BY; two rows share the same pk so they
-- land in the same partition and get merged.  After merge the non-key Tuple `val`
-- must be coalesced normally while the engine must not crash or mishandle the
-- partition-key Tuple (it is silently skipped during coalescing).
SELECT '=== Test 4: Nested Tuple in partition key ===';

CREATE TABLE test_coalescing_tuple_partition (
    id UInt32,
    pk Tuple(a UInt32, inner Tuple(z UInt32)),
    val Tuple(m Nullable(UInt64), n Nullable(UInt64))
) ENGINE = CoalescingMergeTree()
PARTITION BY pk
ORDER BY id
SETTINGS allow_tuple_element_aggregation = 1;

INSERT INTO test_coalescing_tuple_partition VALUES (1, (10, (20)), (100, NULL));
INSERT INTO test_coalescing_tuple_partition VALUES (1, (10, (20)), (NULL, 400));

SELECT 'Nested Tuple partition key - with FINAL:';
SELECT id, pk, val FROM test_coalescing_tuple_partition FINAL ORDER BY id;

OPTIMIZE TABLE test_coalescing_tuple_partition FINAL;

SELECT 'Nested Tuple partition key - after OPTIMIZE:';
SELECT id, pk, val FROM test_coalescing_tuple_partition ORDER BY id;

DROP TABLE test_coalescing_tuple_partition;

-- Test 5: CoalescingMergeTree - nested xxxMap inside an outer Tuple
-- Covers the aggregate_all_columns=true branch of defineColumns in
-- SummingSortedAlgorithm (CoalescingMergeTree reuses that algorithm).
-- The leaf column name "metrics.ratesMap.ID" requires splitName(reverse=true)
-- to recover the full parent path "metrics.ratesMap" for xxxMap detection.
SELECT '=== Test 5: CoalescingMergeTree Nested xxxMap inside Tuple ===';

DROP TABLE IF EXISTS test_coalescing_top_level_map;
DROP TABLE IF EXISTS test_coalescing_wrapped_map;

CREATE TABLE test_coalescing_top_level_map (
    id UInt64,
    ratesMap Tuple(ID Array(UInt64), Value Array(UInt64))
) ENGINE = CoalescingMergeTree ORDER BY id
SETTINGS allow_tuple_element_aggregation = 1;

CREATE TABLE test_coalescing_wrapped_map (
    id UInt64,
    metrics Tuple(
        ratesMap Tuple(ID Array(UInt64), Value Array(UInt64))
    )
) ENGINE = CoalescingMergeTree ORDER BY id
SETTINGS allow_tuple_element_aggregation = 1;

INSERT INTO test_coalescing_top_level_map VALUES (1, ([1, 2], [10, 20])), (1, ([1, 3], [100, 200]));
INSERT INTO test_coalescing_wrapped_map   VALUES (1, (([1, 2], [10, 20]))), (1, (([1, 3], [100, 200])));

OPTIMIZE TABLE test_coalescing_top_level_map FINAL;
OPTIMIZE TABLE test_coalescing_wrapped_map   FINAL;

SELECT 'Coalescing top-level Map - after OPTIMIZE:';
SELECT id, ratesMap FROM test_coalescing_top_level_map ORDER BY id;

SELECT 'Coalescing wrapped Map - after OPTIMIZE:';
SELECT id, metrics FROM test_coalescing_wrapped_map ORDER BY id;

DROP TABLE test_coalescing_top_level_map;
DROP TABLE test_coalescing_wrapped_map;
