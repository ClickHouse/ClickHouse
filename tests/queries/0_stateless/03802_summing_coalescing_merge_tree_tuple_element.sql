DROP TABLE IF EXISTS test_summing_basic;
DROP TABLE IF EXISTS test_summing_empty_tuple;
DROP TABLE IF EXISTS test_summing_nested_tuple;
DROP TABLE IF EXISTS test_summing_unnamed_tuple;
DROP TABLE IF EXISTS test_summing_mixed;
DROP TABLE IF EXISTS test_coalescing_basic;
DROP TABLE IF EXISTS test_coalescing_nested_tuple;

-- Test 1: SummingMergeTree - Basic Tuple with named fields
SELECT '=== Test 1: SummingMergeTree Basic Tuple ===';

CREATE TABLE test_summing_basic (
    id UInt32,
    metrics Tuple(
        val_a UInt64,
        val_b UInt64,
        val_c UInt64
    )
) ENGINE = SummingMergeTree() ORDER BY id
SETTINGS allow_tuple_element_aggregation = 1;

-- Insert data twice with same id to trigger summing
INSERT INTO test_summing_basic VALUES (1, (100, 50, 1)), (2, (200, 80, 1)), (3, (300, 90, 1));
INSERT INTO test_summing_basic VALUES (1, (150, 60, 1)), (2, (250, 85, 1)), (3, (350, 95, 1));

SELECT 'Basic Tuple - with FINAL:';
SELECT id, metrics FROM test_summing_basic FINAL ORDER BY id;

OPTIMIZE TABLE test_summing_basic FINAL;

SELECT 'Basic Tuple - after OPTIMIZE:';
SELECT id, metrics FROM test_summing_basic ORDER BY id;

SELECT 'Basic Tuple - access by position:';
SELECT id, metrics.val_a, metrics.val_b, metrics.val_c
FROM test_summing_basic ORDER BY id;

DROP TABLE test_summing_basic;

-- Test 2: SummingMergeTree - Empty Tuple (should NOT be flattened)
SELECT '=== Test 2: SummingMergeTree Empty Tuple ===';

CREATE TABLE test_summing_empty_tuple (
    id UInt32,
    empty_metrics Tuple(),
    normal_val UInt64
) ENGINE = SummingMergeTree() ORDER BY id
SETTINGS allow_tuple_element_aggregation = 1;

-- Insert data with empty tuple
INSERT INTO test_summing_empty_tuple VALUES (1, tuple(), 100), (2, tuple(), 200);
INSERT INTO test_summing_empty_tuple VALUES (1, tuple(), 150), (2, tuple(), 250);

SELECT 'Empty Tuple - with FINAL:';
SELECT id, empty_metrics, normal_val FROM test_summing_empty_tuple FINAL ORDER BY id;

OPTIMIZE TABLE test_summing_empty_tuple FINAL;

SELECT 'Empty Tuple - after OPTIMIZE:';
SELECT id, empty_metrics, normal_val FROM test_summing_empty_tuple ORDER BY id;

DROP TABLE test_summing_empty_tuple;

-- Test 3: SummingMergeTree - Nested Tuple (multi-level)
SELECT '=== Test 3: SummingMergeTree Nested Tuple ===';

CREATE TABLE test_summing_nested_tuple (
    id UInt32,
    data Tuple(
        level1_a UInt64,
        level1_b UInt64,
        nested Tuple(
            level2_a UInt64,
            level2_b UInt64,
            deep_nested Tuple(
                level3_a UInt64,
                level3_b UInt64
            )
        )
    )
) ENGINE = SummingMergeTree() ORDER BY id
SETTINGS allow_tuple_element_aggregation = 1;

-- Insert nested tuple data
INSERT INTO test_summing_nested_tuple VALUES 
    (1, (100, 50, (200, 10, (1, 5)))),
    (2, (150, 60, (250, 15, (2, 10))));

INSERT INTO test_summing_nested_tuple VALUES 
    (1, (200, 70, (300, 8, (3, 15)))),
    (2, (250, 80, (350, 12, (4, 20))));

SELECT 'Nested Tuple - with FINAL:';
SELECT id, data FROM test_summing_nested_tuple FINAL ORDER BY id;

OPTIMIZE TABLE test_summing_nested_tuple FINAL;

SELECT 'Nested Tuple - after OPTIMIZE:';
SELECT id, data FROM test_summing_nested_tuple ORDER BY id;

DROP TABLE test_summing_nested_tuple;

-- Test 4: SummingMergeTree - Unnamed Tuple (positional access)
SELECT '=== Test 4: SummingMergeTree Unnamed Tuple ===';

CREATE TABLE test_summing_unnamed_tuple (
    id UInt32,
    metrics Tuple(UInt64, UInt64, UInt64)
) ENGINE = SummingMergeTree() ORDER BY id
SETTINGS allow_tuple_element_aggregation = 1;

INSERT INTO test_summing_unnamed_tuple VALUES (1, (100, 50, 1)), (2, (200, 80, 1));
INSERT INTO test_summing_unnamed_tuple VALUES (1, (150, 60, 1)), (2, (250, 85, 1));

SELECT 'Unnamed Tuple - with FINAL:';
SELECT id, metrics FROM test_summing_unnamed_tuple FINAL ORDER BY id;

OPTIMIZE TABLE test_summing_unnamed_tuple FINAL;

SELECT 'Unnamed Tuple - after OPTIMIZE:';
SELECT id, metrics FROM test_summing_unnamed_tuple ORDER BY id;

SELECT 'Unnamed Tuple - access by position:';
SELECT id, metrics.1 as val_1, metrics.2 as val_2, metrics.3 as val_3
FROM test_summing_unnamed_tuple ORDER BY id;

DROP TABLE test_summing_unnamed_tuple;

-- Test 5: SummingMergeTree - Mixed empty and non-empty tuples
SELECT '=== Test 5: SummingMergeTree Mixed Tuples ===';

CREATE TABLE test_summing_mixed (
    id UInt32,
    empty_tuple Tuple(),
    normal_tuple Tuple(val_a UInt64, val_b UInt64),
    another_empty Tuple(),
    nested_with_empty Tuple(
        val UInt64,
        empty_nested Tuple(),
        non_empty_nested Tuple(val_c UInt64)
    )
) ENGINE = SummingMergeTree() ORDER BY id
SETTINGS allow_tuple_element_aggregation = 1;

INSERT INTO test_summing_mixed VALUES 
    (1, tuple(), (100, 50), tuple(), (200, tuple(), tuple(1)));

INSERT INTO test_summing_mixed VALUES 
    (1, tuple(), (150, 60), tuple(), (300, tuple(), tuple(2)));

SELECT 'Mixed Tuples - with FINAL:';
SELECT id, empty_tuple, normal_tuple, another_empty, nested_with_empty 
FROM test_summing_mixed FINAL ORDER BY id;

OPTIMIZE TABLE test_summing_mixed FINAL;

SELECT 'Mixed Tuples - after OPTIMIZE:';
SELECT id, empty_tuple, normal_tuple, another_empty, nested_with_empty 
FROM test_summing_mixed ORDER BY id;

DROP TABLE test_summing_mixed;

-- Test 6: CoalescingMergeTree - Basic Tuple
SELECT '=== Test 6: CoalescingMergeTree Basic Tuple ===';

CREATE TABLE test_coalescing_basic (
    id UInt32,
    metrics Tuple(
        sum_val Nullable(UInt64),
        max_val Nullable(UInt64),
        str_val Nullable(String)
    )
) ENGINE = CoalescingMergeTree() ORDER BY id
SETTINGS allow_tuple_element_aggregation = 1;

-- Insert data with NULLs to test coalescing behavior
INSERT INTO test_coalescing_basic VALUES (1, (100, 50, 'first'));
INSERT INTO test_coalescing_basic VALUES (2, (200, 80, 'second'));
INSERT INTO test_coalescing_basic VALUES (1, (NULL, 60, 'updated'));  -- sum_val is NULL, should keep 100
INSERT INTO test_coalescing_basic VALUES (2, (250, NULL, NULL));       -- max_val and str_val are NULL

SELECT 'Coalescing Basic - with FINAL:';
SELECT id, metrics FROM test_coalescing_basic FINAL ORDER BY id;

OPTIMIZE TABLE test_coalescing_basic FINAL;

SELECT 'Coalescing Basic - after OPTIMIZE:';
SELECT id, metrics FROM test_coalescing_basic ORDER BY id;

DROP TABLE test_coalescing_basic;

-- Test 7: CoalescingMergeTree - Nested Tuple
SELECT '=== Test 7: CoalescingMergeTree Nested Tuple ===';

CREATE TABLE test_coalescing_nested_tuple (
    id UInt32,
    data Tuple(
        val Nullable(UInt64),
        nested Tuple(
            inner_val Nullable(UInt64),
            deep_nested Tuple(
                deep_val Nullable(UInt64)
            )
        )
    )
) ENGINE = CoalescingMergeTree() ORDER BY id
SETTINGS allow_tuple_element_aggregation = 1;

INSERT INTO test_coalescing_nested_tuple VALUES (1, (100, (200, (300))));
INSERT INTO test_coalescing_nested_tuple VALUES (2, (150, (250, (350))));
INSERT INTO test_coalescing_nested_tuple VALUES (1, (NULL, (210, (NULL))));  -- val and deep_val are NULL
INSERT INTO test_coalescing_nested_tuple VALUES (2, (160, (NULL, (360))));   -- inner_val is NULL

SELECT 'Coalescing Nested Tuple - with FINAL:';
SELECT id, data FROM test_coalescing_nested_tuple FINAL ORDER BY id;

OPTIMIZE TABLE test_coalescing_nested_tuple FINAL;

SELECT 'Coalescing Nested Tuple - after OPTIMIZE:';
SELECT id, data FROM test_coalescing_nested_tuple ORDER BY id;

DROP TABLE test_coalescing_nested_tuple;

-- Test 8: SummingMergeTree - Tuple with Nullable and LowCardinality
SELECT '=== Test 8: SummingMergeTree Complex Types ===';

CREATE TABLE test_summing_complex (
    id UInt32,
    metrics Tuple(
        sum_val UInt64,
        nullable_val Nullable(UInt64),
        low_card_val LowCardinality(String),
        nested Tuple(
            inner_sum UInt64,
            inner_nullable Nullable(String)
        )
    )
) ENGINE = SummingMergeTree() ORDER BY id
SETTINGS allow_tuple_element_aggregation = 1;

INSERT INTO test_summing_complex VALUES 
    (1, (100, 50, 'low1', (200, 'inner1'))),
    (2, (150, NULL, 'low2', (250, NULL)));

INSERT INTO test_summing_complex VALUES 
    (1, (200, 60, 'low1', (300, 'inner2'))),
    (2, (250, 70, 'low2', (350, 'inner3')));

SELECT 'Complex Types - with FINAL:';
SELECT id, metrics FROM test_summing_complex FINAL ORDER BY id;

OPTIMIZE TABLE test_summing_complex FINAL;

SELECT 'Complex Types - after OPTIMIZE:';
SELECT id, metrics FROM test_summing_complex ORDER BY id;

DROP TABLE test_summing_complex;

SELECT '=== All Tests Completed ===';
