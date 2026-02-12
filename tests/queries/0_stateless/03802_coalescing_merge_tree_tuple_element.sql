DROP TABLE IF EXISTS test_coalescing_basic;
DROP TABLE IF EXISTS test_coalescing_nested_tuple;
DROP TABLE IF EXISTS test_coalescing_tuple_before_sort_key;

-- Test 1: CoalescingMergeTree - Basic Tuple
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

-- Test 2: CoalescingMergeTree - Nested Tuple
SELECT '=== Test 2: CoalescingMergeTree Nested Tuple ===';

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

-- Test 3: Tuple column before sorting key column (removeReplicatedFromSortingColumns bug)
-- When tuple columns precede the sort key in schema, flattening shifts column indices.
SELECT '=== Test 3: CoalescingMergeTree Tuple before sorting key ===';

CREATE TABLE test_coalescing_tuple_before_sort_key (
    data Tuple(
        val Nullable(UInt64),
        nested Tuple(
            inner_val Nullable(UInt64)
        )
    ),
    id UInt32
) ENGINE = CoalescingMergeTree() ORDER BY id
SETTINGS allow_tuple_element_aggregation = 1;

INSERT INTO test_coalescing_tuple_before_sort_key VALUES ((100, (200)), 1), ((150, (250)), 2);
INSERT INTO test_coalescing_tuple_before_sort_key VALUES ((NULL, (210)), 1), ((160, (NULL)), 2);

SELECT 'Tuple before sort key - with FINAL:';
SELECT id, data FROM test_coalescing_tuple_before_sort_key FINAL ORDER BY id;

OPTIMIZE TABLE test_coalescing_tuple_before_sort_key FINAL;

SELECT 'Tuple before sort key - after OPTIMIZE:';
SELECT id, data FROM test_coalescing_tuple_before_sort_key ORDER BY id;

DROP TABLE test_coalescing_tuple_before_sort_key;
