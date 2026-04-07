DROP TABLE IF EXISTS test_coalescing_basic;
DROP TABLE IF EXISTS test_coalescing_nested_tuple;
DROP TABLE IF EXISTS test_coalescing_tuple_before_sort_key;
DROP TABLE IF EXISTS test_coalescing_explicit_col_nested;
DROP TABLE IF EXISTS test_coalescing_explicit_col_mixed;

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

-- Test 4: CoalescingMergeTree(data) - explicit column_names_to_sum with nested Tuple
-- Only the specified Tuple column should be coalesced; other Tuple columns and plain
-- columns should keep the first value (not coalesced).
SELECT '=== Test 4: Explicit column_names_to_sum with nested Tuple ===';

CREATE TABLE test_coalescing_explicit_col_nested (
    key UInt32,
    data Tuple(
        x Nullable(UInt64),
        inner Tuple(
            y Nullable(UInt64),
            z Nullable(UInt64)
        )
    ),
    other_tuple Tuple(m Nullable(UInt64), n Nullable(UInt64)),
    plain_val UInt64
) ENGINE = CoalescingMergeTree(data) ORDER BY key
SETTINGS allow_tuple_element_aggregation = 1;

INSERT INTO test_coalescing_explicit_col_nested VALUES (1, (10, (20, 30)), (100, 200), 1000);
INSERT INTO test_coalescing_explicit_col_nested VALUES (1, (NULL, (50, NULL)), (300, 400), 2000);
INSERT INTO test_coalescing_explicit_col_nested VALUES (2, (5, (6, 7)), (50, 60), 500);
INSERT INTO test_coalescing_explicit_col_nested VALUES (2, (8, (NULL, 10)), (70, 80), 600);

SELECT 'Explicit nested col - with FINAL:';
SELECT key, data, other_tuple, plain_val FROM test_coalescing_explicit_col_nested FINAL ORDER BY key;

OPTIMIZE TABLE test_coalescing_explicit_col_nested FINAL;

SELECT 'Explicit nested col - after OPTIMIZE:';
SELECT key, data, other_tuple, plain_val FROM test_coalescing_explicit_col_nested ORDER BY key;

DROP TABLE test_coalescing_explicit_col_nested;

-- Test 5: CoalescingMergeTree(coalesced) - only specified Tuple is coalesced, another is not
-- When column_names_to_sum is specified, other Tuple columns must NOT be coalesced
-- even though allow_tuple_element_aggregation is enabled.
SELECT '=== Test 5: Explicit column_names_to_sum - only specified Tuple is coalesced ===';

CREATE TABLE test_coalescing_explicit_col_mixed (
    key UInt32,
    coalesced Tuple(a Nullable(UInt64), b Nullable(UInt64)),
    not_coalesced Tuple(c Nullable(UInt64), d Nullable(UInt64)),
    plain_val UInt64
) ENGINE = CoalescingMergeTree(coalesced) ORDER BY key
SETTINGS allow_tuple_element_aggregation = 1;

INSERT INTO test_coalescing_explicit_col_mixed VALUES (1, (10, 20), (100, 200), 1000);
INSERT INTO test_coalescing_explicit_col_mixed VALUES (1, (NULL, 40), (300, NULL), 2000);
INSERT INTO test_coalescing_explicit_col_mixed VALUES (2, (5, 6), (50, 60), 500);
INSERT INTO test_coalescing_explicit_col_mixed VALUES (2, (7, NULL), (NULL, 80), 600);

SELECT 'Explicit mixed - with FINAL:';
SELECT key, coalesced, not_coalesced, plain_val FROM test_coalescing_explicit_col_mixed FINAL ORDER BY key;

OPTIMIZE TABLE test_coalescing_explicit_col_mixed FINAL;

SELECT 'Explicit mixed - after OPTIMIZE (not_coalesced keeps first value):';
SELECT key, coalesced, not_coalesced, plain_val FROM test_coalescing_explicit_col_mixed ORDER BY key;

DROP TABLE test_coalescing_explicit_col_mixed;
