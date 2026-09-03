-- Test: SummingMergeTree with allow_tuple_element_aggregation
-- Tests for explicit column_names_to_sum and partition/sorting key interactions
-- with flattened Tuple columns.

DROP TABLE IF EXISTS test_summing_explicit_col_nested;
DROP TABLE IF EXISTS test_summing_tuple_partition_key;
DROP TABLE IF EXISTS test_summing_tuple_sub_col_keys;
DROP TABLE IF EXISTS test_summing_prefix_no_false_skip;

-- Test 1: SummingMergeTree(data) - explicit column_names_to_sum with nested Tuple
-- Only the specified Tuple column should be summed; other Tuple columns and plain
-- columns should keep the first value (not aggregated).
-- This also covers: basic Tuple in column_names_to_sum, and verifying that
-- non-specified Tuple columns are NOT summed.
SELECT '=== Test 1: Explicit column_names_to_sum with nested Tuple ===';

CREATE TABLE test_summing_explicit_col_nested (
    key UInt32,
    data Tuple(
        x UInt64,
        inner Tuple(
            y UInt64,
            z UInt64
        )
    ),
    other_tuple Tuple(m UInt64, n UInt64),
    plain_val UInt64
) ENGINE = SummingMergeTree(data) ORDER BY key
SETTINGS allow_tuple_element_aggregation = 1;

INSERT INTO test_summing_explicit_col_nested VALUES (1, (10, (20, 30)), (100, 200), 1000);
INSERT INTO test_summing_explicit_col_nested VALUES (1, (40, (50, 60)), (300, 400), 2000);
INSERT INTO test_summing_explicit_col_nested VALUES (2, (5, (6, 7)), (50, 60), 500);
INSERT INTO test_summing_explicit_col_nested VALUES (2, (8, (9, 10)), (70, 80), 600);

SELECT 'Explicit nested col - with FINAL:';
SELECT key, data, other_tuple, plain_val FROM test_summing_explicit_col_nested FINAL ORDER BY key;

OPTIMIZE TABLE test_summing_explicit_col_nested FINAL;

SELECT 'Explicit nested col - after OPTIMIZE:';
SELECT key, data, other_tuple, plain_val FROM test_summing_explicit_col_nested ORDER BY key;

DROP TABLE test_summing_explicit_col_nested;

-- Test 2: Tuple column in partition key - flattened sub-columns must NOT be aggregated
-- When a Tuple column is part of the partition key, after flattening the sub-columns
-- (e.g. "part_key.a", "part_key.b") must be excluded from aggregation via ancestor
-- prefix matching in isInNamesOrAncestors.
SELECT '=== Test 2: Tuple in partition key ===';

CREATE TABLE test_summing_tuple_partition_key (
    id UInt32,
    part_key Tuple(a UInt32, b UInt32),
    value Tuple(x UInt64, y UInt64)
) ENGINE = SummingMergeTree()
PARTITION BY part_key
ORDER BY id
SETTINGS allow_tuple_element_aggregation = 1;

INSERT INTO test_summing_tuple_partition_key VALUES (1, (1, 2), (100, 200));
INSERT INTO test_summing_tuple_partition_key VALUES (1, (1, 2), (300, 400));

SELECT 'Tuple partition key - with FINAL:';
SELECT id, part_key, value FROM test_summing_tuple_partition_key FINAL ORDER BY id;

OPTIMIZE TABLE test_summing_tuple_partition_key FINAL;

SELECT 'Tuple partition key - after OPTIMIZE:';
SELECT id, part_key, value FROM test_summing_tuple_partition_key ORDER BY id;

DROP TABLE test_summing_tuple_partition_key;

-- Test 3: Tuple sub-columns used as partition key and sorting key
-- When a Tuple sub-column (e.g. key_tuple.pk) is used in PARTITION BY / ORDER BY,
-- getRequiredColumns returns the whole Tuple column 'key_tuple'. After flattening,
-- isInNamesOrAncestors walks up from 'key_tuple.pk' to 'key_tuple' and finds it in
-- partition_and_sorting_required_columns, so ALL sub-columns of that Tuple are
-- excluded from aggregation.
-- A separate Tuple column not referenced by any key should still be summed normally.
SELECT '=== Test 3: Tuple sub-columns as partition and sorting keys ===';

CREATE TABLE test_summing_tuple_sub_col_keys (
    id UInt32,
    key_tuple Tuple(pk UInt32, sk UInt32),
    value Tuple(x UInt64, y UInt64)
) ENGINE = SummingMergeTree()
PARTITION BY key_tuple.pk
ORDER BY key_tuple.sk
SETTINGS allow_tuple_element_aggregation = 1;

-- Both rows share the same pk=1 (same partition) and sk=10 (same sort key),
-- so they will be merged. key_tuple must NOT be summed (it is referenced by
-- partition/sorting keys), while value must be summed.
INSERT INTO test_summing_tuple_sub_col_keys VALUES (0, (1, 10), (100, 200));
INSERT INTO test_summing_tuple_sub_col_keys VALUES (0, (1, 10), (300, 400));

SELECT 'Tuple sub-col keys - with FINAL:';
SELECT key_tuple, value FROM test_summing_tuple_sub_col_keys FINAL ORDER BY key_tuple.sk;

OPTIMIZE TABLE test_summing_tuple_sub_col_keys FINAL;

SELECT 'Tuple sub-col keys - after OPTIMIZE:';
SELECT key_tuple, value FROM test_summing_tuple_sub_col_keys FINAL ORDER BY key_tuple.sk;

DROP TABLE test_summing_tuple_sub_col_keys;

-- Test 4: Ancestor prefix search must not falsely skip flattened tuple sub-columns
-- Column `n` is Tuple(c UInt32, b UInt32), column `n.a` is a plain UInt32 used as partition key.
-- After flattening, `n` becomes `n.c` and `n.b`. The prefix of `n.c` is `n`, which is NOT
-- `n.a` (the partition key). So `n.c` and `n.b` should still be aggregated.
SELECT '=== Test 4: Prefix search must not falsely skip flattened sub-columns ===';

CREATE TABLE test_summing_prefix_no_false_skip (
    `key` UInt32,
    `n` Tuple(
        c UInt32,
        b UInt32),
    `n.a` UInt32
) ENGINE = SummingMergeTree()
PARTITION BY `n.a`
ORDER BY key
SETTINGS allow_tuple_element_aggregation = 1;

-- Insert two rows in the same partition (n.a = 10) with the same key to trigger summing
INSERT INTO test_summing_prefix_no_false_skip VALUES (1, (100, 50), 10);
INSERT INTO test_summing_prefix_no_false_skip VALUES (1, (200, 70), 10);

SELECT 'Prefix no false skip - with FINAL:';
SELECT key, n, `n.a` FROM test_summing_prefix_no_false_skip FINAL ORDER BY key;

OPTIMIZE TABLE test_summing_prefix_no_false_skip FINAL;

SELECT 'Prefix no false skip - after OPTIMIZE:';
SELECT key, n, `n.a` FROM test_summing_prefix_no_false_skip ORDER BY key;

DROP TABLE test_summing_prefix_no_false_skip;
