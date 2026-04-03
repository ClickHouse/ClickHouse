-- Tags: no-parallel
-- Test: Verify that INSERT deduplication works correctly with complex column types
-- (Nullable, Array, Tuple, Map) using non-replicated dedup window.
-- Covers: src/Columns/ColumnNullable.cpp:60 updateHashWithValueRange
-- Covers: src/Columns/ColumnArray.cpp:306 updateHashWithValueRange
-- Covers: src/Columns/ColumnTuple.cpp:412 updateHashWithValueRange
-- Covers: src/Columns/ColumnMap.cpp:180 updateHashWithValueRange


DROP TABLE IF EXISTS test_dedup_nullable;
DROP TABLE IF EXISTS test_dedup_array;
DROP TABLE IF EXISTS test_dedup_tuple;
DROP TABLE IF EXISTS test_dedup_map;
DROP TABLE IF EXISTS test_dedup_mixed;

-- Nullable column dedup
CREATE TABLE test_dedup_nullable (id UInt32, val Nullable(UInt64))
ENGINE = MergeTree() ORDER BY id
SETTINGS non_replicated_deduplication_window=100;
SYSTEM STOP MERGES test_dedup_nullable;

INSERT INTO test_dedup_nullable VALUES (1, 42), (2, NULL), (3, 100);
INSERT INTO test_dedup_nullable VALUES (1, 42), (2, NULL), (3, 100); -- duplicate
SELECT 'nullable_count', count() FROM test_dedup_nullable;
SELECT 'nullable_data', id, val FROM test_dedup_nullable ORDER BY id;

-- Array column dedup
CREATE TABLE test_dedup_array (id UInt32, arr Array(UInt32))
ENGINE = MergeTree() ORDER BY id
SETTINGS non_replicated_deduplication_window=100;
SYSTEM STOP MERGES test_dedup_array;

INSERT INTO test_dedup_array VALUES (1, [1, 2, 3]), (2, [4, 5]), (3, []);
INSERT INTO test_dedup_array VALUES (1, [1, 2, 3]), (2, [4, 5]), (3, []); -- duplicate
SELECT 'array_count', count() FROM test_dedup_array;
SELECT 'array_data', id, arr FROM test_dedup_array ORDER BY id;

-- Tuple column dedup
CREATE TABLE test_dedup_tuple (id UInt32, tup Tuple(UInt32, String))
ENGINE = MergeTree() ORDER BY id
SETTINGS non_replicated_deduplication_window=100;
SYSTEM STOP MERGES test_dedup_tuple;

INSERT INTO test_dedup_tuple VALUES (1, (10, 'abc')), (2, (20, 'def'));
INSERT INTO test_dedup_tuple VALUES (1, (10, 'abc')), (2, (20, 'def')); -- duplicate
SELECT 'tuple_count', count() FROM test_dedup_tuple;
SELECT 'tuple_data', id, tup FROM test_dedup_tuple ORDER BY id;

-- Map column dedup
CREATE TABLE test_dedup_map (id UInt32, mp Map(String, UInt32))
ENGINE = MergeTree() ORDER BY id
SETTINGS non_replicated_deduplication_window=100;
SYSTEM STOP MERGES test_dedup_map;

INSERT INTO test_dedup_map VALUES (1, {'a': 1, 'b': 2}), (2, {'c': 3});
INSERT INTO test_dedup_map VALUES (1, {'a': 1, 'b': 2}), (2, {'c': 3}); -- duplicate
SELECT 'map_count', count() FROM test_dedup_map;
SELECT 'map_data', id, mp FROM test_dedup_map ORDER BY id;

-- Mixed complex types dedup
CREATE TABLE test_dedup_mixed (
    id UInt32,
    name String,
    amount Decimal64(2),
    tags Array(UInt32),
    nullable_val Nullable(UInt64),
    pair Tuple(UInt32, UInt64),
    kv Map(String, UInt32)
) ENGINE = MergeTree() ORDER BY id
SETTINGS non_replicated_deduplication_window=100;
SYSTEM STOP MERGES test_dedup_mixed;

INSERT INTO test_dedup_mixed VALUES (1, 'hello', 1.23, [1, 2, 3], 42, (1, 100), {'a': 1}), (2, 'world', 4.56, [4, 5], NULL, (2, 200), {'c': 3});
INSERT INTO test_dedup_mixed VALUES (1, 'hello', 1.23, [1, 2, 3], 42, (1, 100), {'a': 1}), (2, 'world', 4.56, [4, 5], NULL, (2, 200), {'c': 3}); -- duplicate
SELECT 'mixed_count', count() FROM test_dedup_mixed;

-- Verify DIFFERENT data is NOT deduplicated (no false positives)
INSERT INTO test_dedup_mixed VALUES (3, 'new', 7.89, [6], 99, (3, 300), {'d': 4});
SELECT 'mixed_total_after_different', count() FROM test_dedup_mixed;

DROP TABLE IF EXISTS test_dedup_nullable;
DROP TABLE IF EXISTS test_dedup_array;
DROP TABLE IF EXISTS test_dedup_tuple;
DROP TABLE IF EXISTS test_dedup_map;
DROP TABLE IF EXISTS test_dedup_mixed;
