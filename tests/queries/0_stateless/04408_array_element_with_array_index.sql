-- Basic functionality
SELECT [10, 20, 30, 40][[2, 4, 1]];
SELECT [10, 20, 30, 40][[1, 1, 1]];
SELECT [10, 20, 30, 40][[]::Array(Int32)];

-- Negative indices
SELECT ['a', 'b', 'c'][[-1, -2]];
SELECT [10, 20, 30][[-1, 1]];

-- Out of bounds (Zero mode - returns default)
SELECT [10, 20, 30][[1, 5, 2]];
SELECT ['a', 'b'][[1, 3]];

-- Index zero (treated as OOB in non-const context)
SELECT [10, 20, 30][[0, 1, 2]];

-- arrayElementOrNull mode (OOB -> NULL)
SELECT arrayElementOrNull([10, 20, 30], [1, 5, 2]);
SELECT arrayElementOrNull([10, 20, 30], [0, 1, 2]);
SELECT arrayElementOrNull(['a', 'b'], [1, 3]);

-- Nullable source elements
SELECT [1, NULL, 3][[1, 2, 3]];
SELECT [1, NULL, 3][[2]];
SELECT arrayElementOrNull([1, NULL, 3], [1, 2, 4]);

-- Non-constant arrays (table-based)
DROP TABLE IF EXISTS test_arr_idx;
CREATE TABLE test_arr_idx (arr Array(Int32), idx Array(Int32)) ENGINE = Memory;
INSERT INTO test_arr_idx VALUES ([10,20,30], [1,3]), ([40,50], [2,1,2]), ([100], [1,1]);
SELECT arr[idx] FROM test_arr_idx;
DROP TABLE test_arr_idx;

-- Different integer types for index
SELECT [10, 20, 30][[toUInt8(1), toUInt8(2)]];
SELECT [10, 20, 30][[toInt64(-1), toInt64(-2)]];
SELECT [10, 20, 30][[toUInt64(2), toUInt64(3)]];

-- Nested arrays (Array(Array(Int)))
SELECT [[1,2],[3,4],[5,6]][[2,1]];

-- Tuples
SELECT [(1,'a'), (2,'b'), (3,'c')][[2, 1]];

-- Empty source array
SELECT []::Array(Int32)[[1, 2]];
SELECT arrayElementOrNull([]::Array(Int32), [1, 2]);

-- Equivalence with arrayMap
SELECT [10, 20, 30, 40][[2, 4, 1]] = arrayMap(i -> [10, 20, 30, 40][i], [2, 4, 1]);
