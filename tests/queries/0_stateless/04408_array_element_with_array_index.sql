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

-- Float types
SELECT [1.5, 2.5, 3.5]::Array(Float32)[[2, 3, 1]];
SELECT [1.5, 2.5, 3.5]::Array(Float64)[[3, 1]];

-- Wide integer types
SELECT [toInt128(100), toInt128(200), toInt128(300)][[1, 3]];
SELECT [toUInt128(100), toUInt128(200)][[2, 1]];
SELECT [toInt256(1000), toInt256(2000)][[2]];
SELECT [toUInt256(1000), toUInt256(2000)][[1, 2]];

-- UUID
SELECT [toUUID('61f0c404-5cb3-11e7-907b-a6006ad3dba0'), toUUID('79f0c404-5cb3-11e7-907b-a6006ad3dba0')][[2, 1]];

-- IPv4 / IPv6
SELECT [toIPv4('192.168.1.1'), toIPv4('10.0.0.1'), toIPv4('127.0.0.1')][[3, 1]];
SELECT [toIPv6('::1'), toIPv6('fe80::1')][[2, 1]];

-- Date / DateTime types (stored as UInt16 / UInt32 / Int32)
SELECT [toDate('2024-01-01'), toDate('2024-06-15'), toDate('2024-12-31')][[3, 1]];
SELECT [toDate32('2024-01-01'), toDate32('2024-06-15')][[2, 1]];
SELECT [toDateTime('2024-01-01 00:00:00'), toDateTime('2024-06-15 12:00:00')][[2]];
SELECT [toDateTime64('2024-01-01 00:00:00.000', 3), toDateTime64('2024-06-15 12:00:00.123', 3)][[2, 1]];

-- Decimal types
SELECT [toDecimal32(1.23, 2), toDecimal32(4.56, 2), toDecimal32(7.89, 2)][[2, 3]];
SELECT [toDecimal64(1.23, 2), toDecimal64(4.56, 2)][[1, 2, 1]];
SELECT [toDecimal128(1.23, 2), toDecimal128(4.56, 2)][[2]];

-- FixedString
SELECT [toFixedString('ab', 2), toFixedString('cd', 2), toFixedString('ef', 2)][[3, 1]];

-- Map type inside array
SELECT [map('a', 1, 'b', 2), map('c', 3, 'd', 4)][[2, 1]];

-- Equivalence with arrayMap
SELECT [10, 20, 30, 40][[2, 4, 1]] = arrayMap(i -> [10, 20, 30, 40][i], [2, 4, 1]);
