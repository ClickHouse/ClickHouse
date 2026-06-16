-- Extensive tests for the typed (non-Field-based) sumMap/minMap/maxMap path.
-- Covers key types, value types, operations, nullable, overflow, tuple syntax,
-- state serialization round-trips, merge, and edge cases.

-- { echoOn }

-- ===================== Key type coverage =====================

-- Int8 keys
SELECT 'Int8 keys';
SELECT sumMap([toInt8(1), toInt8(-1), toInt8(1)], [10, 20, 30]);

-- Int16 keys
SELECT 'Int16 keys';
SELECT sumMap([toInt16(100), toInt16(-100), toInt16(100)], [1, 2, 3]);

-- Int32 keys
SELECT 'Int32 keys';
SELECT sumMap([toInt32(1000), toInt32(2000)], [5, 10]);

-- Int64 keys
SELECT 'Int64 keys';
SELECT sumMap([toInt64(1), toInt64(2), toInt64(1)], [100, 200, 300]);

-- Int128 keys
SELECT 'Int128 keys';
SELECT sumMap([toInt128(1), toInt128(2), toInt128(1)], [10, 20, 30]);

-- UInt8 keys
SELECT 'UInt8 keys';
SELECT sumMap([toUInt8(1), toUInt8(2), toUInt8(1)], [10, 20, 30]);

-- UInt16 keys
SELECT 'UInt16 keys';
SELECT sumMap([toUInt16(1), toUInt16(2)], [100, 200]);

-- UInt32 keys
SELECT 'UInt32 keys';
SELECT sumMap([toUInt32(1), toUInt32(2)], [100, 200]);

-- UInt64 keys
SELECT 'UInt64 keys';
SELECT sumMap([toUInt64(1), toUInt64(2), toUInt64(1)], [10, 20, 30]);

-- UInt128 keys
SELECT 'UInt128 keys';
SELECT sumMap([toUInt128(1), toUInt128(2)], [10, 20]);

-- UInt256 keys
SELECT 'UInt256 keys';
SELECT sumMap([toUInt256(1), toUInt256(2)], [10, 20]);

-- Float32 keys
SELECT 'Float32 keys';
SELECT sumMap([toFloat32(1.5), toFloat32(2.5), toFloat32(1.5)], [10, 20, 30]);

-- Float64 keys
SELECT 'Float64 keys';
SELECT sumMap([toFloat64(1.5), toFloat64(2.5), toFloat64(1.5)], [10, 20, 30]);

-- BFloat16 keys
SELECT 'BFloat16 keys';
SELECT sumMap([toBFloat16(1.0), toBFloat16(2.0), toBFloat16(1.0)], [10, 20, 30]);

-- Decimal32 keys
SELECT 'Decimal32 keys';
SELECT sumMap([toDecimal32(1.1, 2), toDecimal32(2.2, 2), toDecimal32(1.1, 2)], [10, 20, 30]);

-- Decimal64 keys
SELECT 'Decimal64 keys';
SELECT sumMap([toDecimal64(1.1, 4), toDecimal64(2.2, 4)], [10, 20]);

-- Decimal128 keys
SELECT 'Decimal128 keys';
SELECT sumMap([toDecimal128(1.1, 4), toDecimal128(2.2, 4)], [10, 20]);

-- Decimal256 keys
SELECT 'Decimal256 keys';
SELECT sumMap([toDecimal256(1.1, 4), toDecimal256(2.2, 4)], [10, 20]);

-- String keys
SELECT 'String keys';
SELECT sumMap(['a', 'b', 'a'], [10, 20, 30]);

-- FixedString keys
SELECT 'FixedString keys';
SELECT sumMap([toFixedString('aa', 2), toFixedString('bb', 2), toFixedString('aa', 2)], [10, 20, 30]);

-- Date keys
SELECT 'Date keys';
SELECT sumMap([toDate('2020-01-01'), toDate('2020-01-02'), toDate('2020-01-01')], [10, 20, 30]);

-- DateTime keys
SELECT 'DateTime keys';
SELECT sumMap([toDateTime('2020-01-01 00:00:00'), toDateTime('2020-01-01 01:00:00')], [10, 20]);

-- Enum8 keys
SELECT 'Enum8 keys';
SELECT sumMap(
    [CAST('a', 'Enum8(\'a\'=1,\'b\'=2)'), CAST('b', 'Enum8(\'a\'=1,\'b\'=2)'), CAST('a', 'Enum8(\'a\'=1,\'b\'=2)')],
    [10, 20, 30]);

-- Enum16 keys
SELECT 'Enum16 keys';
SELECT sumMap(
    [CAST('x', 'Enum16(\'x\'=1000,\'y\'=2000)'), CAST('y', 'Enum16(\'x\'=1000,\'y\'=2000)')],
    [10, 20]);

-- ===================== Value type coverage =====================

-- UInt8 values with overflow promotion
SELECT 'UInt8 values promoted';
SELECT sumMap([1, 2], [toUInt8(200), toUInt8(200)]);
SELECT sumMapWithOverflow([1, 2], [toUInt8(200), toUInt8(200)]);

-- Int8 values
SELECT 'Int8 values';
SELECT sumMap([1, 2, 1], [toInt8(50), toInt8(-50), toInt8(70)]);

-- Float32 values
SELECT 'Float32 values';
SELECT sumMap([1, 2, 1], [toFloat32(1.5), toFloat32(2.5), toFloat32(3.5)]);

-- Float64 values
SELECT 'Float64 values';
SELECT sumMap([1, 2], [toFloat64(1.1), toFloat64(2.2)]);

-- BFloat16 values
SELECT 'BFloat16 values';
SELECT sumMap([1, 2, 1], [toBFloat16(1.0), toBFloat16(2.0), toBFloat16(3.0)]);

-- Decimal32 values
SELECT 'Decimal32 values';
SELECT sumMap([1, 2, 1], [toDecimal32(1.5, 2), toDecimal32(2.5, 2), toDecimal32(3.5, 2)]);

-- Decimal64 values
SELECT 'Decimal64 values';
SELECT sumMap([1, 2], [toDecimal64(1.5, 4), toDecimal64(2.5, 4)]);

-- Decimal128 values
SELECT 'Decimal128 values';
SELECT sumMap([1, 2], [toDecimal128(1.5, 4), toDecimal128(2.5, 4)]);

-- Decimal256 values
SELECT 'Decimal256 values';
SELECT sumMap([1, 2], [toDecimal256(1.5, 4), toDecimal256(2.5, 4)]);

-- Int128 values
SELECT 'Int128 values';
SELECT sumMap([1, 2], [toInt128(100), toInt128(200)]);

-- Int256 values
SELECT 'Int256 values';
SELECT sumMap([1, 2], [toInt256(100), toInt256(200)]);

-- UInt128 values
SELECT 'UInt128 values';
SELECT sumMap([1, 2], [toUInt128(100), toUInt128(200)]);

-- UInt256 values
SELECT 'UInt256 values';
SELECT sumMap([1, 2], [toUInt256(100), toUInt256(200)]);

-- ===================== All three operations =====================

SELECT 'sumMap';
SELECT sumMap([1, 2, 1, 3, 2], [10, 20, 30, 40, 50]);

SELECT 'minMap';
SELECT minMap([1, 2, 1, 3, 2], [10, 20, 30, 40, 50]);

SELECT 'maxMap';
SELECT maxMap([1, 2, 1, 3, 2], [10, 20, 30, 40, 50]);

SELECT 'sumMapWithOverflow';
SELECT sumMapWithOverflow([1, 2, 1, 3, 2], [10, 20, 30, 40, 50]);

-- ===================== Float NaN and Inf handling =====================

-- minMap: NaN should lose to any non-NaN value
SELECT 'minMap with NaN values';
SELECT minMap([1, 2, 1, 2], [nan, 5.0, 3.0, nan]);

-- maxMap: NaN should lose to any non-NaN value
SELECT 'maxMap with NaN values';
SELECT maxMap([1, 2, 1, 2], [nan, 5.0, 3.0, nan]);

-- sumMap with Inf
SELECT 'sumMap with Inf values';
SELECT sumMap([1, 2, 1], [inf, 1.0, -inf]);

-- Float64 key NaN sorting: NaN keys should sort last
SELECT 'Float64 key NaN sorting';
SELECT sumMap([x], [y]) FROM values('x Float64, y Float64', (4, 1), (1, 2), (nan, 3), (6.7, 4), (4, 5), (5, 6));

-- NaN keys must merge into a single key (all NaN bit patterns are equivalent)
SELECT 'Float64 NaN key merging';
SELECT sumMap([nan, nan], [1, 2]);

SELECT 'Float64 NaN key multi-row merge';
SELECT sumMap(k, v) FROM values('k Array(Float64), v Array(Int64)', ([nan], [10]), ([nan], [20]));

-- Float key normalization: +0.0 and -0.0 must merge into one key
SELECT 'Float64 +0/-0 normalization';
SELECT sumMap([0., -0.], [1, 2]);

SELECT 'Float32 +0/-0 normalization';
SELECT sumMap([toFloat32(0.), toFloat32(-0.)], [1, 2]);

SELECT 'BFloat16 +0/-0 normalization';
SELECT sumMap([toBFloat16(0.), toBFloat16(-0.)], [1, 2]);

-- +0.0 / -0.0 across multiple rows (exercises the merge path)
SELECT 'Float64 +0/-0 multi-row merge';
SELECT sumMap(k, v) FROM values('k Array(Float64), v Array(Int64)', ([0.], [1]), ([-0.], [2]));

-- ===================== Nullable values =====================

SELECT 'Nullable values - NULL as no-op';
SELECT sumMap(['a', 'b'], [1, NULL]);
SELECT sumMap(['a', 'b'], [1, toNullable(0)]);

SELECT 'Nullable - mixed NULL and values';
SELECT sumMap([1, 2, 1, 2], [toNullable(toUInt64(10)), NULL, NULL, toNullable(toUInt64(20))]);

SELECT 'minMap with Nullable';
SELECT minMap([1, 2, 1, 2], [toNullable(toInt64(10)), NULL, toNullable(toInt64(5)), toNullable(toInt64(20))]);

SELECT 'maxMap with Nullable';
SELECT maxMap([1, 2, 1, 2], [toNullable(toInt64(10)), NULL, toNullable(toInt64(50)), toNullable(toInt64(20))]);

SELECT 'Nullable Decimal values';
SELECT sumMap([1, 2, 1, 2], [toNullable(toDecimal32(1.5, 2)), NULL, toNullable(toDecimal32(2.5, 2)), toNullable(toDecimal32(3.5, 2))]);

-- All-NULL values for a key
SELECT 'All-NULL for a key';
SELECT sumMap([1, 1], [NULL, NULL]::Array(Nullable(UInt64)));

-- ===================== Multiple value columns =====================

SELECT 'Multiple value columns';
SELECT sumMap([1, 2, 1], [10, 20, 30], [100, 200, 300]);

SELECT 'Multiple value columns - mixed types';
SELECT sumMap([1, 2, 1], [toUInt64(10), toUInt64(20), toUInt64(30)], [toFloat64(1.5), toFloat64(2.5), toFloat64(3.5)]);

SELECT 'minMap multiple columns';
SELECT minMap([1, 2, 1], [10, 20, 5], [100, 200, 50]);

SELECT 'maxMap multiple columns';
SELECT maxMap([1, 2, 1], [10, 20, 50], [100, 200, 500]);

-- ===================== Tuple argument syntax =====================

SELECT 'Tuple syntax sumMap';
SELECT sumMap(([1, 2, 1], [10, 20, 30]));

SELECT 'Tuple syntax minMap';
SELECT minMap(([1, 2, 1], [10, 20, 5]));

SELECT 'Tuple syntax maxMap';
SELECT maxMap(([1, 2, 1], [10, 20, 50]));

SELECT 'Tuple syntax sumMapWithOverflow';
SELECT sumMapWithOverflow(([1, 2, 1], [toUInt8(200), toUInt8(100), toUInt8(200)]));

-- ===================== Compaction (zero removal for sumMap) =====================

-- sumMap compacts: keys with all-zero values are removed
SELECT 'sumMap compaction';
SELECT sumMap([1, 2, 3, 1], [10, 20, 30, -10]);

-- sumMapWithOverflow also compacts
SELECT 'sumMapWithOverflow compaction';
SELECT sumMapWithOverflow([1, 2, 1], [toInt32(10), toInt32(20), toInt32(-10)]);

-- minMap/maxMap do NOT compact
SELECT 'minMap no compaction';
SELECT minMap([1, 2, 1], [0, 20, 0]);

SELECT 'maxMap no compaction';
SELECT maxMap([1, 2, 1], [0, 20, 0]);

-- ===================== State round-trip (serialize/deserialize) =====================

SELECT 'State round-trip sumMap';
SELECT sumMapMerge(s) FROM (SELECT sumMapState([1, 2, 1], [toUInt64(10), toUInt64(20), toUInt64(30)]) AS s);

SELECT 'State round-trip minMap';
SELECT minMapMerge(s) FROM (SELECT minMapState([1, 2, 1], [toInt64(10), toInt64(20), toInt64(5)]) AS s);

SELECT 'State round-trip maxMap';
SELECT maxMapMerge(s) FROM (SELECT maxMapState([1, 2, 1], [toInt64(10), toInt64(20), toInt64(50)]) AS s);

SELECT 'State round-trip with Nullable Decimal';
SELECT sumMapMerge(s) FROM (
    SELECT sumMapState([toUInt16(1), toUInt16(2)], [toNullable(toDecimal32(1.5, 2)), toNullable(toDecimal32(2.5, 2))]) AS s
);

SELECT 'State round-trip with String keys';
SELECT sumMapMerge(s) FROM (
    SELECT sumMapState(['hello', 'world', 'hello'], [1, 2, 3]) AS s
);

-- ===================== Multi-row aggregation =====================

SELECT 'Multi-row aggregation';
SELECT sumMap(k, v) FROM values('k Array(UInt8), v Array(UInt64)',
    ([1, 2, 3], [10, 10, 10]),
    ([3, 4, 5], [10, 10, 10]),
    ([4, 5, 6], [10, 10, 10]),
    ([6, 7, 8], [10, 10, 10]));

SELECT 'Multi-row minMap';
SELECT minMap(k, v) FROM values('k Array(UInt8), v Array(Int64)',
    ([1, 2, 3], [10, 20, 30]),
    ([2, 3, 4], [5, 15, 25]),
    ([3, 4, 5], [1, 10, 20]));

SELECT 'Multi-row maxMap';
SELECT maxMap(k, v) FROM values('k Array(UInt8), v Array(Int64)',
    ([1, 2, 3], [10, 20, 30]),
    ([2, 3, 4], [50, 15, 25]),
    ([3, 4, 5], [1, 100, 20]));

-- ===================== Edge cases =====================

-- Empty arrays
SELECT 'Empty arrays';
SELECT sumMap([], []::Array(UInt64));

-- Single element
SELECT 'Single element';
SELECT sumMap([1], [42]);

-- Large number of keys
SELECT 'Many keys';
SELECT length(sumMap(arrayMap(x -> x, range(1000)), arrayMap(x -> 1, range(1000))).1);

-- Same key repeated many times
SELECT 'Repeated key';
SELECT sumMap(arrayMap(x -> toUInt8(1), range(100)), arrayMap(x -> toUInt64(1), range(100)));

-- ===================== Filtered variants =====================

-- Basic filtered: only keep keys 1 and 3
SELECT 'sumMapFiltered basic';
SELECT sumMapFiltered([1, 3])([1, 2, 3, 1], [10, 20, 30, 40]);

-- Filtered with overflow
SELECT 'sumMapFilteredWithOverflow basic';
SELECT sumMapFilteredWithOverflow([1])([1, 2, 1], [toUInt8(200), toUInt8(100), toUInt8(200)]);

-- Filtered with string keys
SELECT 'sumMapFiltered string keys';
SELECT sumMapFiltered(['a', 'c'])(['a', 'b', 'c', 'a'], [10, 20, 30, 40]);

-- Filtered with no matching keys
SELECT 'sumMapFiltered no match';
SELECT sumMapFiltered([99])([1, 2, 3], [10, 20, 30]);

-- Filtered with all matching keys
SELECT 'sumMapFiltered all match';
SELECT sumMapFiltered([1, 2, 3])([1, 2, 3], [10, 20, 30]);

-- Filtered multi-row aggregation
SELECT 'sumMapFiltered multi-row';
SELECT sumMapFiltered([1, 3])(k, v) FROM values('k Array(UInt8), v Array(UInt64)',
    ([1, 2, 3], [10, 10, 10]),
    ([3, 4, 5], [10, 10, 10]),
    ([1, 3, 5], [5, 5, 5]));

-- Filtered with tuple syntax
SELECT 'sumMapFiltered tuple syntax';
SELECT sumMapFiltered([1, 3])(([1, 2, 3, 1], [10, 20, 30, 40]));

-- Filtered with Nullable values
SELECT 'sumMapFiltered nullable';
SELECT sumMapFiltered([1, 2])([1, 2, 3, 1], [toNullable(toUInt64(10)), NULL, toNullable(toUInt64(30)), toNullable(toUInt64(40))]);

-- Filtered with multiple value columns
SELECT 'sumMapFiltered multiple values';
SELECT sumMapFiltered([1, 3])([1, 2, 3], [10, 20, 30], [100, 200, 300]);

-- Filtered with cross-signedness filter: signed filter values on unsigned keys
-- (mirrors 02961_sumMapFiltered_keepKey which uses toInt8(-3) with UInt64 keys)
SELECT 'sumMapFiltered cross-sign: signed filter on UInt64 keys';
SELECT sumMapFiltered([1, 2, 3, toInt8(-3)])(a, b) FROM values('a Array(UInt64), b Array(Int64)',
    ([1, 2, 3], [10, 10, 10]), ([3, 4, 5], [10, 10, 10]),
    ([4, 5, 6], [10, 10, 10]), ([6, 7, 8], [10, 10, 10]));

-- Unsigned filter values on signed keys
SELECT 'sumMapFiltered cross-sign: unsigned filter on Int32 keys';
SELECT sumMapFiltered([toUInt64(1), toUInt64(3)])(a, b) FROM values('a Array(Int32), b Array(Int64)',
    ([1, 2, 3], [10, 20, 30]), ([3, 4, 5], [40, 50, 60]));

-- Negative filter value that matches no unsigned key should be silently ignored
SELECT 'sumMapFiltered cross-sign: negative filter ignored for unsigned keys';
SELECT sumMapFiltered([toInt64(-1), toInt64(2)])(a, b) FROM values('a Array(UInt64), b Array(Int64)',
    ([1, 2, 3], [10, 20, 30]), ([2, 4, 6], [40, 50, 60]));

-- Filtered getName stability
SELECT 'getName sumMapFiltered';
SELECT toTypeName(sumMapFilteredState([1])([toUInt8(1)], [toUInt64(1)]));
SELECT 'getName sumMapFilteredWithOverflow';
SELECT toTypeName(sumMapFilteredWithOverflowState([1])([toUInt8(1)], [toUInt64(1)]));

-- ===================== Function name stability =====================
-- Verify the typed path reports the correct public names

SELECT 'getName sumMap';
SELECT toTypeName(sumMapState([toUInt8(1)], [toUInt64(1)]));
SELECT 'getName sumMapWithOverflow';
SELECT toTypeName(sumMapWithOverflowState([toUInt8(1)], [toUInt64(1)]));
SELECT 'getName minMap';
SELECT toTypeName(minMapState([toUInt8(1)], [toUInt64(1)]));
SELECT 'getName maxMap';
SELECT toTypeName(maxMapState([toUInt8(1)], [toUInt64(1)]));

-- ===================== AggregatingMergeTree state round-trip =====================
-- Tests serialize -> disk -> deserialize -> merge for both the typed fast path
-- and the generic fallback path. This validates cross-path wire compatibility:
-- states written by one code path must be readable by the other.

-- Typed path: UInt32 keys, UInt64 values
SELECT 'AggMT typed path';
DROP TABLE IF EXISTS test_summap_amt;
CREATE TABLE test_summap_amt (key UInt32, state AggregateFunction(sumMap, Array(UInt32), Array(UInt64))) ENGINE = AggregatingMergeTree ORDER BY key;
INSERT INTO test_summap_amt SELECT 1, sumMapState([toUInt32(1), toUInt32(2)], [toUInt64(10), toUInt64(20)]);
INSERT INTO test_summap_amt SELECT 1, sumMapState([toUInt32(2), toUInt32(3)], [toUInt64(30), toUInt64(40)]);
INSERT INTO test_summap_amt SELECT 1, sumMapState([toUInt32(1), toUInt32(3)], [toUInt64(5), toUInt64(5)]);
OPTIMIZE TABLE test_summap_amt FINAL;
SELECT sumMapMerge(state) FROM test_summap_amt;
DROP TABLE test_summap_amt;

-- Generic path: IPv4 keys (TypeIndex::IPv4 is not in the typed path support list)
SELECT 'AggMT generic path (IPv4 keys)';
DROP TABLE IF EXISTS test_summap_amt_generic;
CREATE TABLE test_summap_amt_generic (key UInt32, state AggregateFunction(sumMap, Array(IPv4), Array(UInt64))) ENGINE = AggregatingMergeTree ORDER BY key;
INSERT INTO test_summap_amt_generic SELECT 1, sumMapState([toIPv4('1.0.0.1'), toIPv4('1.0.0.2')], [toUInt64(10), toUInt64(20)]);
INSERT INTO test_summap_amt_generic SELECT 1, sumMapState([toIPv4('1.0.0.2'), toIPv4('1.0.0.3')], [toUInt64(30), toUInt64(40)]);
INSERT INTO test_summap_amt_generic SELECT 1, sumMapState([toIPv4('1.0.0.1'), toIPv4('1.0.0.3')], [toUInt64(5), toUInt64(5)]);
OPTIMIZE TABLE test_summap_amt_generic FINAL;
SELECT sumMapMerge(state) FROM test_summap_amt_generic;
DROP TABLE test_summap_amt_generic;

-- Typed path: String keys with multiple value columns (arena-backed key storage)
SELECT 'AggMT string keys multi-value';
DROP TABLE IF EXISTS test_summap_amt_str;
CREATE TABLE test_summap_amt_str (key UInt32, state AggregateFunction(sumMap, Array(String), Array(Int64), Array(Float64))) ENGINE = AggregatingMergeTree ORDER BY key;
INSERT INTO test_summap_amt_str SELECT 1, sumMapState(['a', 'b'], [toInt64(10), toInt64(20)], [toFloat64(1.5), toFloat64(2.5)]);
INSERT INTO test_summap_amt_str SELECT 1, sumMapState(['b', 'c'], [toInt64(30), toInt64(40)], [toFloat64(3.5), toFloat64(4.5)]);
OPTIMIZE TABLE test_summap_amt_str FINAL;
SELECT sumMapMerge(state) FROM test_summap_amt_str;
DROP TABLE test_summap_amt_str;

-- minMap through AggregatingMergeTree
SELECT 'AggMT minMap';
DROP TABLE IF EXISTS test_minmap_amt;
CREATE TABLE test_minmap_amt (key UInt32, state AggregateFunction(minMap, Array(UInt32), Array(Int64))) ENGINE = AggregatingMergeTree ORDER BY key;
INSERT INTO test_minmap_amt SELECT 1, minMapState([toUInt32(1), toUInt32(2)], [toInt64(10), toInt64(20)]);
INSERT INTO test_minmap_amt SELECT 1, minMapState([toUInt32(2), toUInt32(3)], [toInt64(5), toInt64(40)]);
INSERT INTO test_minmap_amt SELECT 1, minMapState([toUInt32(1), toUInt32(3)], [toInt64(100), toInt64(1)]);
OPTIMIZE TABLE test_minmap_amt FINAL;
SELECT minMapMerge(state) FROM test_minmap_amt;
DROP TABLE test_minmap_amt;

-- maxMap through AggregatingMergeTree
SELECT 'AggMT maxMap';
DROP TABLE IF EXISTS test_maxmap_amt;
CREATE TABLE test_maxmap_amt (key UInt32, state AggregateFunction(maxMap, Array(UInt32), Array(Int64))) ENGINE = AggregatingMergeTree ORDER BY key;
INSERT INTO test_maxmap_amt SELECT 1, maxMapState([toUInt32(1), toUInt32(2)], [toInt64(10), toInt64(20)]);
INSERT INTO test_maxmap_amt SELECT 1, maxMapState([toUInt32(2), toUInt32(3)], [toInt64(50), toInt64(40)]);
INSERT INTO test_maxmap_amt SELECT 1, maxMapState([toUInt32(1), toUInt32(3)], [toInt64(100), toInt64(1)]);
OPTIMIZE TABLE test_maxmap_amt FINAL;
SELECT maxMapMerge(state) FROM test_maxmap_amt;
DROP TABLE test_maxmap_amt;

-- Nullable values through AggregatingMergeTree
SELECT 'AggMT nullable values';
DROP TABLE IF EXISTS test_summap_amt_nullable;
CREATE TABLE test_summap_amt_nullable (key UInt32, state AggregateFunction(sumMap, Array(UInt32), Array(Nullable(UInt64)))) ENGINE = AggregatingMergeTree ORDER BY key;
INSERT INTO test_summap_amt_nullable SELECT 1, sumMapState([toUInt32(1), toUInt32(2)], [toNullable(toUInt64(10)), NULL]);
INSERT INTO test_summap_amt_nullable SELECT 1, sumMapState([toUInt32(1), toUInt32(2)], [NULL, toNullable(toUInt64(20))]);
INSERT INTO test_summap_amt_nullable SELECT 1, sumMapState([toUInt32(1), toUInt32(2)], [toNullable(toUInt64(5)), toNullable(toUInt64(30))]);
OPTIMIZE TABLE test_summap_amt_nullable FINAL;
SELECT sumMapMerge(state) FROM test_summap_amt_nullable;
DROP TABLE test_summap_amt_nullable;
