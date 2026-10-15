-- { echo }
SET allow_suspicious_low_cardinality_types = 1;

-- A needle equal to the element type's default value must be found. Every row prints the
-- LowCardinality answer beside the same query over a plain array.
SELECT has(CAST(['', 'a'], 'Array(LowCardinality(String))'), '') AS lc, has(CAST(['', 'a'], 'Array(String)'), '') AS oracle;
SELECT indexOf(materialize(CAST(['a', ''], 'Array(LowCardinality(String))')), '') AS lc, indexOf(materialize(CAST(['a', ''], 'Array(String)')), '') AS oracle;
SELECT countEqual(materialize(CAST(['', 'a', ''], 'Array(LowCardinality(String))')), '') AS lc, countEqual(materialize(CAST(['', 'a', ''], 'Array(String)')), '') AS oracle;
SELECT has(materialize(CAST([0, 5], 'Array(LowCardinality(UInt8))')), 0) AS lc, has(materialize(CAST([0, 5], 'Array(UInt8)')), 0) AS oracle;
SELECT has(materialize(CAST([CAST('', 'FixedString(3)'), CAST('abc', 'FixedString(3)')], 'Array(LowCardinality(FixedString(3)))')), CAST('', 'FixedString(3)')) AS lc, has(materialize(CAST([CAST('', 'FixedString(3)'), CAST('abc', 'FixedString(3)')], 'Array(FixedString(3))')), CAST('', 'FixedString(3)')) AS oracle;
-- An Enum needle compares to a FixedString element as a string, where the element type's own padding
-- is not a difference.
SELECT has(materialize(CAST([CAST('', 'FixedString(3)'), CAST('abc', 'FixedString(3)')], 'Array(LowCardinality(FixedString(3)))')), CAST('', 'Enum8('''' = 0, ''o'' = 1)')) AS lc, has(materialize(CAST([CAST('', 'FixedString(3)'), CAST('abc', 'FixedString(3)')], 'Array(FixedString(3))')), CAST('', 'Enum8('''' = 0, ''o'' = 1)')) AS oracle;

-- The Map key and value dictionaries are the second call site.
SELECT mapContainsKey(materialize(CAST(map('', 'v_empty', 'k', 'v_k'), 'Map(LowCardinality(String), String)')), '') AS lc, mapContainsKey(materialize(CAST(map('', 'v_empty', 'k', 'v_k'), 'Map(String, String)')), '') AS oracle;
SELECT materialize(CAST(map('', 'v_empty', 'k', 'v_k'), 'Map(LowCardinality(String), String)'))[''] AS lc, materialize(CAST(map('', 'v_empty', 'k', 'v_k'), 'Map(String, String)'))[''] AS oracle;
SELECT mapContainsValue(materialize(CAST(map('k', '', 'j', 'v'), 'Map(String, LowCardinality(String))')), '') AS lc, mapContainsValue(materialize(CAST(map('k', '', 'j', 'v'), 'Map(String, String)')), '') AS oracle;

-- A constant the element type cannot represent equals no element, while one that survives the cast
-- still finds the default element. The timezone is pinned because the cast to Date drops the
-- needle's time of day and which day that lands on is offset-dependent.
SELECT has(materialize(CAST([0, 5], 'Array(LowCardinality(UInt8))')), 256) AS lc, has(materialize(CAST([0, 5], 'Array(UInt8)')), 256) AS oracle;
SELECT mapContainsKey(materialize(CAST(map(0, 'a', 5, 'b'), 'Map(LowCardinality(UInt8), String)')), 256) AS lc, mapContainsKey(materialize(CAST(map(0, 'a', 5, 'b'), 'Map(UInt8, String)')), 256) AS oracle;
SELECT has(materialize(CAST([toDate('1970-01-01'), toDate('2020-01-01')], 'Array(LowCardinality(Date))')), toDateTime('1970-01-01 00:00:05')) AS lc, has(materialize(CAST([toDate('1970-01-01'), toDate('2020-01-01')], 'Array(Date)')), toDateTime('1970-01-01 00:00:05')) AS oracle SETTINGS session_timezone = 'UTC';
SELECT has(materialize(CAST([0, 5], 'Array(LowCardinality(UInt8))')), toUInt64(0)) AS widened_needle, has(materialize(CAST([0, 1.5], 'Array(LowCardinality(Float64))')), toUInt8(0)) AS integral_needle;
-- An IPv4 element represents a UInt32 needle exactly, and the two have no accurate cast between them,
-- so representability is decided by comparing the needle against its own cast image.
SELECT has(materialize(CAST([toIPv4('0.0.0.0'), toIPv4('1.2.3.4')], 'Array(LowCardinality(IPv4))')), toUInt32(0)) AS lc, has(materialize(CAST([toIPv4('0.0.0.0'), toIPv4('1.2.3.4')], 'Array(IPv4)')), toUInt32(0)) AS oracle;

-- A FixedString needle is padded to its own width and equality ignores that padding.
SELECT has(materialize(CAST(['', 'xy'], 'Array(LowCardinality(String))')), CAST('', 'FixedString(4)')) AS lc, length(arrayFilter(x -> x = CAST('', 'FixedString(4)'), materialize(CAST(['', 'xy'], 'Array(String)')))) AS oracle;

-- -0.0 and 0.0 are equal but a text format stores them apart, so either zero as a needle must match
-- either spelling, and a count must see both. stored_bits is asserted in the same row: if it ever
-- reads 0 the array no longer holds -0.0 and the arm is void.
SELECT arrayMap(x -> reinterpretAsUInt64(x), a) AS stored_bits, has(a, toFloat64(0)) AS positive_zero_needle, has(a, reinterpretAsFloat64(toUInt64(9223372036854775808))) AS negative_zero_needle, length(arrayFilter(x -> x = toFloat64(0), a)) AS oracle FROM format(JSONEachRow, 'a Array(LowCardinality(Float64))', '{"a":[-0.0,1.5]}');
SELECT arrayMap(x -> reinterpretAsUInt64(x), a) AS stored_bits, indexOf(a, toFloat64(0)) AS positive_zero_needle, indexOf(a, reinterpretAsFloat64(toUInt64(9223372036854775808))) AS negative_zero_needle, indexOf(arrayMap(x -> toFloat64(x), a), toFloat64(0)) AS oracle FROM format(JSONEachRow, 'a Array(LowCardinality(Float64))', '{"a":[1.5,-0.0]}');
SELECT arrayMap(x -> reinterpretAsUInt64(x), a) AS stored_bits, countEqual(a, toFloat64(0)) AS positive_zero_needle, countEqual(a, reinterpretAsFloat64(toUInt64(9223372036854775808))) AS negative_zero_needle, length(arrayFilter(x -> x = toFloat64(0), a)) AS oracle FROM format(JSONEachRow, 'a Array(LowCardinality(Float64))', '{"a":[0.0,-0.0,1.5]}');
SELECT arrayMap(x -> reinterpretAsUInt32(x), a) AS stored_bits, has(a, toFloat32(0)) AS positive_zero_needle, has(a, reinterpretAsFloat32(toUInt32(2147483648))) AS negative_zero_needle, length(arrayFilter(x -> x = toFloat32(0), a)) AS oracle FROM format(JSONEachRow, 'a Array(LowCardinality(Float32))', '{"a":[-0.0,1.5]}');

-- A CAST array is folded onto the default slot, so its -0.0 element is stored as +0.0.
SELECT arrayMap(x -> reinterpretAsUInt64(x), materialize(CAST([reinterpretAsFloat64(toUInt64(9223372036854775808)), 1.5], 'Array(LowCardinality(Float64))'))) AS stored_bits, has(materialize(CAST([reinterpretAsFloat64(toUInt64(9223372036854775808)), 1.5], 'Array(LowCardinality(Float64))')), toFloat64(0)) AS positive_zero_needle, has(materialize(CAST([reinterpretAsFloat64(toUInt64(9223372036854775808)), 1.5], 'Array(LowCardinality(Float64))')), reinterpretAsFloat64(toUInt64(9223372036854775808))) AS negative_zero_needle;
SELECT has(materialize(CAST([0, NULL, 1.5], 'Array(LowCardinality(Nullable(Float64)))')), reinterpretAsFloat64(toUInt64(9223372036854775808))) AS lc, length(arrayFilter(x -> x = reinterpretAsFloat64(toUInt64(9223372036854775808)), materialize(CAST([0, NULL, 1.5], 'Array(LowCardinality(Nullable(Float64)))')))) AS oracle;

-- An Enum needle reaches a float element through its underlying number, so it must be declined too.
SELECT arrayMap(x -> reinterpretAsUInt64(x), a) AS stored_bits, has(a, CAST('z', 'Enum8(\'z\' = 0, \'o\' = 1)')) AS enum8_needle, indexOf(a, CAST('z', 'Enum8(\'z\' = 0, \'o\' = 1)')) AS enum8_index, countEqual(a, CAST('z', 'Enum16(\'z\' = 0, \'o\' = 1)')) AS enum16_count, length(arrayFilter(x -> x = toFloat64(0), a)) AS oracle FROM format(JSONEachRow, 'a Array(LowCardinality(Float64))', '{"a":[-0.0,1.5]}');

-- Controls that must not move.
SELECT has(materialize(CAST(['', 'a'], 'Array(LowCardinality(String))')), 'zzz') AS absent_needle, has(materialize(CAST(['a', 'b'], 'Array(LowCardinality(String))')), '') AS default_absent;
SELECT has(materialize(CAST(['', 'a'], 'Array(LowCardinality(String))')), materialize('')) AS non_const_needle, indexOfAssumeSorted(materialize(CAST(['', 'a'], 'Array(LowCardinality(String))')), '') AS assume_sorted;

-- One answer that only the dictionary shortcut produces, so a build that stopped taking it would
-- move it: a NaN is one dictionary entry but never equal to itself, so it disagrees with the
-- plain-array oracle printed beside it, a known defect of the value comparison tracked elsewhere and
-- not of the lookup this test covers. A negative needle over an unsigned element used to disagree
-- the same way; it now agrees, because a constant that does not survive the cast to the dictionary
-- type equals no element.
SELECT has(materialize(CAST([nan, 1.5], 'Array(LowCardinality(Float64))')), nan) AS nan_needle, has(materialize(CAST([nan, 1.5], 'Array(Float64)')), nan) AS oracle;
SELECT has(materialize(CAST([0, 255], 'Array(LowCardinality(UInt8))')), toInt8(-1)) AS negative_needle, has(materialize(CAST([0, 255], 'Array(UInt8)')), toInt8(-1)) AS oracle;

-- LowCardinality(Nullable(T)): a NULL needle finds the NULL element and a default needle finds the default one.
SELECT indexOf(materialize(CAST(['a', NULL, ''], 'Array(LowCardinality(Nullable(String)))')), NULL) AS null_needle, indexOf(materialize(CAST(['a', NULL, ''], 'Array(LowCardinality(Nullable(String)))')), '') AS default_needle;

-- Reached through a real table read rather than a constant-folded literal.
DROP TABLE IF EXISTS t_04881;
CREATE TABLE t_04881 (a Array(LowCardinality(String)), m Map(LowCardinality(String), String)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_04881 VALUES (['', 'a'], map('', 'v_empty', 'k', 'v_k')), (['x', 'y'], map('x', 'v_x', 'y', 'v_y'));
SELECT a, has(a, '') AS has_empty, indexOf(a, '') AS idx_empty, mapContainsKey(m, '') AS key_empty, m[''] AS subscript_empty FROM t_04881 ORDER BY a;
DROP TABLE t_04881;
