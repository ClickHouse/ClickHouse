-- Tests for groupBloomFilter aggregate function -- data types

-- String and FixedString
WITH
    (SELECT groupBloomFilterState(1000)(toString(number)) FROM numbers(100)) AS string_bf,
    (SELECT groupBloomFilterState(1000)(toFixedString('hello', 10)) FROM numbers(1)) AS fixed_string_bf
SELECT
    bloomFilterContains(string_bf, '42'),
    bloomFilterContains(string_bf, 'not_in_filter'),
    bloomFilterContains(fixed_string_bf, toFixedString('hello', 10)),
    bloomFilterContains(fixed_string_bf, toFixedString('world', 10));

-- Small integer types
WITH
    (SELECT groupBloomFilterState(1000)(toUInt8(number % 256)) FROM numbers(100)) AS uint8_bf,
    (SELECT groupBloomFilterState(1000)(toUInt16(number)) FROM numbers(100)) AS uint16_bf,
    (SELECT groupBloomFilterState(4096, 5)(toUInt32(number)) FROM numbers(100)) AS uint32_bf,
    (SELECT groupBloomFilterState(4096, 5)(toInt16(number - 50)) FROM numbers(100)) AS int16_bf,
    (SELECT groupBloomFilterState(1000)(toInt32(number - 50)) FROM numbers(100)) AS int32_bf,
    (SELECT groupBloomFilterState(1000)(toInt64(number - 50)) FROM numbers(100)) AS int64_bf
SELECT
    bloomFilterContains(uint8_bf, toUInt8(42)),
    bloomFilterContains(uint16_bf, toUInt16(42)),
    bloomFilterContains(uint32_bf, toUInt32(42)),
    bloomFilterContains(uint32_bf, toUInt32(100000)),
    bloomFilterContains(int16_bf, toInt16(-10)),
    bloomFilterContains(int16_bf, toInt16(1000)),
    bloomFilterContains(int32_bf, toInt32(-10)),
    bloomFilterContains(int64_bf, toInt64(-10));

-- Floating point types and canonical values
WITH
    (SELECT groupBloomFilterState(1000)(toFloat32(number)) FROM numbers(100)) AS float32_bf,
    (SELECT groupBloomFilterState(1000)(toFloat32(0.0)) FROM numbers(1)) AS float32_zero_bf,
    (SELECT groupBloomFilterState(1000)(CAST(nan AS Float32)) FROM numbers(1)) AS float32_nan_bf,
    (SELECT groupBloomFilterState(1000)(toFloat64(number * 0.1)) FROM numbers(100)) AS float64_bf,
    (SELECT groupBloomFilterState(1000)(toFloat64(0.0)) FROM numbers(1)) AS float64_zero_bf,
    (SELECT groupBloomFilterState(1000)(CAST(nan AS Float64)) FROM numbers(1)) AS float64_nan_bf
SELECT
    bloomFilterContains(float32_bf, toFloat32(42)),
    bloomFilterContains(float32_zero_bf, toFloat32(-0.0)),
    bloomFilterContains(float32_nan_bf, CAST(-nan AS Float32)),
    bloomFilterContains(float64_bf, toFloat64(4.2)),
    bloomFilterContains(float64_zero_bf, toFloat64(-0.0)),
    bloomFilterContains(float64_nan_bf, CAST(-nan AS Float64));

-- Date and time types
WITH
    (SELECT groupBloomFilterState(1000)(toDate('2023-01-01')) FROM numbers(1)) AS date_bf,
    (SELECT groupBloomFilterState(1000)(toDate32('2023-01-01')) FROM numbers(1)) AS date32_bf,
    (SELECT groupBloomFilterState(1000)(toDateTime('2023-01-01 12:00:00')) FROM numbers(1)) AS datetime_bf,
    (SELECT groupBloomFilterState(1000)(toDateTime64('2023-01-01 12:00:00.123456', 6)) FROM numbers(1)) AS datetime64_bf
SELECT
    bloomFilterContains(date_bf, toDate('2023-01-01')),
    bloomFilterContains(date_bf, toDate('2023-12-31')),
    bloomFilterContains(date32_bf, toDate32('2023-01-01')),
    bloomFilterContains(date32_bf, toDate32('2023-12-31')),
    bloomFilterContains(datetime_bf, toDateTime('2023-01-01 12:00:00')),
    bloomFilterContains(datetime_bf, toDateTime('2023-12-31 23:59:59')),
    bloomFilterContains(datetime64_bf, toDateTime64('2023-01-01 12:00:00.123456', 6)),
    bloomFilterContains(datetime64_bf, toDateTime64('2023-12-31 23:59:59.999999', 6));

-- UUID, network, and enum types
WITH
    (SELECT groupBloomFilterState(1000)(toUUID('123e4567-e89b-12d3-a456-426614174000')) FROM numbers(1)) AS uuid_bf,
    (SELECT groupBloomFilterState(1000)(toIPv4('192.168.1.1')) FROM numbers(1)) AS ipv4_bf,
    (SELECT groupBloomFilterState(1000)(toIPv6('2001:0db8:85a3:0000:0000:8a2e:0370:7334')) FROM numbers(1)) AS ipv6_bf,
    (SELECT groupBloomFilterState(1000)(CAST('hello' AS Enum8('hello' = 1, 'world' = 2))) FROM numbers(1)) AS enum8_bf,
    (SELECT groupBloomFilterState(1000)(CAST('hello' AS Enum16('hello' = 1000, 'world' = 2000))) FROM numbers(1)) AS enum16_bf
SELECT
    bloomFilterContains(uuid_bf, toUUID('123e4567-e89b-12d3-a456-426614174000')),
    bloomFilterContains(uuid_bf, toUUID('123e4567-e89b-12d3-a456-426614174001')),
    bloomFilterContains(ipv4_bf, toIPv4('192.168.1.1')),
    bloomFilterContains(ipv4_bf, toIPv4('192.168.1.2')),
    bloomFilterContains(ipv6_bf, toIPv6('2001:0db8:85a3:0000:0000:8a2e:0370:7334')),
    bloomFilterContains(ipv6_bf, toIPv6('2001:0db8:85a3:0000:0000:8a2e:0370:7335')),
    bloomFilterContains(enum8_bf, CAST('hello' AS Enum8('hello' = 1, 'world' = 2))),
    bloomFilterContains(enum8_bf, CAST('world' AS Enum8('hello' = 1, 'world' = 2))),
    bloomFilterContains(enum16_bf, CAST('hello' AS Enum16('hello' = 1000, 'world' = 2000))),
    bloomFilterContains(enum16_bf, CAST('world' AS Enum16('hello' = 1000, 'world' = 2000)));

-- Wide integer types
WITH
    (SELECT groupBloomFilterState(1000)(toUInt128('123456789012345678901234567890')) FROM numbers(1)) AS uint128_bf,
    (SELECT groupBloomFilterState(1000)(toUInt256('123456789012345678901234567890123456789012345678901234567890')) FROM numbers(1)) AS uint256_bf,
    (SELECT groupBloomFilterState(1000)(toInt128('123456789012345678901234567890')) FROM numbers(1)) AS int128_bf,
    (SELECT groupBloomFilterState(1000)(toInt256('123456789012345678901234567890123456789012345678901234567890')) FROM numbers(1)) AS int256_bf
SELECT
    bloomFilterContains(uint128_bf, toUInt128('123456789012345678901234567890')),
    bloomFilterContains(uint128_bf, toUInt128('123456789012345678901234567891')),
    bloomFilterContains(uint256_bf, toUInt256('123456789012345678901234567890123456789012345678901234567890')),
    bloomFilterContains(uint256_bf, toUInt256('123456789012345678901234567890123456789012345678901234567891')),
    bloomFilterContains(int128_bf, toInt128('123456789012345678901234567890')),
    bloomFilterContains(int128_bf, toInt128('123456789012345678901234567891')),
    bloomFilterContains(int256_bf, toInt256('123456789012345678901234567890123456789012345678901234567890')),
    bloomFilterContains(int256_bf, toInt256('123456789012345678901234567890123456789012345678901234567891'));

-- Implicit type casting: `UInt64` filter with narrower/wider numeric probes.
-- Inserted values must have no false negatives; absent probes may have bounded false positives.
WITH
    (SELECT groupBloomFilterState(1000)(number) FROM numbers(1000)) AS bf,
    (SELECT countIf(bloomFilterContains(bf, number % 1000)) FROM numbers(100000)) AS exact_hits,
    (SELECT countIf(bloomFilterContains(bf, number % 150000)) FROM numbers(100000)) AS maybe_hits
SELECT
    exact_hits = 100000,
    maybe_hits >= 1000,
    maybe_hits < 10000;

-- Implicit type casting: Int64 and Float64 filters with narrower expressions
WITH
    (SELECT groupBloomFilterState(1000)(toInt64(number - 50)) FROM numbers(100)) AS int64_bf,
    (SELECT groupBloomFilterState(1000)(toFloat64(number * 0.1)) FROM numbers(100)) AS float64_bf
SELECT
    bloomFilterContains(int64_bf, toInt32(-10)),
    bloomFilterContains(float64_bf, toFloat32(4.2));

-- Nullable, LowCardinality, narrowing, and lossy probes
WITH
    (SELECT groupBloomFilterState(100)(materialize(CAST('hello', 'Nullable(String)'))) FROM numbers(1)) AS nullable_string_bf,
    (SELECT groupBloomFilterState(100)(materialize(CAST(42, 'Nullable(UInt64)'))) FROM numbers(1)) AS nullable_uint64_bf,
    (SELECT groupBloomFilterState(100)(toUInt8(44)) FROM numbers(1)) AS uint8_bf,
    (SELECT groupBloomFilterState(100)(toInt8(-1)) FROM numbers(1)) AS int8_bf,
    (SELECT groupBloomFilterState(100)(toUInt8(42)) FROM numbers(1)) AS uint8_lossy_bf,
    (SELECT groupBloomFilterState(100)(materialize(CAST('hello', 'LowCardinality(Nullable(String))'))) FROM numbers(1)) AS low_cardinality_nullable_string_bf,
    (SELECT groupBloomFilterState(100)(materialize(CAST(NULL, 'Nullable(UInt64)'))) FROM numbers(1)) AS all_null_uint64_bf,
    (SELECT groupBloomFilterState(100)(materialize(CAST(toDateTime64('2023-01-01 12:00:00.123', 3), 'Nullable(DateTime64(3))'))) FROM numbers(1)) AS nullable_datetime64_bf,
    (SELECT groupBloomFilterArrayState(100)(materialize(CAST([42, NULL], 'Array(Nullable(UInt64))'))) FROM numbers(1)) AS nullable_array_uint64_bf,
    (SELECT groupBloomFilterIfState(100)(materialize(CAST(42, 'Nullable(UInt64)')), number = 0) FROM numbers(1)) AS nullable_if_uint64_bf
SELECT
    bloomFilterContains(nullable_string_bf, 'hello'),
    bloomFilterContains(nullable_uint64_bf, toUInt16(42)),
    bloomFilterContains(uint8_bf, toUInt16(44)),
    bloomFilterContains(uint8_bf, toUInt16(300)),
    bloomFilterContains(int8_bf, toInt16(-1)),
    bloomFilterContains(int8_bf, toInt16(255)),
    bloomFilterContains(uint8_lossy_bf, toUInt8(42)),
    bloomFilterContains(uint8_lossy_bf, materialize(42.5)),
    bloomFilterContains(low_cardinality_nullable_string_bf, 'hello'),
    bloomFilterContains(nullable_string_bf, CAST(NULL, 'Nullable(String)')),
    bloomFilterContains(all_null_uint64_bf, toUInt64(42)),
    bloomFilterContains(nullable_uint64_bf, toUInt64(42)),
    bloomFilterContains(nullable_datetime64_bf, toDateTime64('2023-01-01 12:00:00.123', 3)),
    bloomFilterContains(nullable_array_uint64_bf, toUInt64(42)),
    bloomFilterContains(nullable_array_uint64_bf, toUInt64(7)),
    bloomFilterContains(nullable_if_uint64_bf, toUInt64(42));

-- bloomFilterContains with incompatible value type must throw instead of reading a wrong column type
WITH (SELECT groupBloomFilterState(100)(toUUID('550e8400-e29b-41d4-a716-446655440000')) FROM numbers(1)) AS bf
SELECT bloomFilterContains(bf, toUInt64(42)); -- { serverError NOT_IMPLEMENTED }

-- bloomFilterContains with incompatible value type must be rejected during analysis, even without execution.
SELECT bloomFilterContains(bf, toUInt64(42))
FROM
(
    SELECT groupBloomFilterState(100)(toUUID('550e8400-e29b-41d4-a716-446655440000')) AS bf
    FROM numbers(0)
)
LIMIT 0; -- { serverError NOT_IMPLEMENTED }

-- bloomFilterContains with non-bloom first argument must throw
SELECT bloomFilterContains(42, toUInt64(1)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
