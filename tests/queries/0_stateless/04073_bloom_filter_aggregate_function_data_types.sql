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
    (SELECT groupBloomFilterState(1000)(reinterpretAsFloat32(toUInt32(0x7FC00000))) FROM numbers(1)) AS float32_nan_bf,
    (SELECT groupBloomFilterState(1000)(toFloat64(number * 0.1)) FROM numbers(100)) AS float64_bf,
    (SELECT groupBloomFilterState(1000)(toFloat64(0.0)) FROM numbers(1)) AS float64_zero_bf,
    (SELECT groupBloomFilterState(1000)(reinterpretAsFloat64(toUInt64(0x7FF8000000000000))) FROM numbers(1)) AS float64_nan_bf
SELECT
    bloomFilterContains(float32_bf, toFloat32(42)),
    bloomFilterContains(float32_zero_bf, toFloat32(-0.0)),
    bloomFilterContains(float32_nan_bf, reinterpretAsFloat32(toUInt32(0x7FC00000))),
    bloomFilterContains(float32_nan_bf, reinterpretAsFloat32(toUInt32(0xFFC00000))),
    bloomFilterContains(float64_bf, toFloat64(4.2)),
    bloomFilterContains(float64_zero_bf, toFloat64(-0.0)),
    bloomFilterContains(float64_nan_bf, reinterpretAsFloat64(toUInt64(0x7FF8000000000000))),
    bloomFilterContains(float64_nan_bf, reinterpretAsFloat64(toUInt64(0xFFF8000000000000)));

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

-- Implicit type casting: numeric filters with different numeric probe types
WITH
    (SELECT groupBloomFilterState(1000)(toInt64(number - 50)) FROM numbers(100)) AS int64_bf,
    (SELECT groupBloomFilterState(1000)(toFloat64(number * 0.1)) FROM numbers(100)) AS float64_bf,
    (SELECT groupBloomFilterState(1000)(toFloat32(16777216)) FROM numbers(1)) AS float32_boundary_bf,
    (SELECT groupBloomFilterState(1000)(toFloat64(9007199254740992)) FROM numbers(1)) AS float64_boundary_bf
SELECT
    bloomFilterContains(int64_bf, toInt32(-10)),
    bloomFilterContains(float64_bf, toFloat32(4.2)),
    bloomFilterContains(int64_bf, toDecimal32(42, 0)),
    bloomFilterContains(int64_bf, toDecimal64(42, 0)),
    bloomFilterContains(int64_bf, toDecimal128(42, 0)),
    bloomFilterContains(int64_bf, toDecimal256(42, 0)),
    bloomFilterContains(float64_bf, toDecimal64(4.2, 1)),
    bloomFilterContains(float32_boundary_bf, toDecimal32(16777216, 0)),
    bloomFilterContains(float32_boundary_bf, toDecimal32(16777217, 0)),
    bloomFilterContains(float64_boundary_bf, toDecimal64(9007199254740992, 0)),
    bloomFilterContains(float64_boundary_bf, toDecimal64(9007199254740993, 0));

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
    (SELECT groupBloomFilterArrayState(100)(materialize(CAST([42, NULL], 'Array(Nullable(UInt64))'))) FROM numbers(1)) AS nullable_array_uint64_bf
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
    bloomFilterContains(nullable_array_uint64_bf, toUInt64(7));

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

-- bloomFilterContains with aggregate state from a different function must throw
SELECT bloomFilterContains(sumState(number), toUInt64(1))
FROM numbers(1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- Numeric probes of `Bool` filters must preserve their value, including the `UInt8` cast path.
WITH
    (SELECT groupBloomFilterState(CAST(1 AS Bool))) AS true_bf,
    (SELECT groupBloomFilterState(CAST(0 AS Bool))) AS false_bf
SELECT
    bloomFilterContains(true_bf, toUInt8(1)),
    bloomFilterContains(true_bf, toUInt8(0)),
    bloomFilterContains(false_bf, toUInt8(0)),
    bloomFilterContains(false_bf, toUInt8(1)),
    bloomFilterContains(true_bf, toUInt8(42)),
    bloomFilterContains(true_bf, toUInt16(42)),
    bloomFilterContains(true_bf, toInt64(-1)),
    bloomFilterContains(true_bf, toFloat64(42)),
    bloomFilterContains(false_bf, toFloat64(0.5)),
    bloomFilterContains(true_bf, CAST(1 AS Bool));

-- Check every `UInt8` probe and keep nullable numeric probes nullable.
WITH
    (SELECT groupBloomFilterState(CAST(1 AS Bool))) AS true_bf,
    (SELECT groupBloomFilterState(CAST(0 AS Bool))) AS false_bf
SELECT
    countIf(bloomFilterContains(true_bf, toUInt8(number))),
    countIf(bloomFilterContains(false_bf, toUInt8(number)))
FROM numbers(256);

WITH
    (SELECT groupBloomFilterState(value) FROM (SELECT arrayJoin([false, true]) AS value)) AS bf
SELECT probe, bloomFilterContains(bf, probe)
FROM
(
    SELECT arrayJoin(CAST([0, 1, 2, 42, 255, NULL], 'Array(Nullable(UInt8))')) AS probe
)
ORDER BY probe NULLS LAST;

-- Out-of-range whole `Decimal` probes return absent instead of throwing `DECIMAL_OVERFLOW`.
WITH (SELECT groupBloomFilterState(toUInt8(42))) AS bf
SELECT
    bloomFilterContains(bf, toDecimal64('256', 0)),
    bloomFilterContains(bf, toDecimal64('-1', 0)),
    bloomFilterContains(bf, toDecimal64('42', 0));

-- All `Decimal` widths: valid boundaries, overflow, negative, fractional, and nullable probes in one column.
WITH
    (SELECT groupBloomFilterState(value) FROM (SELECT arrayJoin([toUInt8(0), 42, 255]) AS value)) AS bf
SELECT
    probe,
    bloomFilterContains(bf, toDecimal32(probe, 1)),
    bloomFilterContains(bf, toDecimal64(probe, 1)),
    bloomFilterContains(bf, toDecimal128(probe, 1)),
    bloomFilterContains(bf, toDecimal256(probe, 1))
FROM
(
    SELECT arrayJoin(CAST(['-1', '0', '42', '255', '256', '42.5', NULL], 'Array(Nullable(String))')) AS probe
)
ORDER BY probe NULLS LAST;

-- Signed destinations accept both endpoints and reject overflow on either side.
WITH
    (SELECT groupBloomFilterState(value) FROM (SELECT arrayJoin([toInt8(-128), 127]) AS value)) AS bf
SELECT
    probe,
    bloomFilterContains(bf, toDecimal32(probe, 0)),
    bloomFilterContains(bf, toDecimal64(probe, 0)),
    bloomFilterContains(bf, toDecimal128(probe, 0)),
    bloomFilterContains(bf, toDecimal256(probe, 0))
FROM
(
    SELECT arrayJoin(['-129', '-128', '127', '128']) AS probe
)
ORDER BY probe;

-- Decimal probes of `Bool` filters obey the same exact `0`/`1` contract.
WITH (SELECT groupBloomFilterState(true)) AS bf
SELECT
    bloomFilterContains(bf, toDecimal32('1', 0)),
    bloomFilterContains(bf, toDecimal64('2', 0)),
    bloomFilterContains(bf, toDecimal128('256', 0)),
    bloomFilterContains(bf, toDecimal256('-1', 0));

-- Wider integer bounds must be checked before narrowing the whole part.
WITH (SELECT groupBloomFilterState(value) FROM (SELECT arrayJoin([toInt16('-32768'), toInt16('32767')]) AS value)) AS bf
SELECT
    bloomFilterContains(bf, toDecimal32('-32769', 0)),
    bloomFilterContains(bf, toDecimal32('-32768', 0)),
    bloomFilterContains(bf, toDecimal32('32767', 0)),
    bloomFilterContains(bf, toDecimal32('32768', 0));

WITH (SELECT groupBloomFilterState(value) FROM (SELECT arrayJoin([toUInt16('0'), toUInt16('65535')]) AS value)) AS bf
SELECT
    bloomFilterContains(bf, toDecimal32('-1', 0)),
    bloomFilterContains(bf, toDecimal32('0', 0)),
    bloomFilterContains(bf, toDecimal32('65535', 0)),
    bloomFilterContains(bf, toDecimal32('65536', 0));

WITH (SELECT groupBloomFilterState(value) FROM (SELECT arrayJoin([toInt32('-2147483648'), toInt32('2147483647')]) AS value)) AS bf
SELECT
    bloomFilterContains(bf, toDecimal64('-2147483649', 0)),
    bloomFilterContains(bf, toDecimal64('-2147483648', 0)),
    bloomFilterContains(bf, toDecimal64('2147483647', 0)),
    bloomFilterContains(bf, toDecimal64('2147483648', 0));

WITH (SELECT groupBloomFilterState(value) FROM (SELECT arrayJoin([toUInt32('0'), toUInt32('4294967295')]) AS value)) AS bf
SELECT
    bloomFilterContains(bf, toDecimal64('-1', 0)),
    bloomFilterContains(bf, toDecimal64('0', 0)),
    bloomFilterContains(bf, toDecimal64('4294967295', 0)),
    bloomFilterContains(bf, toDecimal64('4294967296', 0));

WITH (SELECT groupBloomFilterState(value) FROM (SELECT arrayJoin([toInt64('-9223372036854775808'), toInt64('9223372036854775807')]) AS value)) AS bf
SELECT
    bloomFilterContains(bf, toDecimal128('-9223372036854775809', 0)),
    bloomFilterContains(bf, toDecimal128('-9223372036854775808', 0)),
    bloomFilterContains(bf, toDecimal128('9223372036854775807', 0)),
    bloomFilterContains(bf, toDecimal128('9223372036854775808', 0));

WITH (SELECT groupBloomFilterState(value) FROM (SELECT arrayJoin([toUInt64('0'), toUInt64('18446744073709551615')]) AS value)) AS bf
SELECT
    bloomFilterContains(bf, toDecimal128('-1', 0)),
    bloomFilterContains(bf, toDecimal128('0', 0)),
    bloomFilterContains(bf, toDecimal128('18446744073709551615', 0)),
    bloomFilterContains(bf, toDecimal128('18446744073709551616', 0));

WITH (SELECT groupBloomFilterState(value) FROM (SELECT arrayJoin([toInt128('-170141183460469231731687303715884105728'), toInt128('170141183460469231731687303715884105727')]) AS value)) AS bf
SELECT
    bloomFilterContains(bf, toDecimal256('-170141183460469231731687303715884105729', 0)),
    bloomFilterContains(bf, toDecimal256('-170141183460469231731687303715884105728', 0)),
    bloomFilterContains(bf, toDecimal256('170141183460469231731687303715884105727', 0)),
    bloomFilterContains(bf, toDecimal256('170141183460469231731687303715884105728', 0));

WITH (SELECT groupBloomFilterState(value) FROM (SELECT arrayJoin([toUInt128('0'), toUInt128('340282366920938463463374607431768211455')]) AS value)) AS bf
SELECT
    bloomFilterContains(bf, toDecimal256('-1', 0)),
    bloomFilterContains(bf, toDecimal256('0', 0)),
    bloomFilterContains(bf, toDecimal256('340282366920938463463374607431768211455', 0)),
    bloomFilterContains(bf, toDecimal256('340282366920938463463374607431768211456', 0));

-- Every valid `Decimal256` whole part fits `Int256`; negative values still do not fit `UInt256`.
WITH
    (SELECT groupBloomFilterState(toInt256('-1'))) AS signed_bf,
    (SELECT groupBloomFilterState(toUInt256('0'))) AS unsigned_bf
SELECT
    bloomFilterContains(signed_bf, toDecimal256('-1', 0)),
    bloomFilterContains(unsigned_bf, toDecimal256('-1', 0)),
    bloomFilterContains(unsigned_bf, toDecimal256('0', 0));

-- Fractional `Decimal` probes must not alias an inserted integer after truncation.
WITH (SELECT groupBloomFilterState(toUInt64(42))) AS bf
SELECT
    bloomFilterContains(bf, toDecimal32('42.5', 1)),
    bloomFilterContains(bf, toDecimal32('42.0', 1)),
    bloomFilterContains(bf, toDecimal64('42.5', 1)),
    bloomFilterContains(bf, toDecimal64('42.0', 1)),
    bloomFilterContains(bf, toDecimal128('42.5', 1)),
    bloomFilterContains(bf, toDecimal128('42.0', 1)),
    bloomFilterContains(bf, toDecimal256('42.5', 1)),
    bloomFilterContains(bf, toDecimal256('42.0', 1)),
    bloomFilterContains(bf, toDecimal64('42', 0));

-- Negative fractions must not alias signed integers or unsigned zero.
WITH
    (SELECT groupBloomFilterState(toInt64(-42))) AS signed_bf,
    (SELECT groupBloomFilterState(toUInt64(0))) AS unsigned_bf
SELECT
    bloomFilterContains(signed_bf, toDecimal64('-42.5', 1)),
    bloomFilterContains(signed_bf, toDecimal64('-42.0', 1)),
    bloomFilterContains(unsigned_bf, toDecimal64('-0.5', 1)),
    bloomFilterContains(unsigned_bf, toDecimal64('0.0', 1));

-- Exact wide integers must remain valid without an integer-to-`Decimal` round trip.
WITH
    (SELECT groupBloomFilterState(toInt128('-18446744073709551616'))) AS int128_bf,
    (SELECT groupBloomFilterState(toUInt128('18446744073709551616'))) AS uint128_bf,
    (SELECT groupBloomFilterState(toInt256('-170141183460469231731687303715884105728'))) AS int256_bf,
    (SELECT groupBloomFilterState(toUInt256('170141183460469231731687303715884105728'))) AS uint256_bf
SELECT
    bloomFilterContains(int128_bf, toDecimal128('-18446744073709551616.5', 1)),
    bloomFilterContains(int128_bf, toDecimal128('-18446744073709551616.0', 1)),
    bloomFilterContains(uint128_bf, toDecimal128('18446744073709551616.5', 1)),
    bloomFilterContains(uint128_bf, toDecimal128('18446744073709551616.0', 1)),
    bloomFilterContains(int256_bf, toDecimal256('-170141183460469231731687303715884105728.5', 1)),
    bloomFilterContains(int256_bf, toDecimal256('-170141183460469231731687303715884105728.0', 1)),
    bloomFilterContains(uint256_bf, toDecimal256('170141183460469231731687303715884105728.5', 1)),
    bloomFilterContains(uint256_bf, toDecimal256('170141183460469231731687303715884105728.0', 1));

-- Exercise vector probes and preserve `NULL` propagation for each `Decimal` width.
WITH
    (SELECT groupBloomFilterState(value) FROM (SELECT arrayJoin([toUInt64(0), 42]) AS value)) AS bf
SELECT
    probe,
    bloomFilterContains(bf, toDecimal32(probe, 1)),
    bloomFilterContains(bf, toDecimal64(probe, 1)),
    bloomFilterContains(bf, toDecimal128(probe, 1)),
    bloomFilterContains(bf, toDecimal256(probe, 1))
FROM
(
    SELECT arrayJoin(CAST(['-0.5', '0.0', '42.0', '42.5', NULL], 'Array(Nullable(String))')) AS probe
)
ORDER BY probe NULLS LAST;

-- Basic construction, execution shapes, and membership checks for `groupBloomFilter` and `bloomFilterContains`.

-- Positive checks and state type name.
WITH
    (SELECT groupBloomFilterState(1000)(number) FROM numbers(100)) AS bf,
    (SELECT groupBloomFilterState(1000)(number) AS state FROM numbers(100)) AS subquery_bf,
    (SELECT groupBloomFilterState(1000)(number) FROM numbers(10)) AS type_bf
SELECT
    bloomFilterContains(bf, toUInt64(42)),
    bloomFilterContains(bf, toUInt64(200)),
    bloomFilterContains(subquery_bf, toUInt64(42)),
    toTypeName(type_bf) LIKE 'AggregateFunction(1, groupBloomFilter%';

-- Const Bloom filter states from `WITH` clauses.
WITH
    (SELECT groupBloomFilterState(1000)(number) FROM numbers(100)) AS uint64_bf,
    (SELECT groupBloomFilterState(1000)(toString(number)) FROM numbers(100)) AS string_bf,
    (SELECT groupBloomFilterState(1000)(toDateTime64('2023-01-01 12:00:00.123', 3)) FROM numbers(1)) AS datetime64_bf
SELECT
    bloomFilterContains(uint64_bf, toUInt64(42)),
    bloomFilterContains(string_bf, '42'),
    bloomFilterContains(datetime64_bf, toDateTime64('2023-01-01 12:00:00.123', 3));

-- Const Bloom filter states with non-const probe columns.
WITH
    (SELECT groupBloomFilterState(1000)(toString(number)) FROM numbers(100)) AS string_bf,
    (SELECT groupBloomFilterState(1000)(toDateTime64('2023-01-01 12:00:00.123', 3)) FROM numbers(1)) AS datetime64_bf
SELECT
    (SELECT sum(bloomFilterContains(string_bf, toString(number))) FROM numbers(10)),
    (SELECT sum(bloomFilterContains(datetime64_bf, toDateTime64('2023-01-01 12:00:00.123', 3) + toIntervalMillisecond(number - number))) FROM numbers(10));

-- Bloom filter and probe are both columns.
SELECT bloomFilterContains(bf, val) AS result
FROM
(
    SELECT
        groupBloomFilterState(1000)(number) AS bf,
        toUInt64(42) AS val
    FROM numbers(100)
);

-- Values in 100..199 are absent from the filter built on 0..99.
WITH
(
    SELECT groupBloomFilterState(1000)(number)
    FROM numbers(100)
) AS old_bloom
SELECT
    count() AS new_values_count,
    count() = 100 AS all_new_values_found
FROM numbers(200)
WHERE number >= 100
  AND NOT bloomFilterContains(old_bloom, number);

-- Values from 0..99 must have no false negatives.
WITH
(
    SELECT groupBloomFilterState(1000)(number)
    FROM numbers(100)
) AS old_bloom
SELECT count() = 0 AS no_false_negatives
FROM numbers(100)
WHERE NOT bloomFilterContains(old_bloom, number);

-- Build and probe Bloom filter states per group.
SELECT key, bloomFilterContains(bf, toUInt64(key + 4)) AS result
FROM
(
    SELECT number % 2 AS key, groupBloomFilterState(1000)(number) AS bf
    FROM numbers(10)
    GROUP BY key
)
ORDER BY key;

-- Empty and boundary values.
WITH
    (SELECT groupBloomFilterState(1000)(number) FROM numbers(0)) AS empty_bf,
    (SELECT groupBloomFilterState(1000)(toString(number)) FROM numbers(10)) AS string_bf,
    (SELECT groupBloomFilterState(1000)(s) FROM (SELECT '' AS s)) AS empty_string_bf,
    (SELECT groupBloomFilterState(1000)(repeat('x', 1000)) FROM numbers(1)) AS long_string_bf
SELECT
    bloomFilterContains(empty_bf, toUInt64(42)),
    bloomFilterContains(string_bf, ''),
    bloomFilterContains(empty_string_bf, ''),
    bloomFilterContains(long_string_bf, repeat('x', 1000));
