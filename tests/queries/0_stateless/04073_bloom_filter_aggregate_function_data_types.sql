-- Tests for groupBloomFilter aggregate function -- data types

-- String: value present
SELECT bloomFilterContains(
    groupBloomFilterState(1000)(toString(number)),
    '42'
) AS result
FROM numbers(100);

-- String: value absent
SELECT bloomFilterContains(
    groupBloomFilterState(1000)(toString(number)),
    'not_in_filter'
) AS result
FROM numbers(100);

-- FixedString: value present
SELECT bloomFilterContains(
    groupBloomFilterState(1000)(toFixedString('hello', 10)),
    toFixedString('hello', 10)
) AS result
FROM numbers(1);

-- FixedString: value absent
SELECT bloomFilterContains(
    groupBloomFilterState(1000)(toFixedString('hello', 10)),
    toFixedString('world', 10)
) AS result
FROM numbers(1);

-- UInt8
SELECT bloomFilterContains(groupBloomFilterState(1000)(toUInt8(number % 256)), toUInt8(42)) AS result
FROM numbers(100);

-- UInt16
SELECT bloomFilterContains(groupBloomFilterState(1000)(toUInt16(number)), toUInt16(42)) AS result
FROM numbers(100);

-- UInt32: value present
SELECT bloomFilterContains(groupBloomFilterState(4096, 5)(toUInt32(number)), toUInt32(42)) AS result
FROM numbers(100);

-- UInt32: value absent
SELECT bloomFilterContains(groupBloomFilterState(4096, 5)(toUInt32(number)), toUInt32(100000)) AS result
FROM numbers(100);

-- Int16: value present
SELECT bloomFilterContains(groupBloomFilterState(4096, 5)(toInt16(number - 50)), toInt16(-10)) AS result
FROM numbers(100);

-- Int16: value absent
SELECT bloomFilterContains(groupBloomFilterState(4096, 5)(toInt16(number - 50)), toInt16(1000)) AS result
FROM numbers(100);

-- Int32
SELECT bloomFilterContains(groupBloomFilterState(1000)(toInt32(number - 50)), toInt32(-10)) AS result
FROM numbers(100);

-- Int64
SELECT bloomFilterContains(groupBloomFilterState(1000)(toInt64(number - 50)), toInt64(-10)) AS result
FROM numbers(100);

-- Float32
SELECT bloomFilterContains(groupBloomFilterState(1000)(toFloat32(number)), toFloat32(42)) AS result
FROM numbers(100);

-- Float32: signed zero must not cause a false negative
WITH (SELECT groupBloomFilterState(1000)(toFloat32(0.0)) FROM numbers(1)) AS bf
SELECT bloomFilterContains(bf, toFloat32(-0.0));

-- Float32: NaN encodings are canonicalized for hashing
WITH (SELECT groupBloomFilterState(1000)(CAST(nan AS Float32)) FROM numbers(1)) AS bf
SELECT bloomFilterContains(bf, CAST(-nan AS Float32));

-- Float64
SELECT bloomFilterContains(groupBloomFilterState(1000)(toFloat64(number * 0.1)), toFloat64(4.2)) AS result
FROM numbers(100);

-- Float64: signed zero must not cause a false negative
WITH (SELECT groupBloomFilterState(1000)(toFloat64(0.0)) FROM numbers(1)) AS bf
SELECT bloomFilterContains(bf, toFloat64(-0.0));

-- Float64: NaN encodings are canonicalized for hashing
WITH (SELECT groupBloomFilterState(1000)(CAST(nan AS Float64)) FROM numbers(1)) AS bf
SELECT bloomFilterContains(bf, CAST(-nan AS Float64));

-- Date: value present
SELECT bloomFilterContains(groupBloomFilterState(1000)(toDate('2023-01-01')), toDate('2023-01-01')) AS result
FROM numbers(1);

-- Date: value absent
SELECT bloomFilterContains(groupBloomFilterState(1000)(toDate('2023-01-01')), toDate('2023-12-31')) AS result
FROM numbers(1);

-- Date32: value present
SELECT bloomFilterContains(groupBloomFilterState(1000)(toDate32('2023-01-01')), toDate32('2023-01-01')) AS result
FROM numbers(1);

-- Date32: value absent
SELECT bloomFilterContains(groupBloomFilterState(1000)(toDate32('2023-01-01')), toDate32('2023-12-31')) AS result
FROM numbers(1);

-- DateTime: value present
SELECT bloomFilterContains(groupBloomFilterState(1000)(toDateTime('2023-01-01 12:00:00')), toDateTime('2023-01-01 12:00:00')) AS result
FROM numbers(1);

-- DateTime: value absent
SELECT bloomFilterContains(groupBloomFilterState(1000)(toDateTime('2023-01-01 12:00:00')), toDateTime('2023-12-31 23:59:59')) AS result
FROM numbers(1);

-- DateTime64: value present
SELECT bloomFilterContains(groupBloomFilterState(1000)(toDateTime64('2023-01-01 12:00:00.123456', 6)), toDateTime64('2023-01-01 12:00:00.123456', 6)) AS result
FROM numbers(1);

-- DateTime64: value absent
SELECT bloomFilterContains(groupBloomFilterState(1000)(toDateTime64('2023-01-01 12:00:00.123456', 6)), toDateTime64('2023-12-31 23:59:59.999999', 6)) AS result
FROM numbers(1);

-- UUID: value present
SELECT bloomFilterContains(groupBloomFilterState(1000)(toUUID('123e4567-e89b-12d3-a456-426614174000')), toUUID('123e4567-e89b-12d3-a456-426614174000')) AS result
FROM numbers(1);

-- UUID: value absent
SELECT bloomFilterContains(groupBloomFilterState(1000)(toUUID('123e4567-e89b-12d3-a456-426614174000')), toUUID('123e4567-e89b-12d3-a456-426614174001')) AS result
FROM numbers(1);

-- IPv4: value present
SELECT bloomFilterContains(groupBloomFilterState(1000)(toIPv4('192.168.1.1')), toIPv4('192.168.1.1')) AS result
FROM numbers(1);

-- IPv4: value absent
SELECT bloomFilterContains(groupBloomFilterState(1000)(toIPv4('192.168.1.1')), toIPv4('192.168.1.2')) AS result
FROM numbers(1);

-- IPv6: value present
SELECT bloomFilterContains(groupBloomFilterState(1000)(toIPv6('2001:0db8:85a3:0000:0000:8a2e:0370:7334')), toIPv6('2001:0db8:85a3:0000:0000:8a2e:0370:7334')) AS result
FROM numbers(1);

-- IPv6: value absent
SELECT bloomFilterContains(groupBloomFilterState(1000)(toIPv6('2001:0db8:85a3:0000:0000:8a2e:0370:7334')), toIPv6('2001:0db8:85a3:0000:0000:8a2e:0370:7335')) AS result
FROM numbers(1);

-- Enum8: value present
SELECT bloomFilterContains(groupBloomFilterState(1000)(CAST('hello' AS Enum8('hello' = 1, 'world' = 2))), CAST('hello' AS Enum8('hello' = 1, 'world' = 2))) AS result
FROM numbers(1);

-- Enum8: value absent
SELECT bloomFilterContains(groupBloomFilterState(1000)(CAST('hello' AS Enum8('hello' = 1, 'world' = 2))), CAST('world' AS Enum8('hello' = 1, 'world' = 2))) AS result
FROM numbers(1);

-- Enum16: value present
SELECT bloomFilterContains(groupBloomFilterState(1000)(CAST('hello' AS Enum16('hello' = 1000, 'world' = 2000))), CAST('hello' AS Enum16('hello' = 1000, 'world' = 2000))) AS result
FROM numbers(1);

-- Enum16: value absent
SELECT bloomFilterContains(groupBloomFilterState(1000)(CAST('hello' AS Enum16('hello' = 1000, 'world' = 2000))), CAST('world' AS Enum16('hello' = 1000, 'world' = 2000))) AS result
FROM numbers(1);

-- UInt128: value present
SELECT bloomFilterContains(groupBloomFilterState(1000)(toUInt128('123456789012345678901234567890')), toUInt128('123456789012345678901234567890')) AS result
FROM numbers(1);

-- UInt128: value absent
SELECT bloomFilterContains(groupBloomFilterState(1000)(toUInt128('123456789012345678901234567890')), toUInt128('123456789012345678901234567891')) AS result
FROM numbers(1);

-- UInt256: value present
SELECT bloomFilterContains(groupBloomFilterState(1000)(toUInt256('123456789012345678901234567890123456789012345678901234567890')), toUInt256('123456789012345678901234567890123456789012345678901234567890')) AS result
FROM numbers(1);

-- UInt256: value absent
SELECT bloomFilterContains(groupBloomFilterState(1000)(toUInt256('123456789012345678901234567890123456789012345678901234567890')), toUInt256('123456789012345678901234567890123456789012345678901234567891')) AS result
FROM numbers(1);

-- Int128: value present
SELECT bloomFilterContains(groupBloomFilterState(1000)(toInt128('123456789012345678901234567890')), toInt128('123456789012345678901234567890')) AS result
FROM numbers(1);

-- Int128: value absent
SELECT bloomFilterContains(groupBloomFilterState(1000)(toInt128('123456789012345678901234567890')), toInt128('123456789012345678901234567891')) AS result
FROM numbers(1);

-- Int256: value present
SELECT bloomFilterContains(groupBloomFilterState(1000)(toInt256('123456789012345678901234567890123456789012345678901234567890')), toInt256('123456789012345678901234567890123456789012345678901234567890')) AS result
FROM numbers(1);

-- Int256: value absent
SELECT bloomFilterContains(groupBloomFilterState(1000)(toInt256('123456789012345678901234567890123456789012345678901234567890')), toInt256('123456789012345678901234567890123456789012345678901234567891')) AS result
FROM numbers(1);

-- Implicit type casting: UInt64 filter with UInt16 value (number % 1000)
-- Regression test for segfault when value type doesn't match bloom filter's element type
WITH (SELECT groupBloomFilterState(1000)(number) FROM numbers(1000)) AS bf
SELECT countIf(bloomFilterContains(bf, number % 1000))
FROM numbers(100000);

-- Implicit type casting: UInt64 filter with UInt32 value (number % 150000)
WITH (SELECT groupBloomFilterState(1000)(number) FROM numbers(1000)) AS bf
SELECT countIf(bloomFilterContains(bf, number % 150000))
FROM numbers(100000);

-- Implicit type casting: Int64 filter with Int32 expression
WITH (SELECT groupBloomFilterState(1000)(toInt64(number - 50)) FROM numbers(100)) AS bf
SELECT bloomFilterContains(bf, toInt32(-10)) AS result
FROM numbers(10) LIMIT 1;

-- Implicit type casting: Float64 filter with Float32 expression
WITH (SELECT groupBloomFilterState(1000)(toFloat64(number * 0.1)) FROM numbers(100)) AS bf
SELECT bloomFilterContains(bf, toFloat32(4.2)) AS result
FROM numbers(10) LIMIT 1;

-- Nullable(String): value present
WITH (SELECT groupBloomFilterState(100)(materialize(CAST('hello', 'Nullable(String)'))) FROM numbers(1)) AS bf
SELECT bloomFilterContains(bf, 'hello');

-- Nullable(UInt64): value present after implicit cast from narrower numeric type
WITH (SELECT groupBloomFilterState(100)(materialize(CAST(42, 'Nullable(UInt64)'))) FROM numbers(1)) AS bf
SELECT bloomFilterContains(bf, toUInt16(42));

-- Narrowing probe cast: a UInt8 filter probed with an out-of-range UInt16 must not wrap/truncate.
-- 300 truncated to UInt8 would be 44; the filter only ever saw 44, so a wrapping cast would report 1.
WITH (SELECT groupBloomFilterState(100)(toUInt8(44)) FROM numbers(1)) AS bf
SELECT bloomFilterContains(bf, toUInt16(44)), bloomFilterContains(bf, toUInt16(300));

-- Narrowing probe cast: signed out-of-range value must not alias an in-range one.
WITH (SELECT groupBloomFilterState(100)(toInt8(-1)) FROM numbers(1)) AS bf
SELECT bloomFilterContains(bf, toInt16(-1)), bloomFilterContains(bf, toInt16(255));

-- Lossy probe cast: a UInt8 filter probed with a fractional Float64 must not round/truncate.
WITH (SELECT groupBloomFilterState(100)(toUInt8(42)) FROM numbers(1)) AS bf
SELECT bloomFilterContains(bf, toUInt8(42)), bloomFilterContains(bf, materialize(42.5));

-- LowCardinality(Nullable(String)): value present
WITH (SELECT groupBloomFilterState(100)(materialize(CAST('hello', 'LowCardinality(Nullable(String))'))) FROM numbers(1)) AS bf
SELECT bloomFilterContains(bf, 'hello');

-- Nullable(String): NULL value propagates to NULL
WITH (SELECT groupBloomFilterState(100)(materialize(CAST('hello', 'Nullable(String)'))) FROM numbers(1)) AS bf
SELECT bloomFilterContains(bf, CAST(NULL, 'Nullable(String)'));

-- Nullable(UInt64): all-NULL build input has no inserted values
WITH (SELECT groupBloomFilterState(100)(materialize(CAST(NULL, 'Nullable(UInt64)'))) FROM numbers(1)) AS bf
SELECT bloomFilterContains(bf, toUInt64(42));

-- Nullable(UInt64): non-NULL value is found through nullable aggregate state layout
WITH (SELECT groupBloomFilterState(100)(materialize(CAST(42, 'Nullable(UInt64)'))) FROM numbers(1)) AS bf
SELECT bloomFilterContains(bf, toUInt64(42));

-- Nullable(DateTime64): decimal-backed value is found through nullable aggregate state layout
WITH (SELECT groupBloomFilterState(100)(materialize(CAST(toDateTime64('2023-01-01 12:00:00.123', 3), 'Nullable(DateTime64(3))'))) FROM numbers(1)) AS bf
SELECT bloomFilterContains(bf, toDateTime64('2023-01-01 12:00:00.123', 3));

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
