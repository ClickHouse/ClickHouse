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

-- Int32
SELECT bloomFilterContains(groupBloomFilterState(1000)(toInt32(number - 50)), toInt32(-10)) AS result
FROM numbers(100);

-- Int64
SELECT bloomFilterContains(groupBloomFilterState(1000)(toInt64(number - 50)), toInt64(-10)) AS result
FROM numbers(100);

-- Float32
SELECT bloomFilterContains(groupBloomFilterState(1000)(toFloat32(number)), toFloat32(42)) AS result
FROM numbers(100);

-- Float64
SELECT bloomFilterContains(groupBloomFilterState(1000)(toFloat64(number * 0.1)), toFloat64(4.2)) AS result
FROM numbers(100);

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

-- bloomFilterContains with non-bloom first argument must throw
SELECT bloomFilterContains(42, toUInt64(1)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
