-- Test deltaSumTimestamp with signed integer types crossing from negative to positive
-- https://github.com/ClickHouse/ClickHouse/issues/104750

-- Int32 values crossing zero: -1, 0 with timestamps 1, 2. Expected delta = 1
SELECT deltaSumTimestamp(toInt32(value), toUInt32(ts))
FROM (SELECT -1 AS value, 1 AS ts UNION ALL SELECT 0 AS value, 2 AS ts);

-- Int64 values crossing zero
SELECT deltaSumTimestamp(toInt64(value), toUInt64(ts))
FROM (SELECT -1 AS value, 1 AS ts UNION ALL SELECT 0 AS value, 2 AS ts);

-- Longer sequence crossing zero: -2, -1, 0, 1, 2. Expected: 4 deltas of +1 = 4
SELECT deltaSumTimestamp(toInt32(value), toUInt32(ts))
FROM (SELECT number::Int32 - 2 AS value, number::UInt32 AS ts FROM numbers(5));

-- Return type should be Int32 for Int32 input
SELECT toTypeName(deltaSumTimestamp(toInt32(1), toUInt32(1)));

-- Return type should be Int64 for Int64 input
SELECT toTypeName(deltaSumTimestamp(toInt64(1), toUInt64(1)));
