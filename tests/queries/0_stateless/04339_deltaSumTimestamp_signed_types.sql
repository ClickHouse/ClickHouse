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

-- Int8 values crossing zero: -2, -1, 0, 1, 2. Expected: 4 deltas of +1 = 4
SELECT deltaSumTimestamp(toInt8(toInt64(number) - 2), toUInt32(number)) FROM numbers(5);

-- Int16 values crossing zero: -2, -1, 0, 1, 2. Expected: 4 deltas of +1 = 4
SELECT deltaSumTimestamp(toInt16(toInt64(number) - 2), toUInt32(number)) FROM numbers(5);

-- Return type should match the signed value type
SELECT toTypeName(deltaSumTimestamp(toInt32(1), toUInt32(1)));
SELECT toTypeName(deltaSumTimestamp(toInt64(1), toUInt64(1)));
SELECT toTypeName(deltaSumTimestamp(toInt8(1), toUInt32(1)));
SELECT toTypeName(deltaSumTimestamp(toInt16(1), toUInt32(1)));

-- Signed timestamps crossing zero: merging two states must order them by signed timestamp.
-- State A (ts = -2, -1; values 10, 20) precedes state B (ts = 1, 2; values 30, 40).
-- Correct signed ordering accumulates the gap 20 -> 30 across the states, giving 30;
-- treating the timestamps as unsigned misorders the states and gives 20.
SELECT deltaSumTimestampMerge(state) FROM
(
    SELECT deltaSumTimestampState(toInt32(value), toInt32(ts)) AS state
    FROM (SELECT number::Int32 - 2 AS ts, (number + 1) * 10 AS value FROM numbers(2))
    UNION ALL
    SELECT deltaSumTimestampState(toInt32(value), toInt32(ts)) AS state
    FROM (SELECT number::Int32 + 1 AS ts, (number + 3) * 10 AS value FROM numbers(2))
);

-- Same with Int64 signed timestamps.
SELECT deltaSumTimestampMerge(state) FROM
(
    SELECT deltaSumTimestampState(toInt32(value), toInt64(ts)) AS state
    FROM (SELECT number::Int64 - 2 AS ts, (number + 1) * 10 AS value FROM numbers(2))
    UNION ALL
    SELECT deltaSumTimestampState(toInt32(value), toInt64(ts)) AS state
    FROM (SELECT number::Int64 + 1 AS ts, (number + 3) * 10 AS value FROM numbers(2))
);
