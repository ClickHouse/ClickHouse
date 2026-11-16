SET session_timezone = 'UTC';

-- A 128- or 256-bit integer source converts to `DateTime`, `DateTime64` and `Time64` with the same
-- overflow handling as a 64-bit one: it saturates instead of wrapping, or being stored raw.

SELECT toDateTime(toUInt128(4294967301)), toDateTime(toUInt64(4294967301));
SELECT toDateTime(toUInt256(4294967301)), toDateTime(toInt128(-1)), toDateTime(toInt64(-1)), toDateTime(toInt256(-1));

SELECT toInt64(toDateTime64(toUInt128(99999999999999), 0)), toInt64(toDateTime64(toUInt64(99999999999999), 0));
SELECT toInt64(toDateTime64(toInt128(-99999999999999), 0)), toInt64(toDateTime64(toInt64(-99999999999999), 0));
SELECT toInt64(toTime64(toUInt128(99999999999999), 0)), toInt64(toTime64(toUInt64(99999999999999), 0));
SELECT toInt64(toTime64(toInt128(-99999999999999), 0)), toInt64(toTime64(toInt64(-99999999999999), 0));

-- An unsigned width narrower than 64 bits can also exceed the `Time64` maximum.
SELECT toInt64(toTime64(toUInt32(4000000), 0)), toInt64(toTime64(toUInt64(4000000), 0));
SELECT toInt64(toDateTime64(toUInt32(4000000), 0)), toInt64(toDateTime64(toUInt64(4000000), 0));

-- In-range values are unaffected.
SELECT toDateTime(toUInt128(1000000)), toDateTime64(toInt256(1000000), 3), toTime64(toUInt128(3600), 0);

-- The behaviour agrees with the 64-bit source in every overflow mode, including `throw`, which these
-- numeric conversions do not consult (a separate, pre-existing gap).
SELECT toDateTime(toUInt128(4294967301)), toDateTime(toUInt64(4294967301)) SETTINGS date_time_overflow_behavior = 'throw';
SELECT toDateTime64(toUInt128(99999999999999), 0), toDateTime64(toUInt64(99999999999999), 0) SETTINGS date_time_overflow_behavior = 'throw';
SELECT toTime64(toUInt128(99999999999999), 0), toTime64(toUInt64(99999999999999), 0) SETTINGS date_time_overflow_behavior = 'throw';

-- The conversion is used for primary-key analysis, and a wrapped part boundary discarded the part.

DROP TABLE IF EXISTS t_big_int_datetime;

CREATE TABLE t_big_int_datetime (n UInt128) ENGINE = MergeTree ORDER BY n
SETTINGS index_granularity = 1, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;

INSERT INTO t_big_int_datetime VALUES (5), (1000000), (4294967297);

SELECT countIf(toDateTime(n) = toDateTime(toUInt64(1000000))), count() FROM t_big_int_datetime WHERE toDateTime(n) = toDateTime(toUInt64(1000000));
SELECT count() FROM t_big_int_datetime WHERE n = 1000000;

DROP TABLE t_big_int_datetime;
