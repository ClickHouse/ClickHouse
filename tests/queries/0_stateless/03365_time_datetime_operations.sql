SET session_timezone = 'UTC';
SET use_legacy_to_time = false;

-- Standard precision operations (Time + DateTime)
SELECT toDateTime('2020-01-01 12:10:10') + toTime('00:02:02') AS dt_plus_t;
SELECT toTime('00:02:02') + toDateTime('2020-01-01 12:10:10') AS t_plus_dt; -- Commutative operation should work
SELECT toDateTime('2020-01-01 12:10:10') - toTime('00:02:02') AS dt_minus_t;

-- High precision operations (Time64 + DateTime64)
SELECT toDateTime64('2020-01-01 12:10:10.123', 3) + toTime64('00:02:02.456', 3) AS dt64_plus_t64;
SELECT toTime64('00:02:02.456', 3) + toDateTime64('2020-01-01 12:10:10.123', 3) AS t64_plus_dt64; -- Commutative operation should work
SELECT toDateTime64('2020-01-01 12:10:10.123', 3) - toTime64('00:02:02.456', 3) AS dt64_minus_t64;

-- Mixed precision operations (Time64 + DateTime)
SELECT toDateTime('2020-01-01 12:10:10') + toTime64('00:02:02.456', 3) AS dt_plus_t64;
SELECT toTime64('00:02:02.456', 3) + toDateTime('2020-01-01 12:10:10') AS t64_plus_dt;
SELECT toDateTime('2020-01-01 12:10:10') - toTime64('00:02:02.456', 3) AS dt_minus_t64;

-- Mixed precision operations (Time + DateTime64)
SELECT toDateTime64('2020-01-01 12:10:10.123', 3) + toTime('00:02:02') AS dt64_plus_t;
SELECT toTime('00:02:02') + toDateTime64('2020-01-01 12:10:10.123', 3) AS t_plus_dt64;
SELECT toDateTime64('2020-01-01 12:10:10.123', 3) - toTime('00:02:02') AS dt64_minus_t;

-- Edge cases: Different scales for DateTime64 and Time64
SELECT toDateTime64('2020-01-01 12:10:10.123456', 6) + toTime64('00:02:02.456', 3) AS dt64_6_plus_t64_3;
SELECT toDateTime64('2020-01-01 12:10:10.123', 3) + toTime64('00:02:02.456789', 6) AS dt64_3_plus_t64_6;
SELECT toDateTime64('2020-01-01 12:10:10.123456', 6) - toTime64('00:02:02.456', 3) AS dt64_6_minus_t64_3;

-- Different representations of the same values
SELECT toDateTime64('2020-01-01 12:10:10.123', 3) + toTime64('00:02:02.456', 3) AS dt64_plus_t64;
SELECT toDateTime64('2020-01-01 12:10:10.123000', 6) + toTime64('00:02:02.456000', 6) AS dt64_plus_t64_scale6;
SELECT toDateTime('2020-01-01 12:10:10') + toTime('00:02:02') AS dt_plus_t;
SELECT toDateTime64('2020-01-01 12:10:10.000', 3) + toTime64('00:02:02.000', 3) AS dt64_plus_t64_zeros;

-- Test with column values
SELECT
    arrayJoin(['2020-01-01 12:10:10', '2020-01-02 12:10:10']) AS dt,
    arrayJoin(['00:02:02', '00:03:03']) AS t,
    toDateTime(dt) + toTime(t) AS result
FORMAT CSV;

SELECT
    arrayJoin(['2020-01-01 12:10:10.123', '2020-01-02 12:10:10.456']) AS dt64,
    arrayJoin(['00:02:02.456', '00:03:03.789']) AS t64,
    toDateTime64(dt64, 3) + toTime64(t64, 3) AS result
FORMAT CSV;

-- Test with mixed column and constant
SELECT
    arrayJoin(['2020-01-01 12:10:10', '2020-01-02 12:10:10']) AS dt,
    toDateTime(dt) + toTime('00:02:02') AS result
FORMAT CSV;

SELECT
    arrayJoin(['00:02:02', '00:03:03']) AS t,
    toDateTime('2020-01-01 12:10:10') + toTime(t) AS result
FORMAT CSV;

-- Edge case: Crossing day boundary
SELECT toDateTime('2020-01-01 23:59:59') + toTime('00:00:02') AS cross_day;

-- Edge case: Leap day
SELECT toDateTime('2020-02-29 23:59:59') + toTime('00:00:02') AS leap_day;

-- Extreme time values
SELECT toDateTime('2020-01-01 00:00:00') + toTime('23:59:59') AS max_time;
SELECT toDateTime('2020-01-01 23:59:59') - toTime('23:59:59') AS min_time;

-- Null handling
SELECT toDateTime(NULL) + toTime('00:02:02');
SELECT toDateTime('2020-01-01 12:10:10') + toTime(NULL);
SELECT toDateTime64(NULL, 3) + toTime64('00:02:02.456', 3);

-- Test with time zone handling
SELECT toDateTime('2020-01-01 12:10:10', 'UTC') + toTime('00:02:02') AS dt_tz_plus_t;

SELECT toTime('00:02:02') - toDateTime('2020-01-01 12:10:10') AS t_minus_dt; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- The explicit timezone of the DateTime argument is preserved
SELECT toTypeName(toDateTime('2020-01-01 12:10:10', 'Europe/Moscow') + toTime('00:02:02'));
SELECT toTypeName(toDateTime64('2020-01-01 12:10:10.123', 3, 'Europe/Moscow') + toTime('00:02:02'));

-- Overflow with throw: out-of-range results must not wrap
SET date_time_overflow_behavior = 'throw';
SELECT toDateTime('2106-02-07 06:28:15') + toTime('00:00:01'); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT toDateTime('1970-01-01 00:00:00') - toTime('00:00:01'); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
-- One tick past the last representable DateTime64(9) value
SELECT toDateTime64('2262-04-11 23:47:16.854775807', 9) + toTime64('00:00:00.000000001', 9); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }

-- Vector path: one row valid, one row overflows
DROP TABLE IF EXISTS test_dt_time_overflow_vec;
CREATE TABLE test_dt_time_overflow_vec (dt DateTime, t Time) ENGINE = Memory;
INSERT INTO test_dt_time_overflow_vec VALUES ('2020-01-01 00:00:00', '00:00:01'), ('2106-02-07 06:28:15', '00:00:01');
SELECT dt + t FROM test_dt_time_overflow_vec ORDER BY dt; -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
DROP TABLE test_dt_time_overflow_vec;

-- Large negative Time64 brings an intermediate overflow back into range
SELECT toDateTime64('2299-12-31 23:59:59', 0) + CAST(toDecimal128('-1200000000.000000000', 9), 'Time64(9)');

-- Overflow with saturate
SET date_time_overflow_behavior = 'saturate';
SELECT toDateTime('2106-02-07 06:28:15') + toTime('00:00:01');
SELECT toDateTime('1970-01-01 00:00:00') - toTime('00:00:01');
