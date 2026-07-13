-- { echo }

SET enable_time_time64_type = 1;
SET use_legacy_to_time = 0;
SET session_timezone = 'UTC';
SET date_time_overflow_behavior = 'throw';

-- Date + Time -> DateTime
SELECT toDate('2024-01-15') + toTime('01:02:03') AS dt, toTypeName(dt);

-- Date + Time64 -> DateTime64
SELECT toDate('2024-01-15') + toTime64('01:02:03.456', 3) AS dt, toTypeName(dt);

-- Date32 + Time -> DateTime64(0)
SELECT toDate32('2024-01-15') + toTime('01:02:03') AS dt, toTypeName(dt);

-- Date32 + Time64 -> DateTime64(6)
SELECT toDate32('2024-01-15') + toTime64('01:02:03.456789', 6) AS dt, toTypeName(dt);

-- Commutativity
SELECT toTime('01:02:03') + toDate('2024-01-15') AS dt, toTypeName(dt);
SELECT toTime64('01:02:03.456', 3) + toDate('2024-01-15') AS dt, toTypeName(dt);
SELECT toTime('01:02:03') + toDate32('2024-01-15') AS dt, toTypeName(dt);
SELECT toTime64('01:02:03.456789', 6) + toDate32('2024-01-15') AS dt, toTypeName(dt);

-- plus() functional syntax
SELECT plus(toDate('2024-01-15'), toTime('01:02:03')) AS dt, toTypeName(dt);
SELECT plus(toDate32('2024-01-15'), toTime64('01:02:03.456', 3)) AS dt, toTypeName(dt);

-- Time64(0) is intentionally different from Time (returns DateTime64(0) not DateTime)
SELECT toDate('2024-01-15') + toTime64('01:02:03', 0) AS dt, toTypeName(dt);
-- Date max works with Time64(0) because DateTime64(0) has larger range than DateTime
SELECT toDate('2149-06-06') + toTime64('23:59:59', 0) AS dt, toTypeName(dt);
-- But Date max with Time overflows because result type would be DateTime which has smaller range
SELECT toDate('2149-06-06') + toTime('23:59:59') AS dt, toTypeName(dt); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }

-- Table-based tests
DROP TABLE IF EXISTS test_date_time64_0;
CREATE TABLE test_date_time64_0 (d Date, t Time64(0)) ENGINE = Memory;
INSERT INTO test_date_time64_0 VALUES
    ('2024-01-15', '01:02:03'),
    ('2149-06-06', '23:59:59');
SELECT d + t AS dt, toTypeName(dt) FROM test_date_time64_0 ORDER BY d;
SELECT t + d AS dt, toTypeName(dt) FROM test_date_time64_0 ORDER BY d;
DROP TABLE test_date_time64_0;

DROP TABLE IF EXISTS test_date_time;
CREATE TABLE test_date_time (d Date, t Time) ENGINE = Memory;
INSERT INTO test_date_time VALUES ('2024-01-15', '01:02:03'), ('2024-06-20', '23:59:59'), ('1970-01-01', '00:00:00');
SELECT d + t AS dt, toTypeName(dt) FROM test_date_time ORDER BY d;
DROP TABLE test_date_time;

DROP TABLE IF EXISTS test_date_time64;
CREATE TABLE test_date_time64 (d Date, t Time64(3)) ENGINE = Memory;
INSERT INTO test_date_time64 VALUES ('2024-01-15', '01:02:03.456'), ('2024-06-20', '23:59:59.999'), ('1970-01-01', '00:00:00.000');
SELECT d + t AS dt, toTypeName(dt) FROM test_date_time64 ORDER BY d;
DROP TABLE test_date_time64;

DROP TABLE IF EXISTS test_date32_time;
CREATE TABLE test_date32_time (d Date32, t Time) ENGINE = Memory;
INSERT INTO test_date32_time VALUES ('2024-01-15', '01:02:03'), ('2200-06-15', '12:00:00'), ('1900-01-01', '23:59:59');
SELECT d + t AS dt, toTypeName(dt) FROM test_date32_time ORDER BY d;
DROP TABLE test_date32_time;

DROP TABLE IF EXISTS test_date32_time64;
CREATE TABLE test_date32_time64 (d Date32, t Time64(3)) ENGINE = Memory;
INSERT INTO test_date32_time64 VALUES ('2024-01-15', '01:02:03.456'), ('2200-06-15', '12:00:00.000'), ('1900-01-01', '23:59:59.999');
SELECT d + t AS dt, toTypeName(dt) FROM test_date32_time64 ORDER BY d;
DROP TABLE test_date32_time64;

-- Const/non-const column combinations

-- Date + Time: const+const, const+vec, vec+const, vec+vec
SELECT toDate('2024-01-15') + toTime('01:02:03');
SELECT toDate('2024-01-15') + materialize(toTime('01:02:03'));
SELECT materialize(toDate('2024-01-15')) + toTime('01:02:03');
SELECT materialize(toDate('2024-01-15')) + materialize(toTime('01:02:03'));

-- Date + Time: table date column + const time
DROP TABLE IF EXISTS test_cv;
CREATE TABLE test_cv (d Date) ENGINE = Memory;
INSERT INTO test_cv VALUES ('2024-01-15'), ('2024-06-20'), ('1970-01-01');
SELECT d + toTime('01:02:03') FROM test_cv ORDER BY d;
-- Date + Time: const date + table time column
DROP TABLE IF EXISTS test_cv2;
CREATE TABLE test_cv2 (t Time) ENGINE = Memory;
INSERT INTO test_cv2 VALUES ('01:02:03'), ('23:59:59'), ('00:00:00');
SELECT toDate('2024-01-15') + t FROM test_cv2 ORDER BY t;
DROP TABLE test_cv;
DROP TABLE test_cv2;

-- Date + Time64: table date column + const time64, const date + table time64 column
DROP TABLE IF EXISTS test_cv3;
CREATE TABLE test_cv3 (d Date, t Time64(3)) ENGINE = Memory;
INSERT INTO test_cv3 VALUES ('2024-01-15', '01:02:03.100'), ('2024-06-20', '23:59:59.999');
SELECT d + toTime64('12:00:00.000', 3) FROM test_cv3 ORDER BY d;
SELECT toDate('2024-01-15') + t FROM test_cv3 ORDER BY t;
SELECT d + t FROM test_cv3 ORDER BY d;
DROP TABLE test_cv3;

-- Date32 + Time: table date32 column + const time, const date32 + table time column
DROP TABLE IF EXISTS test_cv4;
CREATE TABLE test_cv4 (d Date32, t Time) ENGINE = Memory;
INSERT INTO test_cv4 VALUES ('2024-01-15', '01:02:03'), ('2200-06-15', '12:00:00');
SELECT d + toTime('06:00:00') FROM test_cv4 ORDER BY d;
SELECT toDate32('2024-01-15') + t FROM test_cv4 ORDER BY t;
SELECT d + t FROM test_cv4 ORDER BY d;
DROP TABLE test_cv4;

-- Date32 + Time64: table date32 column + const time64, const date32 + table time64 column
DROP TABLE IF EXISTS test_cv5;
CREATE TABLE test_cv5 (d Date32, t Time64(6)) ENGINE = Memory;
INSERT INTO test_cv5 VALUES ('2024-01-15', '01:02:03.123456'), ('1900-01-01', '23:59:59.999999');
SELECT d + toTime64('06:00:00.000000', 6) FROM test_cv5 ORDER BY d;
SELECT toDate32('2024-01-15') + t FROM test_cv5 ORDER BY t;
SELECT d + t FROM test_cv5 ORDER BY d;
DROP TABLE test_cv5;

-- Edge cases: Date + Time -> DateTime

-- Epoch boundaries
SELECT toDate('1970-01-01') + toTime('00:00:00');
SELECT toDate('1970-01-01') + toTime('23:59:59');
-- End of day
SELECT toDate('2024-01-15') + toTime('23:59:59');
-- Negative time -> wraps to previous day
SELECT toDate('2024-01-15') + toTime(-1);
-- Time exceeding 24h -> wraps into next day
SELECT toDate('2024-01-15') + toTime(90000);
-- Near DateTime max boundary
SELECT toDate('2106-02-07') + toTime('00:00:00');
SELECT toDate('2106-02-07') + toTime('06:28:15');

-- Edge cases: Date + Time64 -> DateTime64

-- Epoch boundaries
SELECT toDate('1970-01-01') + toTime64('00:00:00.000', 3);
SELECT toDate('1970-01-01') + toTime64('23:59:59.999', 3);
-- End of day with max scale
SELECT toDate('2024-01-15') + toTime64('23:59:59.999999999', 9) AS dt, toTypeName(dt);
-- Negative time
SELECT toDate('2024-01-15') + toTime64(-1, 3);
-- Negative time from epoch (DateTime64 supports pre-epoch)
SELECT toDate('1970-01-01') + toTime64(-1, 3);
-- Time exceeding 24h
SELECT toDate('2024-01-15') + toTime64(90000, 3);
-- Near boundary dates
SELECT toDate('2106-02-07') + toTime64('06:28:15.000', 3);
-- Date max is in range for DateTime64(3)
SELECT toDate('2149-06-06') + toTime64('23:59:59.999', 3);

-- Edge cases: Date32 + Time -> DateTime64(0)

-- Epoch boundaries
SELECT toDate32('1970-01-01') + toTime('00:00:00');
SELECT toDate32('1970-01-01') + toTime('23:59:59');
-- Pre-epoch
SELECT toDate32('1900-01-01') + toTime('00:00:00');
SELECT toDate32('1900-01-01') + toTime('23:59:59');
-- Negative time
SELECT toDate32('2024-01-15') + toTime(-1);
-- Time exceeding 24h
SELECT toDate32('2024-01-15') + toTime(90000);
-- Beyond DateTime range (DateTime64 handles it)
SELECT toDate32('2200-06-15') + toTime('12:00:00');
SELECT toDate32('2200-06-15') + toTime('23:59:59');
-- Date32 max supported range
SELECT toDate32('2299-12-31') + toTime('23:59:59');

-- Edge cases: Date32 + Time64 -> DateTime64(s)

-- Epoch boundaries
SELECT toDate32('1970-01-01') + toTime64('00:00:00.000', 3);
SELECT toDate32('1970-01-01') + toTime64('23:59:59.999', 3);
-- Pre-epoch
SELECT toDate32('1900-01-01') + toTime64('00:00:00.000', 3);
SELECT toDate32('1900-01-01') + toTime64('23:59:59.999', 3);
-- Negative time
SELECT toDate32('2024-01-15') + toTime64(-1, 3);
-- Time exceeding 24h
SELECT toDate32('2024-01-15') + toTime64(90000, 3);
-- Beyond DateTime range
SELECT toDate32('2200-06-15') + toTime64('12:30:45.678', 3);

-- Normalization and multi-day rollover (scalar)

SELECT toDate('2024-01-15') + toTime('25:70:70');
SELECT toDate32('2024-01-15') + toTime64('25:70:70.123456', 6);
-- Maximum visible Time range
SELECT toDate('2024-01-15') + toTime('999:59:59');
-- Sub-second negative and overflow for Time64
SELECT toDate('2024-01-15') + toTime64('-00:00:00.001', 3);
SELECT toDate('2024-01-15') + toTime64('24:00:00.001', 3);

-- Date + Time with negative / >24h / normalized values
DROP TABLE IF EXISTS test_roll_time;
CREATE TABLE test_roll_time (d Date, t Time) ENGINE = Memory;
INSERT INTO test_roll_time VALUES
    ('2024-01-15', -1),
    ('2024-01-15', 90000),
    ('2024-01-15', '25:70:70');
SELECT d + t AS dt, toTypeName(dt) FROM test_roll_time ORDER BY t;
DROP TABLE test_roll_time;

-- Date32 + Time64 with negative / >24h / normalized values
DROP TABLE IF EXISTS test_roll_time64;
CREATE TABLE test_roll_time64 (d Date32, t Time64(6)) ENGINE = Memory;
INSERT INTO test_roll_time64 VALUES
    ('2024-01-15', '-00:00:00.001000'),
    ('2024-01-15', '24:00:00.001000'),
    ('2024-01-15', '25:70:70.123456');
SELECT d + t AS dt, toTypeName(dt) FROM test_roll_time64 ORDER BY t;
DROP TABLE test_roll_time64;

-- Scale 8 fits the full DateTime64 range (1900-2299), but scale 9 is limited to ~2262 by Int64 capacity
-- Precision 8 still reaches full Date32/DateTime64 range
SELECT toDate32('2299-12-31') + toTime64('23:59:59.99999999', 8) AS dt, toTypeName(dt);
-- Precision 9 is still fully safe for all Date values, because Date tops out before 2262
SELECT toDate('2149-06-06') + toTime64('23:59:59.999999999', 9) AS dt, toTypeName(dt);

-- For in-range values, Date + Time matches Date + INTERVAL
-- (overflow behavior may differ: Date+Time respects date_time_overflow_behavior, INTERVAL does not)
SELECT (toDate('2024-01-15') + toTime(3723)) = (toDate('2024-01-15') + INTERVAL 3723 SECOND);
-- 24h rollover: Date + Time(86400) vs Date + INTERVAL 86400 SECOND
SELECT (toDate('2024-01-15') + toTime(86400)) = (toDate('2024-01-15') + INTERVAL 86400 SECOND);
-- Negative time: Date + Time(-1) vs Date + INTERVAL -1 SECOND
SELECT (toDate('2024-01-15') + toTime(-1)) = (toDate('2024-01-15') + INTERVAL -1 SECOND);
-- Date32
SELECT (toDate32('2024-01-15') + toTime(3723)) = (toDate32('2024-01-15') + INTERVAL 3723 SECOND);

-- Timezone handling

SET session_timezone = 'UTC';
SELECT toDate('2024-01-15') + toTime('01:02:03') AS dtest_utc;
SELECT toUnixTimestamp(toDate('2024-01-15') + toTime(0)) AS ts_utc;
-- Verify underlying millisecond timestamp is correct under UTC
SELECT toUnixTimestamp64Milli(toDate('2024-01-15') + toTime64('00:00:00.123', 3)) AS ts64_utc;

-- Side-by-side with INTERVAL under UTC
SELECT (toDate('2024-01-15') + toTime(3723)) = (toDate('2024-01-15') + INTERVAL 3723 SECOND) AS same_utc;

SET session_timezone = 'America/New_York';
SELECT toDate('2024-01-15') + toTime('01:02:03') AS dtest_ny;
SELECT toUnixTimestamp(toDate('2024-01-15') + toTime(0)) AS ts_ny;
-- Verify underlying millisecond timestamp differs under New York timezone
SELECT toUnixTimestamp64Milli(toDate('2024-01-15') + toTime64('00:00:00.123', 3)) AS ts64_ny;

-- Side-by-side with INTERVAL under New York timezone
SELECT (toDate('2024-01-15') + toTime(3723)) = (toDate('2024-01-15') + INTERVAL 3723 SECOND) AS same_ny;

SET session_timezone = 'Asia/Kolkata';
SELECT toDate('2024-01-15') + toTime('01:02:03') AS dtest_kol;
SELECT toUnixTimestamp(toDate('2024-01-15') + toTime(0)) AS ts_kol;

-- Side-by-side with INTERVAL under Kolkata timezone
SELECT (toDate('2024-01-15') + toTime(3723)) = (toDate('2024-01-15') + INTERVAL 3723 SECOND) AS same_kol;

-- Verify timestamps differ between timezones (proving tz affects computation)
SET session_timezone = 'UTC';
SELECT
    toUnixTimestamp(toDateTime('2024-01-15 00:00:00', 'UTC')) AS ts_utc,
    toUnixTimestamp(toDateTime('2024-01-15 00:00:00', 'America/New_York')) AS ts_ny,
    ts_utc != ts_ny AS differ;

-- DST: Date+Time adds raw seconds to midnight, which can differ from parsing a
-- local timestamp string when DST transitions create gaps or overlaps.

SET session_timezone = 'Europe/London';

-- 2023-10-29: London clocks fall back from 02:00 to 01:00 (01:30 occurs twice).
-- Date+Time gives midnight+5400s. toDateTime parses '01:30' picking the earlier occurrence.
-- Both resolve to the same instant, so the result is equal.
SELECT (toDate('2023-10-29') + toTime('01:30:00')) = toDateTime('2023-10-29 01:30:00', 'Europe/London');

-- 2023-03-26: London clocks spring forward from 01:00 to 02:00 (01:30 does not exist).
-- Date+Time gives midnight+5400s = 02:30 BST (the gap is skipped arithmetically).
-- toDateTime parses nonexistent '01:30' as 00:30 GMT (shifts back before the gap).
-- These are different instants, so the result is not equal.
SELECT (toDate('2023-03-26') + toTime('01:30:00')) = toDateTime('2023-03-26 01:30:00', 'Europe/London');
SELECT (toDate('2023-03-26') + toTime64('01:30:00.123', 3)) = toDateTime64('2023-03-26 01:30:00.123', 3, 'Europe/London');

-- Show the actual values: 02:30 vs 00:30, different unix timestamps
SELECT
    toDate('2023-03-26') + toTime('01:30:00') AS combined,
    toDateTime('2023-03-26 01:30:00', 'Europe/London') AS parsed;
SELECT
    toUnixTimestamp(toDate('2023-03-26') + toTime('01:30:00')) AS combined_ts,
    toUnixTimestamp(toDateTime('2023-03-26 01:30:00', 'Europe/London')) AS parsed_ts;

-- Date+Time matches midnight+INTERVAL (both just add raw seconds to midnight)
SELECT
    (toDate('2023-03-26') + toTime('01:30:00')) =
    (toDateTime('2023-03-26 00:00:00', 'Europe/London') + INTERVAL 5400 SECOND);

SET session_timezone = 'UTC';

-- Common-type test: Date+Time and Date+Time64(0) should unify to DateTime64(0)
SELECT DISTINCT toTypeName(dt) FROM
(
    SELECT toDate('2024-01-15') + toTime('01:02:03') AS dt
    UNION ALL
    SELECT toDate('2024-01-15') + toTime64('01:02:03', 0) AS dt
);

-- Error cases

SELECT toDate('2024-01-15') - toTime('01:02:03'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT toTime('01:02:03') - toDate('2024-01-15'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT toDateTime('2024-01-15 00:00:00') + toTime('01:02:03'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT toDate('2024-01-15') - toTime64('01:02:03.456', 3); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT toDateTime('2024-01-15 00:00:00') + toTime64('01:02:03.456', 3); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT toDateTime64('2024-01-15 00:00:00.000', 3) + toTime('01:02:03'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT toDateTime64('2024-01-15 00:00:00.000', 3) + toTime64('01:02:03.456', 3); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- Overflow with throw (already the default from top of file)

-- Date + Time -> DateTime: underflow below epoch
SELECT toDate('1970-01-01') + toTime(-1); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
-- Date + Time -> DateTime: max valid DateTime
SELECT toDate('2106-02-07') + toTime('06:28:15');
-- Date + Time -> DateTime: one second past max
SELECT toDate('2106-02-07') + toTime('06:28:16'); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
-- Date + Time -> DateTime: Date max far exceeds DateTime range
SELECT toDate('2149-06-06') + toTime('00:00:00'); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }

-- Date32 + Time -> DateTime64(0): underflow below 1900
SELECT toDate32('1900-01-01') + toTime(-1); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
-- Date32 + Time64 -> DateTime64: underflow below 1900
SELECT toDate32('1900-01-01') + toTime64('-00:00:00.000001', 6); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }

-- DateTime64(9) specific boundary: lower limit (1900-01-01 midnight)
SELECT toDate32('1900-01-01') + toTime64('00:00:00.000000000', 9) AS dt, toTypeName(dt);
-- DateTime64(9): a value on the last valid day (exact upper limit is 23:47:16.854775807 below)
SELECT toDate32('2262-04-11') + toTime64('23:47:16.000000000', 9) AS dt, toTypeName(dt);
-- DateTime64(9) specific boundary: past Int64 capacity at scale 9
SELECT toDate32('2262-04-12') + toTime64('00:00:00.000000000', 9); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }

-- One tick past the last representable DateTime64(9) value
SELECT toDate32('2262-04-11') + toTime64('23:47:16.854775808', 9); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
-- Last representable tick at scale 9
SELECT toDate32('2262-04-11') + toTime64('23:47:16.854775807', 9) AS dt, toTypeName(dt);

-- Vector path: one row valid, one row past DateTime64(9) limit
DROP TABLE IF EXISTS test_overflow_int64;
CREATE TABLE test_overflow_int64 (d Date32, t Time64(9)) ENGINE = Memory;
INSERT INTO test_overflow_int64 VALUES
    ('2262-04-11', '23:47:16.854775807'),
    ('2262-04-11', '23:47:16.854775808');
SELECT d + t FROM test_overflow_int64 ORDER BY t; -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
DROP TABLE test_overflow_int64;

-- Vector overflow regression: throw must work on non-const columns too
DROP TABLE IF EXISTS test_overflow_vec;
CREATE TABLE test_overflow_vec (d Date, t Time) ENGINE = Memory;
INSERT INTO test_overflow_vec VALUES
    ('2106-02-07', '06:28:15'),
    ('2149-06-06', '00:00:00');
SELECT d + t FROM test_overflow_vec ORDER BY d; -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
DROP TABLE test_overflow_vec;

-- Vector path: Date32 + Time64(9) with one row past the DateTime64(9) limit
DROP TABLE IF EXISTS test_overflow_vec64;
CREATE TABLE test_overflow_vec64 (d Date32, t Time64(9)) ENGINE = Memory;
INSERT INTO test_overflow_vec64 VALUES
    ('2262-04-11', '23:47:16.000000000'),
    ('2262-04-12', '00:00:00.000000000');
SELECT d + t FROM test_overflow_vec64 ORDER BY d; -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
DROP TABLE test_overflow_vec64;

-- Large negative Time64 can bring an intermediate overflow back into range.
-- midnight(2299-12-31) * 10^9 overflows Int64, but subtracting 1.2B seconds lands in range.
SELECT toDate32('2299-12-31') + CAST(toDecimal128('-1200000000.000000000', 9), 'Time64(9)');

-- Vector path: one row has large negative time (in range), other row overflows.
DROP TABLE IF EXISTS test_intermediate_overflow;
CREATE TABLE test_intermediate_overflow (t Time64(9)) ENGINE = Memory;
INSERT INTO test_intermediate_overflow SELECT arrayJoin([CAST(toDecimal128('-1200000000.000000000', 9), 'Time64(9)'), toTime64('00:00:00.000000000', 9)]);
SELECT toDate32('2299-12-31') + t FROM test_intermediate_overflow ORDER BY t; -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
DROP TABLE test_intermediate_overflow;

-- Overflow with saturate

SET date_time_overflow_behavior = 'saturate';

-- Date + Time: underflow saturates to epoch
SELECT toDate('1970-01-01') + toTime(-1);
-- Date + Time: overflow saturates to DateTime max
SELECT toDate('2149-06-06') + toTime('23:59:59');
-- Date32 + Time64: underflow saturates to DateTime64 min
SELECT toDate32('1900-01-01') + toTime64('-00:00:00.000001', 6);
-- DateTime64(9) overflow saturates to the last representable value
SELECT toDate32('2262-04-11') + toTime64('23:47:16.854775808', 9);

-- Large negative time brings intermediate overflow back into range (should NOT saturate)
SELECT toDate32('2299-12-31') + CAST(toDecimal128('-1200000000.000000000', 9), 'Time64(9)');

-- Vector: first row is in range despite intermediate overflow, second row saturates
DROP TABLE IF EXISTS test_saturate_intermediate;
CREATE TABLE test_saturate_intermediate (t Time64(9)) ENGINE = Memory;
INSERT INTO test_saturate_intermediate SELECT arrayJoin([CAST(toDecimal128('-1200000000.000000000', 9), 'Time64(9)'), toTime64('00:00:00.000000000', 9)]);
SELECT toDate32('2299-12-31') + t FROM test_saturate_intermediate ORDER BY t;
DROP TABLE test_saturate_intermediate;

SET date_time_overflow_behavior = 'throw';

-- Time values beyond the visible range display as saturated (999:59:59 or -999:59:59
-- depending on sign) but internally store their full numeric value. Date+Time uses
-- the internal value, so two Time values that print identically can produce different
-- DateTime results.
SELECT
    toTime(9999999) AS t_raw,
    toTime(3599999) AS t_vis,
    t_raw = t_vis AS same_time;
SELECT
    toDate('2024-01-15') + toTime(9999999) AS dt_raw,
    toDate('2024-01-15') + toTime(3599999) AS dt_vis,
    dt_raw = dt_vis AS same_dt;
SELECT
    (toDate('2024-01-15') + toTime(9999999)) =
    (toDate('2024-01-15') + toTime(3599999)) AS same_dt_from_same_visible_time;
