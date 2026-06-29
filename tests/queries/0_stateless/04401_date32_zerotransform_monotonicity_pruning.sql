-- Date32 ZeroTransform date/time functions report themselves monotonic to the primary index, so they
-- must be monotonic over the whole Date32 range. Previously several wrapped for out-of-range arguments
-- (toStartOfDay, the relative-number / *NumSinceEpoch transforms, toStartOfISOYear for early-1970 dates,
-- and toDaysSinceYearZero for raw Date32 values before year 0), which made primary-key pruning drop
-- granules that actually contain matching rows. They now saturate.

SET session_timezone = 'UTC';

DROP TABLE IF EXISTS t_d32_mono;
CREATE TABLE t_d32_mono (d Date32) ENGINE = MergeTree ORDER BY d
    SETTINGS index_granularity = 1, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;
INSERT INTO t_d32_mono VALUES ('1900-01-01'),('1955-06-15'),('1969-12-31'),('1970-01-02'),('2000-01-01'),('2106-06-15'),('2150-01-01'),('2299-12-31');

SELECT '-- toStartOfDay';
SELECT count() FROM t_d32_mono WHERE toStartOfDay(d) >= toDateTime('2000-01-01 00:00:00', 'UTC') SETTINGS force_primary_key = 1;
SELECT countIf(toStartOfDay(d) >= toDateTime('2000-01-01 00:00:00', 'UTC')) FROM t_d32_mono;

SELECT '-- toStartOfISOYear';
SELECT count() FROM t_d32_mono WHERE toStartOfISOYear(d) >= toDate32('2000-01-01') SETTINGS force_primary_key = 1;
SELECT countIf(toStartOfISOYear(d) >= toDate32('2000-01-01')) FROM t_d32_mono;

SELECT '-- toRelativeSecondNum';
SELECT count() FROM t_d32_mono WHERE toRelativeSecondNum(d) >= 946684800 SETTINGS force_primary_key = 1;
SELECT countIf(toRelativeSecondNum(d) >= 946684800) FROM t_d32_mono;

SELECT '-- toRelativeMinuteNum';
SELECT count() FROM t_d32_mono WHERE toRelativeMinuteNum(d) >= 15778080 SETTINGS force_primary_key = 1;
SELECT countIf(toRelativeMinuteNum(d) >= 15778080) FROM t_d32_mono;

SELECT '-- toRelativeHourNum';
SELECT count() FROM t_d32_mono WHERE toRelativeHourNum(d) >= 262968 SETTINGS force_primary_key = 1;
SELECT countIf(toRelativeHourNum(d) >= 262968) FROM t_d32_mono;

SELECT '-- toRelativeWeekNum';
SELECT count() FROM t_d32_mono WHERE toRelativeWeekNum(d) >= 1565 SETTINGS force_primary_key = 1;
SELECT countIf(toRelativeWeekNum(d) >= 1565) FROM t_d32_mono;

SELECT '-- toRelativeDayNum';
SELECT count() FROM t_d32_mono WHERE toRelativeDayNum(d) >= 10957 SETTINGS force_primary_key = 1;
SELECT countIf(toRelativeDayNum(d) >= 10957) FROM t_d32_mono;

SELECT '-- toMonthNumSinceEpoch';
SELECT count() FROM t_d32_mono WHERE toMonthNumSinceEpoch(d) >= 360 SETTINGS force_primary_key = 1;
SELECT countIf(toMonthNumSinceEpoch(d) >= 360) FROM t_d32_mono;

SELECT '-- toYearNumSinceEpoch';
SELECT count() FROM t_d32_mono WHERE toYearNumSinceEpoch(d) >= 30 SETTINGS force_primary_key = 1;
SELECT countIf(toYearNumSinceEpoch(d) >= 30) FROM t_d32_mono;

DROP TABLE t_d32_mono;

-- toDaysSinceYearZero is monotonic within the valid Date32 range and only wraps for raw Date32 values
-- before year 0 (reachable via fromDaysSinceYearZero32). Use a key column crossing the year-0 boundary.
DROP TABLE IF EXISTS t_d32_oor;
CREATE TABLE t_d32_oor (d Date32) ENGINE = MergeTree ORDER BY d
    SETTINGS index_granularity = 1, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;
INSERT INTO t_d32_oor SELECT fromDaysSinceYearZero32(x) FROM (SELECT arrayJoin([toUInt32(4294967295), toUInt32(0), toUInt32(719528), toUInt32(840057)]) AS x);

SELECT '-- toDaysSinceYearZero (raw out-of-range Date32 keys)';
SELECT count() FROM t_d32_oor WHERE toDaysSinceYearZero(d) >= 719528 SETTINGS force_primary_key = 1;
SELECT countIf(toDaysSinceYearZero(d) >= 719528) FROM t_d32_oor;

DROP TABLE t_d32_oor;
