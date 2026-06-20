-- Previously toStartOfDay and the relative-number / *NumSinceEpoch transforms wrapped for out-of-range Date32
-- arguments (pre-1970-01-01 or post-2149-06-06), which made primary-key pruning drop granules that
-- actually contain matching rows. They now saturate instead, making them monotonic and allowing pruning to work correctly.

SET session_timezone = 'UTC';

DROP TABLE IF EXISTS t_d32_mono;
CREATE TABLE t_d32_mono (d Date32) ENGINE = MergeTree ORDER BY d
    SETTINGS index_granularity = 1, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;
INSERT INTO t_d32_mono VALUES ('1900-01-01'),('1955-06-15'),('1969-12-31'),('1970-01-02'),('2000-01-01'),('2106-06-15'),('2150-01-01'),('2299-12-31');

SELECT '-- toStartOfDay';
SELECT count() FROM t_d32_mono WHERE toStartOfDay(d) >= toDateTime('2000-01-01 00:00:00', 'UTC') SETTINGS force_primary_key = 1;
SELECT countIf(toStartOfDay(d) >= toDateTime('2000-01-01 00:00:00', 'UTC')) FROM t_d32_mono;

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
