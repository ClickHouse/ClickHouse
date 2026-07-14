-- DateTime64 ZeroTransform date/time functions report themselves monotonic to the primary index, so they
-- must be monotonic over the whole DateTime64 range. Several previously wrapped for out-of-range arguments
-- (toStartOfDay and the relative-number / *NumSinceEpoch transforms narrowed the result to UInt32/UInt16
-- without saturating), which made primary-key pruning drop granules that actually contain matching rows
-- (and tripped the exact_ranges assertion in the trivial-count projection optimization). They now saturate.

SET session_timezone = 'UTC';

DROP TABLE IF EXISTS t_dt64_mono;
CREATE TABLE t_dt64_mono (d DateTime64(5)) ENGINE = MergeTree ORDER BY d
    SETTINGS index_granularity = 1, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;
-- Values spanning the DateTime64 range, including before 1970 and beyond 2106 where the seconds-since-epoch
-- exceed the UInt32 result and would wrap.
INSERT INTO t_dt64_mono VALUES ('1900-01-01 00:00:00'),('1969-12-31 23:59:59'),('1970-01-02 00:00:00'),('2000-01-01 00:00:00'),('2106-06-15 00:00:00'),('2200-01-01 00:00:00'),('2262-04-11 00:00:00');

SELECT '-- toStartOfDay';
SELECT count() FROM t_dt64_mono WHERE toStartOfDay(d) >= toDateTime('2000-01-01 00:00:00', 'UTC') SETTINGS force_primary_key = 1;
SELECT countIf(toStartOfDay(d) >= toDateTime('2000-01-01 00:00:00', 'UTC')) FROM t_dt64_mono;

SELECT '-- toRelativeSecondNum';
SELECT count() FROM t_dt64_mono WHERE toRelativeSecondNum(d) >= 946684800 SETTINGS force_primary_key = 1;
SELECT countIf(toRelativeSecondNum(d) >= 946684800) FROM t_dt64_mono;

SELECT '-- toRelativeMinuteNum';
SELECT count() FROM t_dt64_mono WHERE toRelativeMinuteNum(d) >= 15778080 SETTINGS force_primary_key = 1;
SELECT countIf(toRelativeMinuteNum(d) >= 15778080) FROM t_dt64_mono;

SELECT '-- toRelativeHourNum';
SELECT count() FROM t_dt64_mono WHERE toRelativeHourNum(d) >= 262968 SETTINGS force_primary_key = 1;
SELECT countIf(toRelativeHourNum(d) >= 262968) FROM t_dt64_mono;

SELECT '-- toRelativeDayNum';
SELECT count() FROM t_dt64_mono WHERE toRelativeDayNum(d) >= 10957 SETTINGS force_primary_key = 1;
SELECT countIf(toRelativeDayNum(d) >= 10957) FROM t_dt64_mono;

SELECT '-- toRelativeWeekNum';
SELECT count() FROM t_dt64_mono WHERE toRelativeWeekNum(d) >= 1565 SETTINGS force_primary_key = 1;
SELECT countIf(toRelativeWeekNum(d) >= 1565) FROM t_dt64_mono;

SELECT '-- toMonthNumSinceEpoch';
SELECT count() FROM t_dt64_mono WHERE toMonthNumSinceEpoch(d) >= 360 SETTINGS force_primary_key = 1;
SELECT countIf(toMonthNumSinceEpoch(d) >= 360) FROM t_dt64_mono;

SELECT '-- toYearNumSinceEpoch';
SELECT count() FROM t_dt64_mono WHERE toYearNumSinceEpoch(d) >= 30 SETTINGS force_primary_key = 1;
SELECT countIf(toYearNumSinceEpoch(d) >= 30) FROM t_dt64_mono;

-- Trivial-count projection path (the exact_ranges assertion): count() with an AggregatingMergeTree key.
SELECT '-- toStartOfDay trivial count with projection';
SELECT count() FROM t_dt64_mono WHERE toStartOfDay(d) >= toDateTime('2000-01-01 00:00:00', 'UTC')
    SETTINGS force_primary_key = 1, optimize_use_implicit_projections = 1, optimize_trivial_count_query = 1;

DROP TABLE t_dt64_mono;
