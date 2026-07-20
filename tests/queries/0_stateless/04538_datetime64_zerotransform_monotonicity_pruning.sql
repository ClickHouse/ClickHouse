-- DateTime64 ZeroTransform date/time functions report themselves monotonic to the primary index, so they
-- must be monotonic over the whole DateTime64 range. Several previously wrapped for out-of-range arguments
-- (toStartOfDay, the round-down toStartOf{Minute,FiveMinutes,TenMinutes,FifteenMinutes,Hour}, timeSlot,
-- the default toStartOfInterval, and the relative-number / *NumSinceEpoch transforms narrowed the result to
-- UInt32/UInt16 without saturating), which made primary-key pruning drop granules that actually contain
-- matching rows (and tripped the exact_ranges assertion in the trivial-count projection optimization).
-- They now saturate. Each check compares the primary-key-pruned count() against the unpruned countIf(): a
-- monotonic function makes them equal, a wrapping one made the pruned count too small (or too large).

SET session_timezone = 'UTC';
-- Pin standard-precision results: the wrapping narrow-cast paths this test covers are only reached when
-- extended results are off. With extended results the transforms return wider Date32/DateTime64 values that
-- never wrap, so the checks would pass regardless of the fix.
SET enable_extended_results_for_datetime_functions = 0;

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

-- Round-down transforms: their standard-precision DateTime64 result is seconds-since-epoch and exceeds the
-- UInt32 result beyond 2106, so it used to wrap. The 2000-01-01 threshold matches four of the seven rows.
SELECT '-- toStartOfHour';
SELECT count() FROM t_dt64_mono WHERE toStartOfHour(d) >= toDateTime('2000-01-01 00:00:00', 'UTC') SETTINGS force_primary_key = 1;
SELECT countIf(toStartOfHour(d) >= toDateTime('2000-01-01 00:00:00', 'UTC')) FROM t_dt64_mono;

SELECT '-- toStartOfMinute';
SELECT count() FROM t_dt64_mono WHERE toStartOfMinute(d) >= toDateTime('2000-01-01 00:00:00', 'UTC') SETTINGS force_primary_key = 1;
SELECT countIf(toStartOfMinute(d) >= toDateTime('2000-01-01 00:00:00', 'UTC')) FROM t_dt64_mono;

SELECT '-- toStartOfFiveMinutes';
SELECT count() FROM t_dt64_mono WHERE toStartOfFiveMinutes(d) >= toDateTime('2000-01-01 00:00:00', 'UTC') SETTINGS force_primary_key = 1;
SELECT countIf(toStartOfFiveMinutes(d) >= toDateTime('2000-01-01 00:00:00', 'UTC')) FROM t_dt64_mono;

SELECT '-- toStartOfTenMinutes';
SELECT count() FROM t_dt64_mono WHERE toStartOfTenMinutes(d) >= toDateTime('2000-01-01 00:00:00', 'UTC') SETTINGS force_primary_key = 1;
SELECT countIf(toStartOfTenMinutes(d) >= toDateTime('2000-01-01 00:00:00', 'UTC')) FROM t_dt64_mono;

SELECT '-- toStartOfFifteenMinutes';
SELECT count() FROM t_dt64_mono WHERE toStartOfFifteenMinutes(d) >= toDateTime('2000-01-01 00:00:00', 'UTC') SETTINGS force_primary_key = 1;
SELECT countIf(toStartOfFifteenMinutes(d) >= toDateTime('2000-01-01 00:00:00', 'UTC')) FROM t_dt64_mono;

SELECT '-- timeSlot';
SELECT count() FROM t_dt64_mono WHERE timeSlot(d) >= toDateTime('2000-01-01 00:00:00', 'UTC') SETTINGS force_primary_key = 1;
SELECT countIf(timeSlot(d) >= toDateTime('2000-01-01 00:00:00', 'UTC')) FROM t_dt64_mono;

-- Default toStartOfInterval: INTERVAL 1 {MINUTE,HOUR,DAY} yields a DateTime (UInt32 seconds) result that
-- wrapped for out-of-range arguments; INTERVAL 1 {WEEK,YEAR} yields a Date (UInt16 days) result that wrapped.
SELECT '-- toStartOfInterval 1 HOUR';
SELECT count() FROM t_dt64_mono WHERE toStartOfInterval(d, INTERVAL 1 HOUR) >= toDateTime('2000-01-01 00:00:00', 'UTC') SETTINGS force_primary_key = 1;
SELECT countIf(toStartOfInterval(d, INTERVAL 1 HOUR) >= toDateTime('2000-01-01 00:00:00', 'UTC')) FROM t_dt64_mono;

SELECT '-- toStartOfInterval 1 DAY';
SELECT count() FROM t_dt64_mono WHERE toStartOfInterval(d, INTERVAL 1 DAY) >= toDateTime('2000-01-01 00:00:00', 'UTC') SETTINGS force_primary_key = 1;
SELECT countIf(toStartOfInterval(d, INTERVAL 1 DAY) >= toDateTime('2000-01-01 00:00:00', 'UTC')) FROM t_dt64_mono;

SELECT '-- toStartOfInterval 1 YEAR';
SELECT count() FROM t_dt64_mono WHERE toStartOfInterval(d, INTERVAL 1 YEAR) >= toDate('2000-01-01') SETTINGS force_primary_key = 1;
SELECT countIf(toStartOfInterval(d, INTERVAL 1 YEAR) >= toDate('2000-01-01')) FROM t_dt64_mono;

-- Trivial-count projection path (the exact_ranges assertion): count() with an AggregatingMergeTree key.
SELECT '-- toStartOfDay trivial count with projection';
SELECT count() FROM t_dt64_mono WHERE toStartOfDay(d) >= toDateTime('2000-01-01 00:00:00', 'UTC')
    SETTINGS force_primary_key = 1, optimize_use_implicit_projections = 1, optimize_trivial_count_query = 1;

DROP TABLE t_dt64_mono;

-- Date carrier (UInt16 days): the standard-precision toStartOfDay / toRelativeSecondNum results are
-- seconds-since-epoch (UInt32), which overflows for a Date beyond 2106-02-07 (Date max is 2149-06-06) and
-- used to wrap the same way as the DateTime64 carrier above, mis-pruning a Date primary key. The default
-- toStartOfInterval on a Date argument with INTERVAL DAY yields the same UInt32 DateTime result and had the
-- same high-side wrap. Date is unsigned / epoch-based, so saturating to the type maximum keeps it monotonic.
DROP TABLE IF EXISTS t_date_mono;
CREATE TABLE t_date_mono (d Date) ENGINE = MergeTree ORDER BY d
    SETTINGS index_granularity = 1, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;
INSERT INTO t_date_mono VALUES ('1970-01-02'),('2000-01-01'),('2106-02-06'),('2106-02-08'),('2149-06-06');

SELECT '-- Date toStartOfDay';
SELECT count() FROM t_date_mono WHERE toStartOfDay(d) >= toDateTime('2000-01-01 00:00:00', 'UTC') SETTINGS force_primary_key = 1;
SELECT countIf(toStartOfDay(d) >= toDateTime('2000-01-01 00:00:00', 'UTC')) FROM t_date_mono;

SELECT '-- Date toRelativeSecondNum';
SELECT count() FROM t_date_mono WHERE toRelativeSecondNum(d) >= 946684800 SETTINGS force_primary_key = 1;
SELECT countIf(toRelativeSecondNum(d) >= 946684800) FROM t_date_mono;

SELECT '-- Date toStartOfInterval 1 DAY';
SELECT count() FROM t_date_mono WHERE toStartOfInterval(d, INTERVAL 1 DAY) >= toDateTime('2000-01-01 00:00:00', 'UTC') SETTINGS force_primary_key = 1;
SELECT countIf(toStartOfInterval(d, INTERVAL 1 DAY) >= toDateTime('2000-01-01 00:00:00', 'UTC')) FROM t_date_mono;

DROP TABLE t_date_mono;
