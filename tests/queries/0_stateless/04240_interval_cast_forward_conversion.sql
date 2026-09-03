-- Forward CAST between IntervalKind units must use the right conversion factor.
-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/104986

-- { echo }
-- Adjacent kinds: forward (smaller -> larger)
SELECT CAST(toIntervalNanosecond(1000) AS IntervalMicrosecond);
SELECT CAST(toIntervalMicrosecond(1000) AS IntervalMillisecond);
SELECT CAST(toIntervalMillisecond(1000) AS IntervalSecond);
SELECT CAST(toIntervalSecond(60) AS IntervalMinute);
SELECT CAST(toIntervalMinute(60) AS IntervalHour);
SELECT CAST(toIntervalHour(24) AS IntervalDay);
SELECT CAST(toIntervalDay(7) AS IntervalWeek);
SELECT CAST(toIntervalWeek(4) AS IntervalMonth);
SELECT CAST(toIntervalMonth(3) AS IntervalQuarter);
SELECT CAST(toIntervalQuarter(4) AS IntervalYear);

-- Adjacent kinds: backward (larger -> smaller)
SELECT CAST(toIntervalMicrosecond(1) AS IntervalNanosecond);
SELECT CAST(toIntervalMillisecond(1) AS IntervalMicrosecond);
SELECT CAST(toIntervalSecond(1) AS IntervalMillisecond);
SELECT CAST(toIntervalMinute(1) AS IntervalSecond);
SELECT CAST(toIntervalHour(1) AS IntervalMinute);
SELECT CAST(toIntervalDay(1) AS IntervalHour);
SELECT CAST(toIntervalWeek(1) AS IntervalDay);
SELECT CAST(toIntervalMonth(1) AS IntervalWeek);
SELECT CAST(toIntervalQuarter(1) AS IntervalMonth);
SELECT CAST(toIntervalYear(1) AS IntervalQuarter);

-- Multi-step conversions
SELECT CAST(toIntervalSecond(3600) AS IntervalHour);
SELECT CAST(toIntervalSecond(86400) AS IntervalDay);
SELECT CAST(toIntervalHour(1) AS IntervalSecond);
SELECT CAST(toIntervalDay(1) AS IntervalSecond);
SELECT CAST(toIntervalNanosecond(1000000000) AS IntervalSecond);
SELECT CAST(toIntervalMonth(12) AS IntervalYear);
SELECT CAST(toIntervalWeek(48) AS IntervalYear);
SELECT CAST(toIntervalYear(1) AS IntervalMonth);

-- Same kind: identity
SELECT CAST(toIntervalSecond(42) AS IntervalSecond);
