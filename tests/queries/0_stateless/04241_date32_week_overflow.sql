-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/104970
-- `Date32`/`DateTime64` values whose rounded result falls before the Unix epoch
-- must clamp to 1970-01-01 instead of overflowing through UInt16

SET session_timezone = 'UTC';

-- toStartOfWeek on Date32: primary bug from the issue
SELECT toStartOfWeek(toDate32('1970-01-01'));
SELECT toStartOfWeek(toDate32('1970-01-02'));
SELECT toStartOfWeek(toDate32('1970-01-03'));
SELECT toStartOfWeek(toDate32('1900-01-01'));
SELECT toStartOfWeek(toDate32('1969-12-28'));

-- toLastDayOfWeek on Date32
SELECT toLastDayOfWeek(toDate32('1900-01-01'));

-- toMonday on Date32 and DateTime64 (the issue lists this as an open risk)
SELECT toMonday(toDate32('1970-01-01'));
SELECT toMonday(toDate32('1969-12-30'));
SELECT toMonday(toDateTime64('1969-12-30 00:00:00', 0));

-- toStartOfMonth / toLastDayOfMonth / toStartOfQuarter / toStartOfYear on Date32
SELECT toStartOfMonth(toDate32('1969-01-01'));
SELECT toLastDayOfMonth(toDate32('1969-12-31'));
SELECT toStartOfQuarter(toDate32('1969-01-01'));
SELECT toStartOfYear(toDate32('1969-01-01'));

-- Sanity: post-epoch values are unchanged
SELECT toStartOfWeek(toDate32('2020-05-10'));
SELECT toMonday(toDate32('2020-05-10'));
SELECT toStartOfMonth(toDate32('2020-05-10'));
SELECT toStartOfQuarter(toDate32('2020-05-10'));
SELECT toStartOfYear(toDate32('2020-05-10'));
