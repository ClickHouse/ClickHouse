-- Date32 supports the extended range [0000-01-01, 9999-12-31], same as DateTime64.
-- https://github.com/ClickHouse/ClickHouse/issues/111524

SET session_timezone = 'UTC';

SELECT 'parsing from String';
SELECT toDate32('0079-08-24');
SELECT toDate32('0000-01-01');
SELECT toDate32('1899-12-31');
SELECT toDate32('1900-01-01');
SELECT toDate32('2299-12-31');
SELECT toDate32('2300-01-01');
SELECT toDate32('9999-12-31');
SELECT CAST('0079-08-24' AS Date32);

SELECT 'parsing is symmetric with the cast from DateTime64';
SELECT toDate32(toDateTime64('0079-08-24 13:00:00', 0)), toDate32('0079-08-24');
SELECT toDate32(toDateTime64('9999-12-31 13:00:00', 0)), toDate32('9999-12-31');

SELECT 'the example from the issue';
SELECT age('year', toDate32('0079-08-24'), toDate32('2026-07-23'));

SELECT 'OrZero and OrNull accept the extended range, garbage is unchanged';
SELECT toDate32OrZero('1899-01-01'), toDate32OrNull('1899-01-01');
SELECT toDate32OrZero('2500-06-15'), toDate32OrNull('2500-06-15');
SELECT toDate32OrZero('garbage'), toDate32OrNull('garbage');

SELECT 'the underlying Int32 value';
SELECT toInt32(toDate32('0000-01-01')), toInt32(toDate32('9999-12-31'));

SELECT 'conversion from numbers: day numbers up to 9999-12-31, seconds above';
SELECT toDate32(-719528);
SELECT toDate32(-1000000);
SELECT toDate32(120530);
SELECT toDate32(2932896);
SELECT toDate32(2932897);
SELECT toDate32(4102444800);
SELECT toDate32(253402300799);
SELECT toDate32(999999999999);

SELECT 'date and time functions over the extended range';
SELECT toYear(toDate32('0079-08-24')), toMonth(toDate32('0079-08-24')), toDayOfMonth(toDate32('0079-08-24')), toDayOfWeek(toDate32('0079-08-24'));
SELECT toYear(toDate32('9999-12-31')), toMonth(toDate32('9999-12-31')), toDayOfMonth(toDate32('9999-12-31')), toDayOfWeek(toDate32('9999-12-31'));
-- Without enable_extended_results_for_datetime_functions these return Date and clamp (legacy behavior)
SELECT toStartOfMonth(toDate32('0079-08-24')), toStartOfYear(toDate32('0079-08-24')), toLastDayOfMonth(toDate32('0079-08-24'));
SELECT toStartOfMonth(toDate32('0079-08-24')), toStartOfYear(toDate32('0079-08-24')), toLastDayOfMonth(toDate32('0079-08-24')) SETTINGS enable_extended_results_for_datetime_functions = 1;
SELECT toMonday(toDate32('0079-08-24')) SETTINGS enable_extended_results_for_datetime_functions = 1;
SELECT toISOYear(toDate32('0079-08-24')), toISOWeek(toDate32('0079-08-24')), toQuarter(toDate32('0079-08-24')), toDayOfYear(toDate32('0079-08-24'));
SELECT toYYYYMM(toDate32('0079-08-24')), toYYYYMMDD(toDate32('0079-08-24'));
SELECT formatDateTime(toDate32('0079-08-24'), '%Y-%m-%d %W');
SELECT toString(toDate32('0079-08-24'));

SELECT 'arithmetic over the extended range';
SELECT toDate32('0079-08-24') + INTERVAL 1 DAY, toDate32('0079-08-24') - INTERVAL 1 DAY;
SELECT toDate32('0079-08-24') + INTERVAL 3 MONTH, toDate32('0079-08-24') + INTERVAL 2000 YEAR;
SELECT addDays(toDate32('9999-12-31'), -1), addMonths(toDate32('0000-01-01'), 1);
SELECT dateDiff('year', toDate32('0079-08-24'), toDate32('2079-08-24'));
SELECT dateDiff('day', toDate32('0079-08-24'), toDate32('0079-08-25'));

SELECT 'makeDate32 and YYYYMMDDToDate32';
SELECT makeDate32(79, 8, 24), makeDate32(0, 1, 1), makeDate32(9999, 12, 31);
SELECT makeDate32(79, 236);
SELECT YYYYMMDDToDate32(791231), YYYYMMDDToDate32(99991231);

SELECT 'changeDate';
SELECT changeYear(toDate32('2000-06-15'), 850), changeYear(toDate32('2000-06-15'), 9999);
SELECT changeMonth(toDate32('0850-06-15'), 12), changeDay(toDate32('0850-06-15'), 1);

SELECT 'fromDaysSinceYearZero32 and toDaysSinceYearZero';
SELECT fromDaysSinceYearZero32(0), fromDaysSinceYearZero32(3652424);
SELECT toDaysSinceYearZero(toDate32('0000-01-01')), toDaysSinceYearZero(toDate32('9999-12-31'));

SELECT 'saturation at the boundaries of the representable range';
SELECT toDate32(-719529);
SELECT toDate32(-9223372036854775808);
-- The ordinary conversion saturates, while the numeric accurate cast rejects a value the type cannot represent
-- (see `04652_accurate_cast_numeric_to_date32`); this pull request only moves the boundary of the representable
-- window of `Date32` from `1900-01-01` to `0000-01-01`.
SELECT accurateCastOrNull(-719529, 'Date32'), accurateCastOrNull(-719528, 'Date32');
SELECT accurateCastOrNull('9999-12-31', 'Date32'), accurateCastOrNull('99999-12-31', 'Date32');

SELECT 'text formats';
SELECT * FROM format(CSV, 'd Date32', '0079-08-24\n2345-01-02\n9999-12-31') ORDER BY d;
SELECT * FROM format(JSONEachRow, 'd Date32', '{"d" : "0079-08-24"}');
SELECT * FROM format(TSV, 'd Date32', '0079-08-24');
SELECT * FROM format(Values, 'd Date32', '(\'0079-08-24\')');

SELECT 'sorting and index over the extended range';
DROP TABLE IF EXISTS date32_extended_range;
CREATE TABLE date32_extended_range (d Date32) ENGINE = MergeTree ORDER BY d;
INSERT INTO date32_extended_range VALUES ('0100-01-01'), ('1500-06-15'), ('2000-01-01'), ('9000-12-31');
SELECT * FROM date32_extended_range ORDER BY d;
SELECT count() FROM date32_extended_range WHERE d < toDate32('1900-01-01');
SELECT count() FROM date32_extended_range WHERE d BETWEEN toDate32('1000-01-01') AND toDate32('2299-12-31');
SELECT min(d), max(d) FROM date32_extended_range;
DROP TABLE date32_extended_range;
