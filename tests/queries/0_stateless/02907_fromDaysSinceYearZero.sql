SET session_timezone = 'Europe/Amsterdam'; -- disable time zone randomization in CI

SELECT 'Const argument';
SELECT 719528 AS x, fromDaysSinceYearZero(x);
SELECT 739136 AS x, fromDaysSinceYearZero(x);
SELECT 785063 AS x, fromDaysSinceYearZero(x);
SELECT 840057 AS x, fromDaysSinceYearZero32(x);
SELECT 693961 AS x, fromDaysSinceYearZero32(x);
SELECT 719527 AS x, fromDaysSinceYearZero32(x);
SELECT 719528 AS x, fromDaysSinceYearZero32(x);
SELECT 739136 AS x, fromDaysSinceYearZero32(x);
SELECT 693961 AS x, fromDaysSinceYearZero32(x);
SELECT 785063 AS x, fromDaysSinceYearZero32(x);
SELECT 785064 AS x, fromDaysSinceYearZero32(x);
SELECT -10 AS x, fromDaysSinceYearZero(x), 'lower clip, 1970-01-01 min'; -- {serverError ILLEGAL_TYPE_OF_ARGUMENT}
SELECT -10 AS x, fromDaysSinceYearZero32(x), 'lower clip, 1970-01-01 min'; -- {serverError ILLEGAL_TYPE_OF_ARGUMENT}
SELECT fromDaysSinceYearZero(NULL);
SELECT fromDaysSinceYearZero32(NULL);

SELECT FROM_DAYS(1); -- test alias
