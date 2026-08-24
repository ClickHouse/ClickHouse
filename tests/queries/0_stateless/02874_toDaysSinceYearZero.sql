SET session_timezone = 'Europe/Amsterdam'; -- disable time zone randomization in CI

SELECT 'Invalid parameters';
SELECT toDaysSinceYearZero(); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT toDaysSinceYearZero(toDate('2023-09-08'), 3); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT toDaysSinceYearZero('str'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT toDaysSinceYearZero(42); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT 'Const argument';
SELECT toDaysSinceYearZero(toDate('1970-01-01'));
SELECT toDaysSinceYearZero(toDate('2023-09-08'));
SELECT toDaysSinceYearZero(toDate('2023-09-08'), 'America/Los_Angeles');
SELECT toDaysSinceYearZero(toDate32('1900-01-01'));
SELECT toDaysSinceYearZero(toDate32('2023-09-08'));
SELECT toDaysSinceYearZero(toDate32('2023-09-08'), 'America/Los_Angeles');
SELECT toDaysSinceYearZero(toDateTime('1970-01-01 00:00:00'));
SELECT toDaysSinceYearZero(toDateTime('2023-09-08 11:11:11'));
SELECT toDaysSinceYearZero(toDateTime('2023-09-08 11:11:11'), 'America/Los_Angeles');
SELECT toDaysSinceYearZero(toDateTime64('1900-01-01 00:00:00.000', 3));
SELECT toDaysSinceYearZero(toDateTime64('2023-09-08 11:11:11.123', 3));
SELECT toDaysSinceYearZero(toDateTime64('2023-09-08 11:11:11.123', 3), 'America/Los_Angeles');
SELECT toDaysSinceYearZero(toDateTime64('2023-09-08 11:11:11.123123123', 9));
SELECT toDaysSinceYearZero(NULL);

SELECT 'Non-const argument';
SELECT toDaysSinceYearZero(materialize(toDate('2023-09-08')));
SELECT toDaysSinceYearZero(materialize(toDate32('2023-09-08')));
SELECT toDaysSinceYearZero(materialize(toDateTime('2023-09-08 11:11:11')));
SELECT toDaysSinceYearZero(materialize(toDateTime64('2023-09-08 11:11:11.123', 3)));
SELECT toDaysSinceYearZero(materialize(toDateTime64('2023-09-08 11:11:11.123123123', 9)));

SELECT 'MySQL alias';
SELECT to_days(toDate('2023-09-08'));
SELECT TO_DAYS(toDate('2023-09-08'));
