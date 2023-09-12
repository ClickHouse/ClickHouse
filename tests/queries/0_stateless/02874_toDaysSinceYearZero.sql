SELECT 'Invalid parameters';
SELECT toDaysSinceYearZero(); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT toDaysSinceYearZero(toDate('2023-09-08'), toDate('2023-09-08')); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT toDaysSinceYearZero('str'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT toDaysSinceYearZero(42); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT toDaysSinceYearZero(toDateTime('2023-09-08 11:11:11')); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT toDaysSinceYearZero(toDateTime64('2023-09-08 11:11:11', 3)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT 'Const argument';
SELECT toDaysSinceYearZero(toDate('1970-01-01'));
SELECT toDaysSinceYearZero(toDate('2023-09-08'));
SELECT toDaysSinceYearZero(toDate32('1900-01-01'));
SELECT toDaysSinceYearZero(toDate32('2023-09-08'));
SELECT toDaysSinceYearZero(NULL);

SELECT 'Non-const argument';
SELECT toDaysSinceYearZero(materialize(toDate('2023-09-08')));
SELECT toDaysSinceYearZero(materialize(toDate32('2023-09-08')));

SELECT 'MySQL alias';
SELECT to_days(toDate('2023-09-08'));
SELECT TO_DAYS(toDate('2023-09-08'));
