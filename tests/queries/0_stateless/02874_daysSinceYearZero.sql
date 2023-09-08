SELECT 'Reject invalid parameters';
SELECT daysSinceYearZero(); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT daysSinceYearZero(toDate('2023-09-08'), toDate('2023-09-08')); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT daysSinceYearZero('str'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT daysSinceYearZero(42); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT daysSinceYearZero(toDateTime('2023-09-08 11:11:11')); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT daysSinceYearZero(toDateTime64('2023-09-08 11:11:11', 3)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT 'Const argument';
SELECT daysSinceYearZero(toDate('1970-01-01'));
SELECT daysSinceYearZero(toDate('2023-09-08'));
SELECT daysSinceYearZero(toDate32('1900-01-01'));
SELECT daysSinceYearZero(toDate32('2023-09-08'));
SELECT daysSinceYearZero(NULL);

SELECT 'Non-const argument';
SELECT daysSinceYearZero(materialize(toDate('2023-09-08')));
SELECT daysSinceYearZero(materialize(toDate32('2023-09-08')));

SELECT 'MySQL alias';
SELECT to_days(toDate('2023-09-08'));
SELECT TO_DAYS(toDate('2023-09-08'));
