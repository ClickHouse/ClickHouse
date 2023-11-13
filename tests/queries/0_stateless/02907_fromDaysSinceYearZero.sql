SET session_timezone = 'Europe/Amsterdam'; -- disable time zone randomization in CI

SELECT '-- negative tests';
SELECT fromDaysSinceYearZero(); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT fromDaysSinceYearZero32(); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT fromDaysSinceYearZero(1, 2); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT fromDaysSinceYearZero32(1, 2); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT fromDaysSinceYearZero('needs a number'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT fromDaysSinceYearZero32('needs a number'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT fromDaysSinceYearZero(-3); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT fromDaysSinceYearZero32(-3); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT '-- const and non-const arguments';

SELECT 719527 AS x, fromDaysSinceYearZero(x), fromDaysSinceYearZero(materialize(x)); -- outside Date's range
SELECT 719528 AS x, fromDaysSinceYearZero(x), fromDaysSinceYearZero(materialize(x));
SELECT 719529 AS x, fromDaysSinceYearZero(x), fromDaysSinceYearZero(materialize(x));
SELECT 785062 AS x, fromDaysSinceYearZero(x), fromDaysSinceYearZero(materialize(x));
SELECT 785063 AS x, fromDaysSinceYearZero(x), fromDaysSinceYearZero(materialize(x));
SELECT 785064 AS x, fromDaysSinceYearZero(x), fromDaysSinceYearZero(materialize(x)); -- outside Date's range

SELECT 693960 AS x, fromDaysSinceYearZero32(x), fromDaysSinceYearZero32(materialize(x)); -- outside Date32's range
SELECT 693961 AS x, fromDaysSinceYearZero32(x), fromDaysSinceYearZero32(materialize(x));
SELECT 693962 AS x, fromDaysSinceYearZero32(x), fromDaysSinceYearZero32(materialize(x));
SELECT 840056 AS x, fromDaysSinceYearZero32(x), fromDaysSinceYearZero32(materialize(x));
SELECT 840057 AS x, fromDaysSinceYearZero32(x), fromDaysSinceYearZero32(materialize(x));
SELECT 840058 AS x, fromDaysSinceYearZero32(x), fromDaysSinceYearZero32(materialize(x)); -- outside Date32's range

SELECT '-- integer types != UInt32';
SELECT toUInt8(255) AS x, fromDaysSinceYearZero(x), fromDaysSinceYearZero32(x); -- outside Date's range for all UInt8-s
SELECT toUInt16(65535) AS x, fromDaysSinceYearZero(x), fromDaysSinceYearZero32(x); -- outside Date's range for all UInt16-s
SELECT toUInt64(719529) AS x, fromDaysSinceYearZero(x), fromDaysSinceYearZero32(x); -- something useful

SELECT '-- NULL handling';
SELECT fromDaysSinceYearZero(NULL), fromDaysSinceYearZero32(NULL);

SELECT '-- Alias';
SELECT FROM_DAYS(1);
