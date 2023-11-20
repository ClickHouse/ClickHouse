SET session_timezone = 'Europe/Amsterdam'; -- disable time zone randomization in CI

SELECT '-- negative tests';
SELECT fromDaysSinceYearZero(); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT fromDaysSinceYearZero32(); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT fromDaysSinceYearZero(1, 2); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT fromDaysSinceYearZero32(1, 2); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT fromDaysSinceYearZero('needs a number'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT fromDaysSinceYearZero32('needs a number'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT fromDaysSinceYearZero(-3); -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT fromDaysSinceYearZero32(-3); -- { serverError ARGUMENT_OUT_OF_BOUND }

SELECT '-- const and non-const arguments';
SELECT 719527 AS x, toInt32(x) AS y, fromDaysSinceYearZero(x), fromDaysSinceYearZero(materialize(x)), fromDaysSinceYearZero(y), fromDaysSinceYearZero(materialize(y)); -- outside Date's range
SELECT 719528 AS x, toInt32(x) AS y, fromDaysSinceYearZero(x), fromDaysSinceYearZero(materialize(x)), fromDaysSinceYearZero(y), fromDaysSinceYearZero(materialize(y));
SELECT 719529 AS x, toInt32(x) AS y, fromDaysSinceYearZero(x), fromDaysSinceYearZero(materialize(x)), fromDaysSinceYearZero(y), fromDaysSinceYearZero(materialize(y));
SELECT 785062 AS x, toInt32(x) AS y, fromDaysSinceYearZero(x), fromDaysSinceYearZero(materialize(x)), fromDaysSinceYearZero(y), fromDaysSinceYearZero(materialize(y));
SELECT 785063 AS x, toInt32(x) AS y, fromDaysSinceYearZero(x), fromDaysSinceYearZero(materialize(x)), fromDaysSinceYearZero(y), fromDaysSinceYearZero(materialize(y));
SELECT 785064 AS x, toInt32(x) AS y, fromDaysSinceYearZero(x), fromDaysSinceYearZero(materialize(x)), fromDaysSinceYearZero(y), fromDaysSinceYearZero(materialize(y)); -- outside Date's range

SELECT 693960 AS x, toInt32(x) AS y, fromDaysSinceYearZero32(x), fromDaysSinceYearZero32(materialize(x)), fromDaysSinceYearZero32(y), fromDaysSinceYearZero32(materialize(y)); -- outside Date32's range
SELECT 693961 AS x, toInt32(x) AS y, fromDaysSinceYearZero32(x), fromDaysSinceYearZero32(materialize(x)), fromDaysSinceYearZero32(y), fromDaysSinceYearZero32(materialize(y));
SELECT 693962 AS x, toInt32(x) AS y, fromDaysSinceYearZero32(x), fromDaysSinceYearZero32(materialize(x)), fromDaysSinceYearZero32(y), fromDaysSinceYearZero32(materialize(y));
SELECT 840056 AS x, toInt32(x) AS y, fromDaysSinceYearZero32(x), fromDaysSinceYearZero32(materialize(x)), fromDaysSinceYearZero32(y), fromDaysSinceYearZero32(materialize(y));
SELECT 840057 AS x, toInt32(x) AS y, fromDaysSinceYearZero32(x), fromDaysSinceYearZero32(materialize(x)), fromDaysSinceYearZero32(y), fromDaysSinceYearZero32(materialize(y));
SELECT 840058 AS x, toInt32(x) AS y, fromDaysSinceYearZero32(x), fromDaysSinceYearZero32(materialize(x)), fromDaysSinceYearZero32(y), fromDaysSinceYearZero32(materialize(y)); -- outside Date32's range

SELECT '-- integer types != (U)Int32';
SELECT toUInt8(255) AS x,  toInt8(127) AS y, fromDaysSinceYearZero(x), fromDaysSinceYearZero32(x), fromDaysSinceYearZero(y), fromDaysSinceYearZero32(y); -- outside Date's range for all (U)Int8-s
SELECT toUInt16(65535) AS x, toInt16(32767) AS y, fromDaysSinceYearZero(x), fromDaysSinceYearZero32(x), fromDaysSinceYearZero(y), fromDaysSinceYearZero32(y); -- outside Date's range for all (U)Int16-s
SELECT toUInt64(719529) AS x, toInt64(719529) AS y, fromDaysSinceYearZero(x), fromDaysSinceYearZero32(x), fromDaysSinceYearZero(y), fromDaysSinceYearZero32(y); -- something useful

SELECT '-- NULL handling';
SELECT fromDaysSinceYearZero(NULL), fromDaysSinceYearZero32(NULL);

SELECT '-- Alias';
SELECT FROM_DAYS(1);
