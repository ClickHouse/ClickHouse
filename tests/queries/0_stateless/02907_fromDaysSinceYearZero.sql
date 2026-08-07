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

SELECT '-- UInt32 and Int32 arguments, both const and non-const';
SELECT 719527 AS u, toInt32(u) AS s, fromDaysSinceYearZero(u), fromDaysSinceYearZero(materialize(u)), fromDaysSinceYearZero(s), fromDaysSinceYearZero(materialize(s)); -- outside Date's range
SELECT 719528 AS u, toInt32(u) AS s, fromDaysSinceYearZero(u), fromDaysSinceYearZero(materialize(u)), fromDaysSinceYearZero(s), fromDaysSinceYearZero(materialize(s));
SELECT 719529 AS u, toInt32(u) AS s, fromDaysSinceYearZero(u), fromDaysSinceYearZero(materialize(u)), fromDaysSinceYearZero(s), fromDaysSinceYearZero(materialize(s));
SELECT 785062 AS u, toInt32(u) AS s, fromDaysSinceYearZero(u), fromDaysSinceYearZero(materialize(u)), fromDaysSinceYearZero(s), fromDaysSinceYearZero(materialize(s));
SELECT 785063 AS u, toInt32(u) AS s, fromDaysSinceYearZero(u), fromDaysSinceYearZero(materialize(u)), fromDaysSinceYearZero(s), fromDaysSinceYearZero(materialize(s));
SELECT 785064 AS u, toInt32(u) AS s, fromDaysSinceYearZero(u), fromDaysSinceYearZero(materialize(u)), fromDaysSinceYearZero(s), fromDaysSinceYearZero(materialize(s)); -- outside Date's range

SELECT 693960 AS u, toInt32(u) AS s, fromDaysSinceYearZero32(u), fromDaysSinceYearZero32(materialize(u)), fromDaysSinceYearZero32(s), fromDaysSinceYearZero32(materialize(s)); -- outside Date32's range
SELECT 693961 AS u, toInt32(u) AS s, fromDaysSinceYearZero32(u), fromDaysSinceYearZero32(materialize(u)), fromDaysSinceYearZero32(s), fromDaysSinceYearZero32(materialize(s));
SELECT 693962 AS u, toInt32(u) AS s, fromDaysSinceYearZero32(u), fromDaysSinceYearZero32(materialize(u)), fromDaysSinceYearZero32(s), fromDaysSinceYearZero32(materialize(s));
SELECT 840056 AS u, toInt32(u) AS s, fromDaysSinceYearZero32(u), fromDaysSinceYearZero32(materialize(u)), fromDaysSinceYearZero32(s), fromDaysSinceYearZero32(materialize(s));
SELECT 840057 AS u, toInt32(u) AS s, fromDaysSinceYearZero32(u), fromDaysSinceYearZero32(materialize(u)), fromDaysSinceYearZero32(s), fromDaysSinceYearZero32(materialize(s));
SELECT 840058 AS u, toInt32(u) AS s, fromDaysSinceYearZero32(u), fromDaysSinceYearZero32(materialize(u)), fromDaysSinceYearZero32(s), fromDaysSinceYearZero32(materialize(s)); -- outside Date32's range

SELECT '-- integer types != (U)Int32';
SELECT toUInt8(255) AS u,  toInt8(127) AS s, fromDaysSinceYearZero(u), fromDaysSinceYearZero32(u), fromDaysSinceYearZero(s), fromDaysSinceYearZero32(s); -- outside Date's range for all (U)Int8-s
SELECT toUInt16(65535) AS u, toInt16(32767) AS s, fromDaysSinceYearZero(u), fromDaysSinceYearZero32(u), fromDaysSinceYearZero(s), fromDaysSinceYearZero32(s); -- outside Date's range for all (U)Int16-s
SELECT toUInt64(719529) AS u, toInt64(719529) AS s, fromDaysSinceYearZero(u), fromDaysSinceYearZero32(u), fromDaysSinceYearZero(s), fromDaysSinceYearZero32(s); -- something useful

SELECT '-- NULL handling';
SELECT fromDaysSinceYearZero(NULL), fromDaysSinceYearZero32(NULL);

SELECT '-- ubsan bugs';
SELECT fromDaysSinceYearZero32(2147483648);
SELECT fromDaysSinceYearZero32(3);

SELECT '-- Alias';
SELECT FROM_DAYS(1);
