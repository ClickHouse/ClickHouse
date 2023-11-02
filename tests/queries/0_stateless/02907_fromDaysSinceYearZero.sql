SET session_timezone = 'Europe/Amsterdam'; -- disable time zone randomization in CI

SET date_time_overflow_behavior = 'ignore';
SELECT 'date_time_overflow_behavior = ignore';

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
SELECT -10 AS x, fromDaysSinceYearZero(x), 'lower clip, 1970-01-01 min'; -- {serverError ARGUMENT_OUT_OF_BOUND}
SELECT -10 AS x, fromDaysSinceYearZero32(x), 'lower clip, 1970-01-01 min'; -- {serverError ARGUMENT_OUT_OF_BOUND}
SELECT fromDaysSinceYearZero(NULL);
SELECT fromDaysSinceYearZero32(NULL);

SET date_time_overflow_behavior = 'saturate';
SELECT 'date_time_overflow_behavior = saturate';

SELECT 'Const argument';
SELECT 719527 AS x, fromDaysSinceYearZero(x), 'lower clip, 1970-01-01 min';
SELECT 719528 AS x, fromDaysSinceYearZero(x);
SELECT 739136 AS x, fromDaysSinceYearZero(x);
SELECT 785063 AS x, fromDaysSinceYearZero(x);
SELECT 785064 AS x, fromDaysSinceYearZero(x), 'upper clip, 2149-06-06 max';
SELECT 693961 AS x, fromDaysSinceYearZero(x);
SELECT 693960 AS x, fromDaysSinceYearZero32(x), 'lower clip, 1900-01-01 min';
SELECT 693961 AS x, fromDaysSinceYearZero32(x);
SELECT 840056 AS x, fromDaysSinceYearZero32(x);
SELECT 840057 AS x, fromDaysSinceYearZero32(x), 'upper clip, 2299-12-31 max';
SELECT 719527 AS x, fromDaysSinceYearZero32(x);
SELECT 719528 AS x, fromDaysSinceYearZero32(x);
SELECT 739136 AS x, fromDaysSinceYearZero32(x);
SELECT 693961 AS x, fromDaysSinceYearZero32(x);
SELECT 785063 AS x, fromDaysSinceYearZero32(x);
SELECT 785064 AS x, fromDaysSinceYearZero32(x);
SELECT -10 AS x, fromDaysSinceYearZero(x), 'lower clip, 1970-01-01 min'; -- {serverError ARGUMENT_OUT_OF_BOUND}
SELECT -10 AS x, fromDaysSinceYearZero32(x), 'lower clip, 1970-01-01 min'; -- {serverError ARGUMENT_OUT_OF_BOUND}
SELECT fromDaysSinceYearZero(NULL);
SELECT fromDaysSinceYearZero32(NULL);


SET date_time_overflow_behavior = 'throw';
SELECT 'date_time_overflow_behavior = throw';

SELECT 'Const argument';
SELECT 719528 AS x, fromDaysSinceYearZero(x);
SELECT 739136 AS x, fromDaysSinceYearZero(x);
SELECT 785063 AS x, fromDaysSinceYearZero(x);
SELECT 840056 AS x, fromDaysSinceYearZero32(x);
SELECT 719527 AS x, fromDaysSinceYearZero32(x);
SELECT 719528 AS x, fromDaysSinceYearZero32(x);
SELECT 739136 AS x, fromDaysSinceYearZero32(x);
SELECT 693961 AS x, fromDaysSinceYearZero32(x);
SELECT 785063 AS x, fromDaysSinceYearZero32(x);
SELECT 785064 AS x, fromDaysSinceYearZero32(x);
SELECT fromDaysSinceYearZero(NULL);
SELECT fromDaysSinceYearZero32(NULL);
SELECT 0.5 AS x, fromDaysSinceYearZero(x); -- {serverError ILLEGAL_TYPE_OF_ARGUMENT}
SELECT -10 AS x, fromDaysSinceYearZero(x), 'lower clip, 1970-01-01 min'; -- {serverError ARGUMENT_OUT_OF_BOUND}
SELECT -10 AS x, fromDaysSinceYearZero32(x), 'lower clip, 1970-01-01 min'; -- {serverError ARGUMENT_OUT_OF_BOUND}
SELECT 0 AS x, fromDaysSinceYearZero(x), 'lower clip, 1970-01-01 min'; -- {serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE}
SELECT 719527 AS x, fromDaysSinceYearZero(x), 'lower clip, 1970-01-01 min'; -- {serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE}
SELECT 785064 AS x, fromDaysSinceYearZero(x), 'upper clip, 2149-06-06 max'; -- {serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE}
SELECT 78506444 AS x, fromDaysSinceYearZero(x), 'upper clip, 2149-06-06 max'; -- {serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE}
SELECT 0 AS x, fromDaysSinceYearZero32(x), 'lower clip, 1900-01-01 min'; -- {serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE}
SELECT 693960 AS x, fromDaysSinceYearZero32(x), 'lower clip, 1900-01-01 min'; -- {serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE}
SELECT 840057 AS x, fromDaysSinceYearZero32(x), 'upper clip, 2299-12-31 max'; -- {serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE}
SELECT 840057444 AS x, fromDaysSinceYearZero32(x), 'upper clip, 2299-12-31 max'; -- {serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE}
