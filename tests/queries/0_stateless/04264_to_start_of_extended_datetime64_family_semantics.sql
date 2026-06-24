-- { echo }

SET session_timezone = 'UTC';

-- Error cases: zero args, wrong arg type, three args.

SELECT toStartOfDayExtended();             -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT toStartOfHourExtended();            -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT toStartOfMinuteExtended();          -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT toStartOfFiveMinutesExtended();     -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT toStartOfTenMinutesExtended();      -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT toStartOfFifteenMinutesExtended();  -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

SELECT toStartOfDayExtended('abc');             -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT toStartOfHourExtended('abc');            -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT toStartOfMinuteExtended('abc');          -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT toStartOfFiveMinutesExtended('abc');     -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT toStartOfTenMinutesExtended('abc');      -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT toStartOfFifteenMinutesExtended('abc');  -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT toStartOfDayExtended(toDateTime('2024-01-01', 'UTC'), 'UTC', 'extra');             -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT toStartOfHourExtended(toDateTime('2024-01-01', 'UTC'), 'UTC', 'extra');            -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT toStartOfMinuteExtended(toDateTime('2024-01-01', 'UTC'), 'UTC', 'extra');          -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT toStartOfFiveMinutesExtended(toDateTime('2024-01-01', 'UTC'), 'UTC', 'extra');     -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT toStartOfTenMinutesExtended(toDateTime('2024-01-01', 'UTC'), 'UTC', 'extra');      -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT toStartOfFifteenMinutesExtended(toDateTime('2024-01-01', 'UTC'), 'UTC', 'extra');  -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

-- `Date` / `Date32` inputs are rejected during analysis: the sub-day `*Extended` functions have no
-- sub-day component for a date-only argument, and the exception names the `*Extended` function.
SELECT toStartOfHourExtended(toDate('2024-01-01'));         -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT toStartOfMinuteExtended(toDate32('2024-01-01'));     -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT toStartOfFiveMinutesExtended(toDate('2024-01-01'));  -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT toStartOfTenMinutesExtended(toDate32('2024-01-01')); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT toStartOfFifteenMinutesExtended(toDate('2024-01-01'));     -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- Return type is `DateTime64` when the input is `DateTime64` (the wide-result path), `DateTime`
-- for `DateTime` input (no wrap risk in 1970-2106 range). `Date`/`Date32` inputs throw at execute
-- time for the sub-second family (only `toStartOfDayExtended` accepts them).
SELECT toTypeName(toStartOfDayExtended(toDateTime64('2024-01-01', 3, 'UTC'))),
       toTypeName(toStartOfHourExtended(toDateTime64('2024-01-01', 3, 'UTC'))),
       toTypeName(toStartOfMinuteExtended(toDateTime64('2024-01-01', 3, 'UTC'))),
       toTypeName(toStartOfFiveMinutesExtended(toDateTime64('2024-01-01', 3, 'UTC'))),
       toTypeName(toStartOfTenMinutesExtended(toDateTime64('2024-01-01', 3, 'UTC'))),
       toTypeName(toStartOfFifteenMinutesExtended(toDateTime64('2024-01-01', 3, 'UTC')));

-- Const column at every supported DateTime64 scale.
WITH toDateTime64('2019-09-16 19:20:12.345678910', 0, 'Asia/Istanbul') AS dt64
SELECT dt64, toStartOfDayExtended(dt64), toStartOfHourExtended(dt64), toStartOfMinuteExtended(dt64), toStartOfFiveMinutesExtended(dt64), toStartOfTenMinutesExtended(dt64), toStartOfFifteenMinutesExtended(dt64);

WITH toDateTime64('2019-09-16 19:20:12.345678910', 3, 'Asia/Istanbul') AS dt64
SELECT dt64, toStartOfDayExtended(dt64), toStartOfHourExtended(dt64), toStartOfMinuteExtended(dt64), toStartOfFiveMinutesExtended(dt64), toStartOfTenMinutesExtended(dt64), toStartOfFifteenMinutesExtended(dt64);

WITH toDateTime64('2019-09-16 19:20:12.345678910', 6, 'Asia/Istanbul') AS dt64
SELECT dt64, toStartOfDayExtended(dt64), toStartOfHourExtended(dt64), toStartOfMinuteExtended(dt64), toStartOfFiveMinutesExtended(dt64), toStartOfTenMinutesExtended(dt64), toStartOfFifteenMinutesExtended(dt64);

WITH toDateTime64('2019-09-16 19:20:12.345678910', 9, 'Asia/Istanbul') AS dt64
SELECT dt64, toStartOfDayExtended(dt64), toStartOfHourExtended(dt64), toStartOfMinuteExtended(dt64), toStartOfFiveMinutesExtended(dt64), toStartOfTenMinutesExtended(dt64), toStartOfFifteenMinutesExtended(dt64);

-- Same matrix on a non-const (materialized) column.
WITH toDateTime64('2019-09-16 19:20:12.345678910', 0, 'Asia/Istanbul') AS x
SELECT materialize(x) AS dt64, toStartOfDayExtended(dt64), toStartOfHourExtended(dt64), toStartOfMinuteExtended(dt64), toStartOfFiveMinutesExtended(dt64), toStartOfTenMinutesExtended(dt64), toStartOfFifteenMinutesExtended(dt64);

WITH toDateTime64('2019-09-16 19:20:12.345678910', 3, 'Asia/Istanbul') AS x
SELECT materialize(x) AS dt64, toStartOfDayExtended(dt64), toStartOfHourExtended(dt64), toStartOfMinuteExtended(dt64), toStartOfFiveMinutesExtended(dt64), toStartOfTenMinutesExtended(dt64), toStartOfFifteenMinutesExtended(dt64);

WITH toDateTime64('2019-09-16 19:20:12.345678910', 6, 'Asia/Istanbul') AS x
SELECT materialize(x) AS dt64, toStartOfDayExtended(dt64), toStartOfHourExtended(dt64), toStartOfMinuteExtended(dt64), toStartOfFiveMinutesExtended(dt64), toStartOfTenMinutesExtended(dt64), toStartOfFifteenMinutesExtended(dt64);

WITH toDateTime64('2019-09-16 19:20:12.345678910', 9, 'Asia/Istanbul') AS x
SELECT materialize(x) AS dt64, toStartOfDayExtended(dt64), toStartOfHourExtended(dt64), toStartOfMinuteExtended(dt64), toStartOfFiveMinutesExtended(dt64), toStartOfTenMinutesExtended(dt64), toStartOfFifteenMinutesExtended(dt64);

-- One mid-range example per input type. Only `toStartOfDayExtended` accepts `Date`/`Date32`;
-- the others require a time-bearing input, so we skip those rows on the date-only inputs.

SELECT toStartOfDayExtended(toDate('2024-01-01'));
SELECT toStartOfDayExtended(toDate32('2024-01-01'));

SELECT toStartOfDayExtended(toDateTime('2024-01-01 12:34:56', 'UTC')),
       toStartOfHourExtended(toDateTime('2024-01-01 12:34:56', 'UTC')),
       toStartOfMinuteExtended(toDateTime('2024-01-01 12:34:56', 'UTC')),
       toStartOfFiveMinutesExtended(toDateTime('2024-01-01 12:34:56', 'UTC')),
       toStartOfTenMinutesExtended(toDateTime('2024-01-01 12:34:56', 'UTC')),
       toStartOfFifteenMinutesExtended(toDateTime('2024-01-01 12:34:56', 'UTC'));

SELECT toStartOfDayExtended(toDateTime64('2024-01-01 12:34:56.789', 3, 'UTC')),
       toStartOfHourExtended(toDateTime64('2024-01-01 12:34:56.789', 3, 'UTC')),
       toStartOfMinuteExtended(toDateTime64('2024-01-01 12:34:56.789', 3, 'UTC')),
       toStartOfFiveMinutesExtended(toDateTime64('2024-01-01 12:34:56.789', 3, 'UTC')),
       toStartOfTenMinutesExtended(toDateTime64('2024-01-01 12:34:56.789', 3, 'UTC')),
       toStartOfFifteenMinutesExtended(toDateTime64('2024-01-01 12:34:56.789', 3, 'UTC'));

-- Edge cases: pre-epoch (narrow versions wrap; Extended returns honest DateTime64).
SELECT toStartOfDayExtended(toDateTime64('1969-12-31 22:00:00', 0, 'UTC')),
       toStartOfHourExtended(toDateTime64('1969-12-31 22:30:00', 0, 'UTC')),
       toStartOfMinuteExtended(toDateTime64('1969-12-31 23:59:30', 0, 'UTC')),
       toStartOfFiveMinutesExtended(toDateTime64('1969-12-31 23:57:30', 0, 'UTC')),
       toStartOfTenMinutesExtended(toDateTime64('1969-12-31 23:55:30', 0, 'UTC')),
       toStartOfFifteenMinutesExtended(toDateTime64('1969-12-31 23:50:30', 0, 'UTC'));

-- Edge cases: pre-epoch with NON-ZERO FRACTIONAL part. Round-down should produce the predecessor
-- whole-second boundary; without the `--components.whole` correction in `TransformDateTime64`,
-- this rounds UP instead of DOWN. Lock that bug class in place.
SELECT toStartOfDayExtended(toDateTime64('1969-12-30 23:59:59.500', 3, 'UTC')),
       toStartOfHourExtended(toDateTime64('1969-12-31 22:59:59.500', 3, 'UTC')),
       toStartOfMinuteExtended(toDateTime64('1969-12-31 23:59:59.500', 3, 'UTC')),
       toStartOfFiveMinutesExtended(toDateTime64('1969-12-31 23:54:59.500', 3, 'UTC')),
       toStartOfTenMinutesExtended(toDateTime64('1969-12-31 23:49:59.500', 3, 'UTC')),
       toStartOfFifteenMinutesExtended(toDateTime64('1969-12-31 23:44:59.500', 3, 'UTC'));

-- Edge cases: post-2106 (the narrow UInt32-seconds wrap point).
SELECT toStartOfDayExtended(toDateTime64('2106-02-07 06:00:00', 0, 'UTC')),
       toStartOfHourExtended(toDateTime64('2106-02-07 06:28:15', 0, 'UTC')),
       toStartOfMinuteExtended(toDateTime64('2106-02-07 06:28:16', 0, 'UTC')),
       toStartOfFiveMinutesExtended(toDateTime64('2200-01-01 00:00:00', 0, 'UTC')),
       toStartOfTenMinutesExtended(toDateTime64('2200-01-01 00:00:00', 0, 'UTC')),
       toStartOfFifteenMinutesExtended(toDateTime64('2200-01-01 00:00:00', 0, 'UTC'));

-- Documented min/max per input type, exercising every variant where the input is supported.

WITH toDateTime('1970-01-01 00:00:00', 'UTC') AS dt_min,
     toDateTime('2106-02-07 06:28:15', 'UTC') AS dt_max
SELECT toStartOfDayExtended(dt_min),            toStartOfDayExtended(dt_max),
       toStartOfHourExtended(dt_min),           toStartOfHourExtended(dt_max),
       toStartOfMinuteExtended(dt_min),         toStartOfMinuteExtended(dt_max),
       toStartOfFiveMinutesExtended(dt_min),    toStartOfFiveMinutesExtended(dt_max),
       toStartOfTenMinutesExtended(dt_min),     toStartOfTenMinutesExtended(dt_max),
       toStartOfFifteenMinutesExtended(dt_min), toStartOfFifteenMinutesExtended(dt_max);

WITH toDateTime64('1900-01-01 00:00:00.000', 3, 'UTC') AS dt64_min,
     toDateTime64('2299-12-31 23:59:59.999', 3, 'UTC') AS dt64_max
SELECT toStartOfDayExtended(dt64_min),            toStartOfDayExtended(dt64_max),
       toStartOfHourExtended(dt64_min),           toStartOfHourExtended(dt64_max),
       toStartOfMinuteExtended(dt64_min),         toStartOfMinuteExtended(dt64_max),
       toStartOfFiveMinutesExtended(dt64_min),    toStartOfFiveMinutesExtended(dt64_max),
       toStartOfTenMinutesExtended(dt64_min),     toStartOfTenMinutesExtended(dt64_max),
       toStartOfFifteenMinutesExtended(dt64_min), toStartOfFifteenMinutesExtended(dt64_max);

-- Two-arg form (datetime + tz string) for `toStartOfHourExtended`, smoke-tests the variadic path.
SELECT toStartOfHourExtended(toDateTime('2024-01-01 00:00:00', 'UTC'), 'America/New_York'),
       toStartOfHourExtended(toDateTime('2024-01-01 00:00:00', 'UTC'), 'Asia/Tokyo'),
       toStartOfHourExtended(toDateTime('2024-01-01 00:00:00', 'UTC'), 'UTC');
