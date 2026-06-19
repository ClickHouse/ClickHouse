-- { echo }

SET session_timezone = 'UTC';

-- Error cases: zero args, wrong arg type, three args (one or two args is the only supported overload).

SELECT toRelativeWeekNumExtended();   -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT toRelativeDayNumExtended();    -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT toRelativeHourNumExtended();   -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT toRelativeMinuteNumExtended(); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT toRelativeSecondNumExtended(); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

SELECT toRelativeWeekNumExtended('abc');   -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT toRelativeDayNumExtended('abc');    -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT toRelativeHourNumExtended('abc');   -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT toRelativeMinuteNumExtended('abc'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT toRelativeSecondNumExtended('abc'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT toRelativeWeekNumExtended(toDateTime('2024-01-01', 'UTC'), 'UTC', 'extra');   -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT toRelativeDayNumExtended(toDateTime('2024-01-01', 'UTC'), 'UTC', 'extra');    -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT toRelativeHourNumExtended(toDateTime('2024-01-01', 'UTC'), 'UTC', 'extra');   -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT toRelativeMinuteNumExtended(toDateTime('2024-01-01', 'UTC'), 'UTC', 'extra'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT toRelativeSecondNumExtended(toDateTime('2024-01-01', 'UTC'), 'UTC', 'extra'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

-- Return type is Int64 for every variant; guards against accidental widening or narrowing later.
SELECT toTypeName(toRelativeWeekNumExtended(toDateTime64('2024-01-01', 3, 'UTC'))),
       toTypeName(toRelativeDayNumExtended(toDateTime64('2024-01-01', 3, 'UTC'))),
       toTypeName(toRelativeHourNumExtended(toDateTime64('2024-01-01', 3, 'UTC'))),
       toTypeName(toRelativeMinuteNumExtended(toDateTime64('2024-01-01', 3, 'UTC'))),
       toTypeName(toRelativeSecondNumExtended(toDateTime64('2024-01-01', 3, 'UTC')));

-- Const column at every supported DateTime64 scale (the impl branches on scale_multiplier).
WITH toDateTime64('2019-09-16 19:20:12.345678910', 0, 'Asia/Istanbul') AS dt64
SELECT dt64, toRelativeWeekNumExtended(dt64), toRelativeDayNumExtended(dt64), toRelativeHourNumExtended(dt64), toRelativeMinuteNumExtended(dt64), toRelativeSecondNumExtended(dt64);

WITH toDateTime64('2019-09-16 19:20:12.345678910', 3, 'Asia/Istanbul') AS dt64
SELECT dt64, toRelativeWeekNumExtended(dt64), toRelativeDayNumExtended(dt64), toRelativeHourNumExtended(dt64), toRelativeMinuteNumExtended(dt64), toRelativeSecondNumExtended(dt64);

WITH toDateTime64('2019-09-16 19:20:12.345678910', 6, 'Asia/Istanbul') AS dt64
SELECT dt64, toRelativeWeekNumExtended(dt64), toRelativeDayNumExtended(dt64), toRelativeHourNumExtended(dt64), toRelativeMinuteNumExtended(dt64), toRelativeSecondNumExtended(dt64);

WITH toDateTime64('2019-09-16 19:20:12.345678910', 9, 'Asia/Istanbul') AS dt64
SELECT dt64, toRelativeWeekNumExtended(dt64), toRelativeDayNumExtended(dt64), toRelativeHourNumExtended(dt64), toRelativeMinuteNumExtended(dt64), toRelativeSecondNumExtended(dt64);

-- Same matrix on a non-const (materialized) column: guards against const-folding code
-- paths giving different answers than the column-evaluation path.
WITH toDateTime64('2019-09-16 19:20:12.345678910', 0, 'Asia/Istanbul') AS x
SELECT materialize(x) AS dt64, toRelativeWeekNumExtended(dt64), toRelativeDayNumExtended(dt64), toRelativeHourNumExtended(dt64), toRelativeMinuteNumExtended(dt64), toRelativeSecondNumExtended(dt64);

WITH toDateTime64('2019-09-16 19:20:12.345678910', 3, 'Asia/Istanbul') AS x
SELECT materialize(x) AS dt64, toRelativeWeekNumExtended(dt64), toRelativeDayNumExtended(dt64), toRelativeHourNumExtended(dt64), toRelativeMinuteNumExtended(dt64), toRelativeSecondNumExtended(dt64);

WITH toDateTime64('2019-09-16 19:20:12.345678910', 6, 'Asia/Istanbul') AS x
SELECT materialize(x) AS dt64, toRelativeWeekNumExtended(dt64), toRelativeDayNumExtended(dt64), toRelativeHourNumExtended(dt64), toRelativeMinuteNumExtended(dt64), toRelativeSecondNumExtended(dt64);

WITH toDateTime64('2019-09-16 19:20:12.345678910', 9, 'Asia/Istanbul') AS x
SELECT materialize(x) AS dt64, toRelativeWeekNumExtended(dt64), toRelativeDayNumExtended(dt64), toRelativeHourNumExtended(dt64), toRelativeMinuteNumExtended(dt64), toRelativeSecondNumExtended(dt64);

-- One mid-range example per input type, exercising each `execute(...)` overload of the impl:
-- `UInt16` (Date), `UInt32` (DateTime), `Int32` (Date32), `Int64` (DateTime64).

SELECT toRelativeWeekNumExtended(toDate('2024-01-01')),
       toRelativeDayNumExtended(toDate('2024-01-01')),
       toRelativeHourNumExtended(toDate('2024-01-01')),
       toRelativeMinuteNumExtended(toDate('2024-01-01')),
       toRelativeSecondNumExtended(toDate('2024-01-01'));

SELECT toRelativeWeekNumExtended(toDateTime('2024-01-01 12:34:56', 'UTC')),
       toRelativeDayNumExtended(toDateTime('2024-01-01 12:34:56', 'UTC')),
       toRelativeHourNumExtended(toDateTime('2024-01-01 12:34:56', 'UTC')),
       toRelativeMinuteNumExtended(toDateTime('2024-01-01 12:34:56', 'UTC')),
       toRelativeSecondNumExtended(toDateTime('2024-01-01 12:34:56', 'UTC'));

SELECT toRelativeWeekNumExtended(toDate32('2024-01-01')),
       toRelativeDayNumExtended(toDate32('2024-01-01')),
       toRelativeHourNumExtended(toDate32('2024-01-01')),
       toRelativeMinuteNumExtended(toDate32('2024-01-01')),
       toRelativeSecondNumExtended(toDate32('2024-01-01'));

SELECT toRelativeWeekNumExtended(toDateTime64('2024-01-01 12:34:56.789', 3, 'UTC')),
       toRelativeDayNumExtended(toDateTime64('2024-01-01 12:34:56.789', 3, 'UTC')),
       toRelativeHourNumExtended(toDateTime64('2024-01-01 12:34:56.789', 3, 'UTC')),
       toRelativeMinuteNumExtended(toDateTime64('2024-01-01 12:34:56.789', 3, 'UTC')),
       toRelativeSecondNumExtended(toDateTime64('2024-01-01 12:34:56.789', 3, 'UTC'));

-- Epoch boundary (one second before, at, after) — the narrow `toRelativeSecondNum` would
-- return `UInt32` max - 1 instead of `-1` at one-second-before-epoch.
SELECT toRelativeSecondNumExtended(toDateTime64('1969-12-31 23:59:59', 0, 'UTC')),
       toRelativeSecondNumExtended(toDateTime64('1970-01-01 00:00:00', 0, 'UTC')),
       toRelativeSecondNumExtended(toDateTime64('1970-01-01 00:00:01', 0, 'UTC'));

-- A clearly pre-epoch timestamp: every variant returns the honest signed counter.
SELECT toRelativeWeekNumExtended(toDateTime64('1960-01-01 00:00:00', 0, 'UTC')),
       toRelativeDayNumExtended(toDateTime64('1960-01-01 00:00:00', 0, 'UTC')),
       toRelativeHourNumExtended(toDateTime64('1960-01-01 00:00:00', 0, 'UTC')),
       toRelativeMinuteNumExtended(toDateTime64('1960-01-01 00:00:00', 0, 'UTC')),
       toRelativeSecondNumExtended(toDateTime64('1960-01-01 00:00:00', 0, 'UTC'));

-- The `toRelativeSecondNum` wrap is at `UInt32` max (2106-02-07 06:28:16 UTC).
-- `*NumExtended` keeps growing past it; the narrow version would silently roll over to 0.
SELECT toRelativeSecondNumExtended(toDateTime64('2106-02-07 06:28:15', 0, 'UTC')),
       toRelativeSecondNumExtended(toDateTime64('2106-02-07 06:28:16', 0, 'UTC')),
       toRelativeSecondNumExtended(toDateTime64('2200-01-01 00:00:00', 0, 'UTC'));

-- Documented min/max of each input type, with *all eight* `toRelative*Num` functions:
-- both the new `*NumExtended` (Week/Day/Hour/Minute/Second) and the narrow Year/Quarter/Month
-- counters that have no `Extended` form. Catches "return type can't span the input range"
-- regressions on either family.
WITH toDate('1970-01-01') AS d_min, toDate('2149-06-06') AS d_max
SELECT toRelativeYearNum(d_min),     toRelativeYearNum(d_max),
       toRelativeQuarterNum(d_min),  toRelativeQuarterNum(d_max),
       toRelativeMonthNum(d_min),    toRelativeMonthNum(d_max),
       toRelativeWeekNumExtended(d_min),   toRelativeWeekNumExtended(d_max),
       toRelativeDayNumExtended(d_min),    toRelativeDayNumExtended(d_max),
       toRelativeHourNumExtended(d_min),   toRelativeHourNumExtended(d_max),
       toRelativeMinuteNumExtended(d_min), toRelativeMinuteNumExtended(d_max),
       toRelativeSecondNumExtended(d_min), toRelativeSecondNumExtended(d_max);

WITH toDateTime('1970-01-01 00:00:00', 'UTC') AS dt_min,
     toDateTime('2106-02-07 06:28:15', 'UTC') AS dt_max
SELECT toRelativeYearNum(dt_min),     toRelativeYearNum(dt_max),
       toRelativeQuarterNum(dt_min),  toRelativeQuarterNum(dt_max),
       toRelativeMonthNum(dt_min),    toRelativeMonthNum(dt_max),
       toRelativeWeekNumExtended(dt_min),   toRelativeWeekNumExtended(dt_max),
       toRelativeDayNumExtended(dt_min),    toRelativeDayNumExtended(dt_max),
       toRelativeHourNumExtended(dt_min),   toRelativeHourNumExtended(dt_max),
       toRelativeMinuteNumExtended(dt_min), toRelativeMinuteNumExtended(dt_max),
       toRelativeSecondNumExtended(dt_min), toRelativeSecondNumExtended(dt_max);

WITH toDate32('1900-01-01') AS d32_min, toDate32('2299-12-31') AS d32_max
SELECT toRelativeYearNum(d32_min),     toRelativeYearNum(d32_max),
       toRelativeQuarterNum(d32_min),  toRelativeQuarterNum(d32_max),
       toRelativeMonthNum(d32_min),    toRelativeMonthNum(d32_max),
       toRelativeWeekNumExtended(d32_min),   toRelativeWeekNumExtended(d32_max),
       toRelativeDayNumExtended(d32_min),    toRelativeDayNumExtended(d32_max),
       toRelativeHourNumExtended(d32_min),   toRelativeHourNumExtended(d32_max),
       toRelativeMinuteNumExtended(d32_min), toRelativeMinuteNumExtended(d32_max),
       toRelativeSecondNumExtended(d32_min), toRelativeSecondNumExtended(d32_max);

WITH toDateTime64('1900-01-01 00:00:00.000', 3, 'UTC') AS dt64_min,
     toDateTime64('2299-12-31 23:59:59.999', 3, 'UTC') AS dt64_max
SELECT toRelativeYearNum(dt64_min),     toRelativeYearNum(dt64_max),
       toRelativeQuarterNum(dt64_min),  toRelativeQuarterNum(dt64_max),
       toRelativeMonthNum(dt64_min),    toRelativeMonthNum(dt64_max),
       toRelativeWeekNumExtended(dt64_min),   toRelativeWeekNumExtended(dt64_max),
       toRelativeDayNumExtended(dt64_min),    toRelativeDayNumExtended(dt64_max),
       toRelativeHourNumExtended(dt64_min),   toRelativeHourNumExtended(dt64_max),
       toRelativeMinuteNumExtended(dt64_min), toRelativeMinuteNumExtended(dt64_max),
       toRelativeSecondNumExtended(dt64_min), toRelativeSecondNumExtended(dt64_max);

-- Two-arg form (date + tz string), smoke-tests the variadic argument path.
SELECT toRelativeHourNumExtended(toDateTime('2024-01-01 00:00:00', 'UTC'), 'America/New_York'),
       toRelativeHourNumExtended(toDateTime('2024-01-01 00:00:00', 'UTC'), 'Asia/Tokyo'),
       toRelativeHourNumExtended(toDateTime('2024-01-01 00:00:00', 'UTC'), 'UTC');
