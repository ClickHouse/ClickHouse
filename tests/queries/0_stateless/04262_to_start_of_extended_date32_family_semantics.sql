-- { echo }

SET session_timezone = 'UTC';

-- Error cases: zero args, wrong arg type, too many args.

SELECT toMondayExtended();         -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT toStartOfMonthExtended();   -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT toLastDayOfMonthExtended(); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT toStartOfQuarterExtended(); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT toStartOfYearExtended();    -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT toStartOfWeekExtended();    -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT toLastDayOfWeekExtended();  -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

SELECT toMondayExtended('abc');         -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT toStartOfMonthExtended('abc');   -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT toLastDayOfMonthExtended('abc'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT toStartOfQuarterExtended('abc'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT toStartOfYearExtended('abc');    -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT toStartOfWeekExtended('abc');    -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT toLastDayOfWeekExtended('abc');  -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT toMondayExtended(toDateTime('2024-01-01', 'UTC'), 'UTC', 'extra');         -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT toStartOfMonthExtended(toDateTime('2024-01-01', 'UTC'), 'UTC', 'extra');   -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT toLastDayOfMonthExtended(toDateTime('2024-01-01', 'UTC'), 'UTC', 'extra'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT toStartOfQuarterExtended(toDateTime('2024-01-01', 'UTC'), 'UTC', 'extra'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT toStartOfYearExtended(toDateTime('2024-01-01', 'UTC'), 'UTC', 'extra');    -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT toStartOfWeekExtended(toDateTime('2024-01-01', 'UTC'), 0, 'UTC', 'extra'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT toLastDayOfWeekExtended(toDateTime('2024-01-01', 'UTC'), 0, 'UTC', 'extra'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

-- Return type for every variant; guards against accidental widening or narrowing.
SELECT toTypeName(toMondayExtended(toDateTime64('2024-01-01', 3, 'UTC'))),
       toTypeName(toStartOfMonthExtended(toDateTime64('2024-01-01', 3, 'UTC'))),
       toTypeName(toLastDayOfMonthExtended(toDateTime64('2024-01-01', 3, 'UTC'))),
       toTypeName(toStartOfQuarterExtended(toDateTime64('2024-01-01', 3, 'UTC'))),
       toTypeName(toStartOfYearExtended(toDateTime64('2024-01-01', 3, 'UTC'))),
       toTypeName(toStartOfWeekExtended(toDateTime64('2024-01-01', 3, 'UTC'))),
       toTypeName(toLastDayOfWeekExtended(toDateTime64('2024-01-01', 3, 'UTC')));

-- Const column at every supported DateTime64 scale (the impl branches on scale_multiplier).
WITH toDateTime64('2019-09-16 19:20:12.345678910', 0, 'Asia/Istanbul') AS dt64
SELECT dt64, toMondayExtended(dt64), toStartOfMonthExtended(dt64), toLastDayOfMonthExtended(dt64), toStartOfQuarterExtended(dt64), toStartOfYearExtended(dt64), toStartOfWeekExtended(dt64), toLastDayOfWeekExtended(dt64);

WITH toDateTime64('2019-09-16 19:20:12.345678910', 3, 'Asia/Istanbul') AS dt64
SELECT dt64, toMondayExtended(dt64), toStartOfMonthExtended(dt64), toLastDayOfMonthExtended(dt64), toStartOfQuarterExtended(dt64), toStartOfYearExtended(dt64), toStartOfWeekExtended(dt64), toLastDayOfWeekExtended(dt64);

WITH toDateTime64('2019-09-16 19:20:12.345678910', 6, 'Asia/Istanbul') AS dt64
SELECT dt64, toMondayExtended(dt64), toStartOfMonthExtended(dt64), toLastDayOfMonthExtended(dt64), toStartOfQuarterExtended(dt64), toStartOfYearExtended(dt64), toStartOfWeekExtended(dt64), toLastDayOfWeekExtended(dt64);

WITH toDateTime64('2019-09-16 19:20:12.345678910', 9, 'Asia/Istanbul') AS dt64
SELECT dt64, toMondayExtended(dt64), toStartOfMonthExtended(dt64), toLastDayOfMonthExtended(dt64), toStartOfQuarterExtended(dt64), toStartOfYearExtended(dt64), toStartOfWeekExtended(dt64), toLastDayOfWeekExtended(dt64);

-- Same matrix on a non-const (materialized) column: guards against const-folding code
-- paths giving different answers than the column-evaluation path.
WITH toDateTime64('2019-09-16 19:20:12.345678910', 0, 'Asia/Istanbul') AS x
SELECT materialize(x) AS dt64, toMondayExtended(dt64), toStartOfMonthExtended(dt64), toLastDayOfMonthExtended(dt64), toStartOfQuarterExtended(dt64), toStartOfYearExtended(dt64), toStartOfWeekExtended(dt64), toLastDayOfWeekExtended(dt64);

WITH toDateTime64('2019-09-16 19:20:12.345678910', 3, 'Asia/Istanbul') AS x
SELECT materialize(x) AS dt64, toMondayExtended(dt64), toStartOfMonthExtended(dt64), toLastDayOfMonthExtended(dt64), toStartOfQuarterExtended(dt64), toStartOfYearExtended(dt64), toStartOfWeekExtended(dt64), toLastDayOfWeekExtended(dt64);

WITH toDateTime64('2019-09-16 19:20:12.345678910', 6, 'Asia/Istanbul') AS x
SELECT materialize(x) AS dt64, toMondayExtended(dt64), toStartOfMonthExtended(dt64), toLastDayOfMonthExtended(dt64), toStartOfQuarterExtended(dt64), toStartOfYearExtended(dt64), toStartOfWeekExtended(dt64), toLastDayOfWeekExtended(dt64);

WITH toDateTime64('2019-09-16 19:20:12.345678910', 9, 'Asia/Istanbul') AS x
SELECT materialize(x) AS dt64, toMondayExtended(dt64), toStartOfMonthExtended(dt64), toLastDayOfMonthExtended(dt64), toStartOfQuarterExtended(dt64), toStartOfYearExtended(dt64), toStartOfWeekExtended(dt64), toLastDayOfWeekExtended(dt64);

-- One mid-range example per input type, exercising each execute(...) overload of the impl.

SELECT toMondayExtended(toDate('2024-01-01')),
       toStartOfMonthExtended(toDate('2024-01-01')),
       toLastDayOfMonthExtended(toDate('2024-01-01')),
       toStartOfQuarterExtended(toDate('2024-01-01')),
       toStartOfYearExtended(toDate('2024-01-01')),
       toStartOfWeekExtended(toDate('2024-01-01')),
       toLastDayOfWeekExtended(toDate('2024-01-01'));

SELECT toMondayExtended(toDateTime('2024-01-01 12:34:56', 'UTC')),
       toStartOfMonthExtended(toDateTime('2024-01-01 12:34:56', 'UTC')),
       toLastDayOfMonthExtended(toDateTime('2024-01-01 12:34:56', 'UTC')),
       toStartOfQuarterExtended(toDateTime('2024-01-01 12:34:56', 'UTC')),
       toStartOfYearExtended(toDateTime('2024-01-01 12:34:56', 'UTC')),
       toStartOfWeekExtended(toDateTime('2024-01-01 12:34:56', 'UTC')),
       toLastDayOfWeekExtended(toDateTime('2024-01-01 12:34:56', 'UTC'));

SELECT toMondayExtended(toDate32('2024-01-01')),
       toStartOfMonthExtended(toDate32('2024-01-01')),
       toLastDayOfMonthExtended(toDate32('2024-01-01')),
       toStartOfQuarterExtended(toDate32('2024-01-01')),
       toStartOfYearExtended(toDate32('2024-01-01')),
       toStartOfWeekExtended(toDate32('2024-01-01')),
       toLastDayOfWeekExtended(toDate32('2024-01-01'));

SELECT toMondayExtended(toDateTime64('2024-01-01 12:34:56.789', 3, 'UTC')),
       toStartOfMonthExtended(toDateTime64('2024-01-01 12:34:56.789', 3, 'UTC')),
       toLastDayOfMonthExtended(toDateTime64('2024-01-01 12:34:56.789', 3, 'UTC')),
       toStartOfQuarterExtended(toDateTime64('2024-01-01 12:34:56.789', 3, 'UTC')),
       toStartOfYearExtended(toDateTime64('2024-01-01 12:34:56.789', 3, 'UTC')),
       toStartOfWeekExtended(toDateTime64('2024-01-01 12:34:56.789', 3, 'UTC')),
       toLastDayOfWeekExtended(toDateTime64('2024-01-01 12:34:56.789', 3, 'UTC'));

-- Edge case: pre-epoch input. The narrow versions wrap; the `Extended` variants return
-- honest Date32 values.
SELECT toMondayExtended(toDateTime64('1969-06-15 12:00:00', 0, 'UTC')),
       toStartOfMonthExtended(toDateTime64('1969-06-15 12:00:00', 0, 'UTC')),
       toLastDayOfMonthExtended(toDateTime64('1969-06-15 12:00:00', 0, 'UTC')),
       toStartOfQuarterExtended(toDateTime64('1969-06-15 12:00:00', 0, 'UTC')),
       toStartOfYearExtended(toDateTime64('1969-06-15 12:00:00', 0, 'UTC')),
       toStartOfWeekExtended(toDateTime64('1969-06-15 12:00:00', 0, 'UTC')),
       toLastDayOfWeekExtended(toDateTime64('1969-06-15 12:00:00', 0, 'UTC'));

-- Pre-epoch with NON-ZERO fractional: round-down must produce the predecessor whole-second
-- boundary, not round UP across the floor. Without the `--components.whole` correction in
-- `TransformDateTime64::executeExtendedResult`, these results would all be off by one whole unit.
SELECT toStartOfMonthExtended(toDateTime64('1969-12-31 23:59:59.500', 3, 'UTC')),
       toStartOfYearExtended(toDateTime64('1969-12-31 23:59:59.500', 3, 'UTC')),
       toStartOfQuarterExtended(toDateTime64('1969-12-31 23:59:59.500', 3, 'UTC')),
       toLastDayOfMonthExtended(toDateTime64('1969-12-30 23:59:59.500', 3, 'UTC')),
       toMondayExtended(toDateTime64('1969-12-15 23:59:59.500', 3, 'UTC')),
       toStartOfWeekExtended(toDateTime64('1969-12-15 23:59:59.500', 3, 'UTC')),
       toLastDayOfWeekExtended(toDateTime64('1969-12-15 23:59:59.500', 3, 'UTC'));

-- Edge case: far-future `Date32` past 2149-06-06 (the narrow `Date` upper bound).
SELECT toMondayExtended(toDate32('2200-06-15')),
       toStartOfMonthExtended(toDate32('2200-06-15')),
       toLastDayOfMonthExtended(toDate32('2200-06-15')),
       toStartOfQuarterExtended(toDate32('2200-06-15')),
       toStartOfYearExtended(toDate32('2200-06-15')),
       toStartOfWeekExtended(toDate32('2200-06-15')),
       toLastDayOfWeekExtended(toDate32('2200-06-15'));

-- Documented min/max of each input type with all seven new `*Extended` variants.
-- Catches "return type can't span the input range" regressions.
WITH toDate('1970-01-01') AS d_min, toDate('2149-06-06') AS d_max
SELECT toMondayExtended(d_min),         toMondayExtended(d_max),
       toStartOfMonthExtended(d_min),   toStartOfMonthExtended(d_max),
       toLastDayOfMonthExtended(d_min), toLastDayOfMonthExtended(d_max),
       toStartOfQuarterExtended(d_min), toStartOfQuarterExtended(d_max),
       toStartOfYearExtended(d_min),    toStartOfYearExtended(d_max),
       toStartOfWeekExtended(d_min),    toStartOfWeekExtended(d_max),
       toLastDayOfWeekExtended(d_min),  toLastDayOfWeekExtended(d_max);

WITH toDateTime('1970-01-01 00:00:00', 'UTC') AS dt_min,
     toDateTime('2106-02-07 06:28:15', 'UTC') AS dt_max
SELECT toMondayExtended(dt_min),         toMondayExtended(dt_max),
       toStartOfMonthExtended(dt_min),   toStartOfMonthExtended(dt_max),
       toLastDayOfMonthExtended(dt_min), toLastDayOfMonthExtended(dt_max),
       toStartOfQuarterExtended(dt_min), toStartOfQuarterExtended(dt_max),
       toStartOfYearExtended(dt_min),    toStartOfYearExtended(dt_max),
       toStartOfWeekExtended(dt_min),    toStartOfWeekExtended(dt_max),
       toLastDayOfWeekExtended(dt_min),  toLastDayOfWeekExtended(dt_max);

WITH toDate32('1900-01-01') AS d32_min, toDate32('2299-12-31') AS d32_max
SELECT toMondayExtended(d32_min),         toMondayExtended(d32_max),
       toStartOfMonthExtended(d32_min),   toStartOfMonthExtended(d32_max),
       toLastDayOfMonthExtended(d32_min), toLastDayOfMonthExtended(d32_max),
       toStartOfQuarterExtended(d32_min), toStartOfQuarterExtended(d32_max),
       toStartOfYearExtended(d32_min),    toStartOfYearExtended(d32_max),
       toStartOfWeekExtended(d32_min),    toStartOfWeekExtended(d32_max),
       toLastDayOfWeekExtended(d32_min),  toLastDayOfWeekExtended(d32_max);

WITH toDateTime64('1900-01-01 00:00:00.000', 3, 'UTC') AS dt64_min,
     toDateTime64('2299-12-31 23:59:59.999', 3, 'UTC') AS dt64_max
SELECT toMondayExtended(dt64_min),         toMondayExtended(dt64_max),
       toStartOfMonthExtended(dt64_min),   toStartOfMonthExtended(dt64_max),
       toLastDayOfMonthExtended(dt64_min), toLastDayOfMonthExtended(dt64_max),
       toStartOfQuarterExtended(dt64_min), toStartOfQuarterExtended(dt64_max),
       toStartOfYearExtended(dt64_min),    toStartOfYearExtended(dt64_max),
       toStartOfWeekExtended(dt64_min),    toStartOfWeekExtended(dt64_max),
       toLastDayOfWeekExtended(dt64_min),  toLastDayOfWeekExtended(dt64_max);

-- Two-arg form (date + tz string) for `toStartOfMonthExtended`, smoke-tests the variadic path.
SELECT toStartOfMonthExtended(toDateTime('2024-01-01 00:00:00', 'UTC'), 'America/New_York'),
       toStartOfMonthExtended(toDateTime('2024-01-01 00:00:00', 'UTC'), 'Asia/Tokyo'),
       toStartOfMonthExtended(toDateTime('2024-01-01 00:00:00', 'UTC'), 'UTC');
