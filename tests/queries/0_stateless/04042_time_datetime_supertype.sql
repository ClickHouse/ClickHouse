-- Test `getLeastSupertype` for `Time`/`Time64` mixed with `DateTime`/`DateTime64`
-- (PR #99267). The existing test 04037 covers `Time` vs `DateTime64` comparisons;
-- this test covers the explicit supertype resolution paths added to
-- `getLeastSupertype.cpp`, including timezone preservation and scale selection.

SET session_timezone = 'UTC';
SET enable_analyzer = 1;
SET use_variant_as_common_type = 0;

-- Time + DateTime → DateTime
SELECT 'Time + DateTime supertype';
SELECT toTypeName(if(1, CAST('10:00:00', 'Time'), toDateTime('2025-01-01 00:00:00')));

-- Time + DateTime64 → DateTime64 with the DateTime64 scale
SELECT 'Time + DateTime64 supertype';
SELECT toTypeName(if(1, CAST('10:00:00', 'Time'), toDateTime64('2025-01-01 00:00:00', 3)));
SELECT toTypeName(if(1, CAST('10:00:00', 'Time'), toDateTime64('2025-01-01 00:00:00', 6)));

-- Time + DateTime64 with timezone → DateTime64 preserves timezone
SELECT 'Time + DateTime64 tz preservation';
SELECT toTypeName(if(1, CAST('10:00:00', 'Time'), toDateTime64('2025-01-01 00:00:00', 3, 'America/New_York')));

-- Date + Time → NO_COMMON_TYPE (incompatible)
SELECT 'Date + Time incompatible';
SELECT if(1, toDate('2025-01-01'), CAST('10:00:00', 'Time')); -- { serverError NO_COMMON_TYPE }
SELECT if(1, toDate32('2025-01-01'), CAST('10:00:00', 'Time')); -- { serverError NO_COMMON_TYPE }
SELECT if(1, toDate('2025-01-01'), CAST('10:00:00.123', 'Time64(3)')); -- { serverError NO_COMMON_TYPE }
SELECT if(1, toDate32('2025-01-01'), CAST('10:00:00.123', 'Time64(3)')); -- { serverError NO_COMMON_TYPE }

-- Time64 with higher scale + DateTime64 with timezone → DateTime64(max_scale, tz)
SELECT 'Time64 higher scale + DateTime64 tz';
SELECT toTypeName(if(1, CAST('10:00:00.123456', 'Time64(6)'), toDateTime64('2025-01-01 00:00:00', 3, 'Europe/Berlin')));

-- Three-way: Time + Time64 + DateTime64 → DateTime64 with maximum scale
SELECT 'Three-way supertype';
SELECT toTypeName([CAST('10:00:00', 'Time'), CAST('10:00:00.123', 'Time64(3)'), toDateTime64('2025-01-01 00:00:00', 6)]);

-- Explicit CAST: Time64 → DateTime64 with scale change
SELECT 'Time64 to DateTime64 CAST';
SELECT CAST(CAST('14:45:40.123', 'Time64(3)'), 'DateTime64(6)');
SELECT CAST(CAST('14:45:40.123456', 'Time64(6)'), 'DateTime64(3)');

-- Value correctness after promotion
SELECT 'Value correctness after promotion';
SELECT if(1, CAST('14:45:40', 'Time'), toDateTime('2025-01-01 00:00:00'));
SELECT if(1, CAST('14:45:40', 'Time'), toDateTime64('2025-01-01 00:00:00', 3));
