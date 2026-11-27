-- Unix timestamps with microsecond (16 digits) and nanosecond (19 digits) precision
-- must be parsed by the best-effort parser. Previously the parser only knew about
-- 13-digit millisecond timestamps and rejected longer ones with
-- "unexpected number of decimal digits".

-- Exercise toDateTime64 cast in two configurations:
--   1. explicit SETTINGS cast_string_to_date_time_mode = 'best_effort' — pins the parser this PR fixes
--      so the test keeps exercising the right contract even if the default changes later.
--   2. inherited default — pins the assumption that `best_effort` IS the default; a silent default
--      change would flip this section without flipping section 1.
-- Both must agree.
SELECT 'toDateTime64 cast: explicit cast_string_to_date_time_mode = best_effort';
SELECT toDateTime64('1779094968417585', 6, 'UTC') SETTINGS cast_string_to_date_time_mode = 'best_effort';
SELECT toDateTime64('1779094968417585845', 9, 'UTC') SETTINGS cast_string_to_date_time_mode = 'best_effort';

SELECT 'toDateTime64 cast: inherited default (expected to match best_effort)';
SELECT toDateTime64('1779094968417585', 6, 'UTC');
SELECT toDateTime64('1779094968417585845', 9, 'UTC');

SELECT 'parseDateTime64BestEffort';
SELECT parseDateTime64BestEffort('1779094968417585', 6, 'UTC');
SELECT parseDateTime64BestEffort('1779094968417585845', 9, 'UTC');

SELECT 'scale promotion: microsecond digits, nanosecond scale';
SELECT parseDateTime64BestEffort('1779094968417585', 9, 'UTC');

SELECT 'scale truncation: nanosecond digits, microsecond scale';
SELECT parseDateTime64BestEffort('1779094968417585845', 6, 'UTC');

SELECT 'parseDateTime64BestEffortOrNull / OrZero';
SELECT parseDateTime64BestEffortOrNull('1779094968417585845', 9, 'UTC');
SELECT parseDateTime64BestEffortOrZero('1779094968417585845', 9, 'UTC');

SELECT 'non-const';
SELECT parseDateTime64BestEffort(materialize('1779094968417585845'), 9, 'UTC');

SELECT 'parseDateTime64BestEffortUS';
SELECT parseDateTime64BestEffortUS('1779094968417585', 6, 'UTC');
SELECT parseDateTime64BestEffortUS('1779094968417585845', 9, 'UTC');

-- Documented limitation: the subsecond branches assume a 10-digit seconds part,
-- so pre-2001-09-09 subsecond timestamps (12-digit ms, 15-digit us, 18-digit ns)
-- are not handled here and fail with CANNOT_PARSE_DATETIME. Mirrors the existing
-- 13-digit-only ms branch. These assertions pin the current behavior.
SELECT 'pre-2001 subsecond timestamps are not supported (10-digit seconds assumption)';
SELECT parseDateTime64BestEffort('978307200000', 3, 'UTC'); -- {serverError CANNOT_PARSE_DATETIME}
SELECT parseDateTime64BestEffort('978307200000000', 6, 'UTC'); -- {serverError CANNOT_PARSE_DATETIME}
SELECT parseDateTime64BestEffort('978307200000000000', 9, 'UTC'); -- {serverError CANNOT_PARSE_DATETIME}
SELECT parseDateTime64BestEffortOrNull('978307200000000', 6, 'UTC');
SELECT parseDateTime64BestEffortOrZero('978307200000000000', 9, 'UTC');
