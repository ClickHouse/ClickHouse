-- Test `Time`/`Time64` vs `DateTime` comparison with non-UTC timezones, and
-- `Time64` → `DateTime64` explicit CAST with scale changes (PR #99267).
-- The PR's own test (04037) covers UTC and DateTime64 with non-UTC timezone;
-- this test covers plain `DateTime` with non-UTC timezone and verifies UTC
-- normalisation semantics.

SET allow_experimental_time_time64_type = 1;
SET session_timezone = 'UTC';
SET enable_analyzer = 1;

-- Europe/Moscow is UTC+3 at Unix epoch; toDateTime('1970-01-01 15:45:40', 'Europe/Moscow')
-- = 15:45:40 − 3h = 12:45:40 UTC = 45940s.  Time('12:45:40') = 45940s → equal.
SELECT 'Time vs DateTime with Europe/Moscow timezone';
SELECT CAST('12:45:40', 'Time') = toDateTime('1970-01-01 15:45:40', 'Europe/Moscow');
SELECT CAST('15:45:40', 'Time') = toDateTime('1970-01-01 15:45:40', 'Europe/Moscow');
SELECT CAST('15:45:40', 'Time') > toDateTime('1970-01-01 15:45:40', 'Europe/Moscow');

-- Time64(3) vs DateTime using UTC normalisation
SELECT 'Time64 vs DateTime with Europe/Moscow timezone';
SELECT CAST('12:45:40.000', 'Time64(3)') = toDateTime('1970-01-01 15:45:40', 'Europe/Moscow');
SELECT CAST('12:45:40.001', 'Time64(3)') = toDateTime('1970-01-01 15:45:40', 'Europe/Moscow');
SELECT CAST('12:45:40.001', 'Time64(3)') > toDateTime('1970-01-01 15:45:40', 'Europe/Moscow');

-- Time64 → DateTime64 CAST with different scales
SELECT 'Time64 to DateTime64 CAST with scale change';
SELECT CAST(CAST('14:45:40.123456', 'Time64(6)'), 'DateTime64(3)');
SELECT CAST(CAST('14:45:40.123456', 'Time64(6)'), 'DateTime64(9)');
SELECT CAST(CAST('14:45:40.123', 'Time64(3)'), 'DateTime64(6)');
SELECT CAST(CAST('00:00:00.000', 'Time64(3)'), 'DateTime64(3)');
SELECT CAST(CAST('23:59:59.999', 'Time64(3)'), 'DateTime64(3)');

-- Supertype preserves DateTime timezone when mixed with Time
SELECT 'Supertype preserves DateTime timezone when mixed with Time';
SELECT toTypeName(if(1, CAST('10:00:00', 'Time'), toDateTime('2020-01-01 10:00:00', 'Europe/Moscow')));
SELECT toTypeName(if(1, CAST('10:00:00.000', 'Time64(3)'), toDateTime('2020-01-01 10:00:00', 'Europe/Moscow')));

-- accurateCast: Time64 → DateTime64
SELECT 'accurateCast Time64 to DateTime64';
SELECT accurateCast(CAST('14:45:40.123', 'Time64(3)'), 'DateTime64(6)');
SELECT accurateCastOrNull(CAST('14:45:40.123', 'Time64(3)'), 'DateTime64(6)');
