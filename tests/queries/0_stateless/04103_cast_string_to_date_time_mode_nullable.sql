-- Tags: no-fasttest
-- Test that cast_string_to_date_time_mode setting works with Nullable(DateTime) CAST
-- https://github.com/ClickHouse/ClickHouse/issues/101840

SET session_timezone = 'UTC';

-- best_effort mode: non-Nullable (baseline)
SELECT CAST('2020-02-01T20:00:00Z' AS DateTime) SETTINGS cast_string_to_date_time_mode = 'best_effort';
-- best_effort mode: Nullable should also work (was returning NULL before the fix)
SELECT CAST('2020-02-01T20:00:00Z' AS Nullable(DateTime)) SETTINGS cast_string_to_date_time_mode = 'best_effort';

-- best_effort_us mode: non-Nullable (baseline)
SELECT CAST('01/02/2020 20:00:00' AS DateTime) SETTINGS cast_string_to_date_time_mode = 'best_effort_us';
-- best_effort_us mode: Nullable should also work (was returning NULL before the fix)
SELECT CAST('01/02/2020 20:00:00' AS Nullable(DateTime)) SETTINGS cast_string_to_date_time_mode = 'best_effort_us';

-- Invalid strings should still return NULL for Nullable (not throw)
SELECT CAST('not_a_date' AS Nullable(DateTime)) SETTINGS cast_string_to_date_time_mode = 'best_effort';
SELECT CAST('not_a_date' AS Nullable(DateTime)) SETTINGS cast_string_to_date_time_mode = 'best_effort_us';

-- Basic mode should still work as before
SELECT CAST('2020-02-01 20:00:00' AS Nullable(DateTime)) SETTINGS cast_string_to_date_time_mode = 'basic';
