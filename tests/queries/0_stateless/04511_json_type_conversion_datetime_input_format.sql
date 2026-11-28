-- Test: when date_time_input_format differs from cast_string_to_date_time_mode,
-- optimized JSON type conversion must fall back to format+parse for DateTime typed paths.

-- date_time_input_format=best_effort but cast_string_to_date_time_mode=basic:
-- CAST would use basic parsing (fails on ISO), format+parse uses best_effort (succeeds).
SET date_time_input_format = 'best_effort';
SET cast_string_to_date_time_mode = 'basic';
SET session_timezone = 'UTC';

-- Changed typed path: DateTime -> String (should use format+parse due to setting mismatch).
SELECT '-- changed typed path, dt_input != cast_mode';
SELECT '{"dt":"2024-01-15T12:30:00Z"}'::JSON(dt DateTime) AS j, (j::JSON(dt String))."dt" AS dt;

-- Removed typed path: DateTime typed path removed, goes to Dynamic.
SELECT '-- removed typed path, dt_input != cast_mode';
SELECT '{"dt":"2024-01-15T12:30:00Z"}'::JSON(dt DateTime) AS j, (j::JSON)."dt" AS dt;

-- Same test with DateTime64.
SELECT '-- DateTime64 changed typed path, dt_input != cast_mode';
SELECT '{"dt":"2024-01-15T12:30:00.123Z"}'::JSON(dt DateTime64(3)) AS j, (j::JSON(dt String))."dt" AS dt;

-- Control: when both settings match, CAST path works fine.
SET date_time_input_format = 'basic', cast_string_to_date_time_mode = 'basic';
SELECT '-- control: both basic';
SELECT '{"dt":"2024-01-15 12:30:00"}'::JSON(dt DateTime) AS j, (j::JSON(dt String))."dt" AS dt;

-- Both best_effort: should also work fine (no divergence).
SET date_time_input_format = 'best_effort', cast_string_to_date_time_mode = 'best_effort';
SELECT '-- control: both best_effort';
SELECT '{"dt":"2024-01-15T12:30:00Z"}'::JSON(dt DateTime) AS j, (j::JSON(dt String))."dt" AS dt;
