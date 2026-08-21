-- Test that unix timestamps with microsecond and nanosecond precision
-- work in best_effort mode, not just in basic mode.

SET session_timezone = 'UTC';
SET cast_string_to_date_time_mode = 'basic';
SELECT 'basic milli:', toDateTime64('1776858622333', 3);
SELECT 'basic micro:', toDateTime64('1776858622333666', 6);
SELECT 'basic nano:', toDateTime64('1776858622333666999', 9);

SET cast_string_to_date_time_mode = 'best_effort';
SELECT 'best_effort milli:', toDateTime64('1776858622333', 3);
SELECT 'best_effort micro:', toDateTime64('1776858622333666', 6);
SELECT 'best_effort nano:', toDateTime64('1776858622333666999', 9);

-- Verify both modes produce identical results
SELECT 'match micro:', (
    SELECT toDateTime64('1776858622333666', 6) SETTINGS cast_string_to_date_time_mode = 'basic'
) = (
    SELECT toDateTime64('1776858622333666', 6) SETTINGS cast_string_to_date_time_mode = 'best_effort'
);

SELECT 'match nano:', (
    SELECT toDateTime64('1776858622333666999', 9) SETTINGS cast_string_to_date_time_mode = 'basic'
) = (
    SELECT toDateTime64('1776858622333666999', 9) SETTINGS cast_string_to_date_time_mode = 'best_effort'
);
