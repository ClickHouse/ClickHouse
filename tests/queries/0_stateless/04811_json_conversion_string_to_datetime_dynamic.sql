-- Optimized JSON-to-JSON conversion: a String dynamic path converted to a new typed
-- DateTime path must fall back to format+parse when date_time_input_format differs from
-- cast_string_to_date_time_mode (CAST uses the latter, format+parse uses the former).
-- The optimized path must produce the same result as the format+parse path.

SET input_format_try_infer_datetimes = 0;
SET cast_string_to_date_time_mode = 'basic';

-- date_time_input_format defaults to 'best_effort', so it diverges from 'basic' above.
SELECT ('{"a":"2024-01-15T12:30:00Z"}'::JSON)::JSON(a DateTime('UTC')) SETTINGS json_use_optimized_type_conversion = 0;
SELECT ('{"a":"2024-01-15T12:30:00Z"}'::JSON)::JSON(a DateTime('UTC')) SETTINGS json_use_optimized_type_conversion = 1;
