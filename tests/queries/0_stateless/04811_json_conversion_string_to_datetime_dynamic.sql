-- Optimized JSON-to-JSON conversion: a String dynamic path converted to a new typed DateTime path
-- parses with date_time_input_format, like the format+parse path does, so both must agree.
-- cast_string_to_date_time_mode describes the user's own CAST and must not leak into it.

SET input_format_try_infer_datetimes = 0;

-- date_time_input_format defaults to 'best_effort'.
SELECT ('{"a":"2024-01-15T12:30:00Z"}'::JSON)::JSON(a DateTime('UTC')) SETTINGS json_use_optimized_type_conversion = 0;
SELECT ('{"a":"2024-01-15T12:30:00Z"}'::JSON)::JSON(a DateTime('UTC')) SETTINGS json_use_optimized_type_conversion = 1;

SET cast_string_to_date_time_mode = 'basic';

SELECT ('{"a":"2024-01-15T12:30:00Z"}'::JSON)::JSON(a DateTime('UTC')) SETTINGS json_use_optimized_type_conversion = 0;
SELECT ('{"a":"2024-01-15T12:30:00Z"}'::JSON)::JSON(a DateTime('UTC')) SETTINGS json_use_optimized_type_conversion = 1;

SET date_time_input_format = 'basic';

SELECT ('{"a":"2024-01-15 12:30:00"}'::JSON)::JSON(a DateTime('UTC')) SETTINGS json_use_optimized_type_conversion = 0;
SELECT ('{"a":"2024-01-15 12:30:00"}'::JSON)::JSON(a DateTime('UTC')) SETTINGS json_use_optimized_type_conversion = 1;
