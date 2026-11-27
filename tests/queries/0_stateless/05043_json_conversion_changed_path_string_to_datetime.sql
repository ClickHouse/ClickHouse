-- Optimized JSON-to-JSON conversion: a changed typed path from String to DateTime/DateTime64
-- parses with date_time_input_format, like the format+parse path does, so both must agree.
-- cast_string_to_date_time_mode describes the user's own CAST and must not leak into it.

-- date_time_input_format defaults to 'best_effort'.
SELECT '{"dt":"2024-01-15T12:30:00Z"}'::JSON(dt String)::JSON(dt DateTime('UTC')) SETTINGS json_use_optimized_type_conversion = 0;
SELECT '{"dt":"2024-01-15T12:30:00Z"}'::JSON(dt String)::JSON(dt DateTime('UTC')) SETTINGS json_use_optimized_type_conversion = 1;

SELECT '{"dt":"2024-01-15T12:30:00Z"}'::JSON(dt Nullable(String))::JSON(dt DateTime('UTC')) SETTINGS json_use_optimized_type_conversion = 0;
SELECT '{"dt":"2024-01-15T12:30:00Z"}'::JSON(dt Nullable(String))::JSON(dt DateTime('UTC')) SETTINGS json_use_optimized_type_conversion = 1;

SELECT '{"dt":["2024-01-15T12:30:00Z"]}'::JSON(dt Array(String))::JSON(dt Array(DateTime('UTC'))) SETTINGS json_use_optimized_type_conversion = 0;
SELECT '{"dt":["2024-01-15T12:30:00Z"]}'::JSON(dt Array(String))::JSON(dt Array(DateTime('UTC'))) SETTINGS json_use_optimized_type_conversion = 1;

SELECT '{"dt":"2024-01-15T12:30:00Z"}'::JSON(dt String)::JSON(dt DateTime64(3, 'UTC')) SETTINGS json_use_optimized_type_conversion = 0;
SELECT '{"dt":"2024-01-15T12:30:00Z"}'::JSON(dt String)::JSON(dt DateTime64(3, 'UTC')) SETTINGS json_use_optimized_type_conversion = 1;

SET cast_string_to_date_time_mode = 'basic';

SELECT '{"dt":"2024-01-15T12:30:00Z"}'::JSON(dt String)::JSON(dt DateTime('UTC')) SETTINGS json_use_optimized_type_conversion = 0;
SELECT '{"dt":"2024-01-15T12:30:00Z"}'::JSON(dt String)::JSON(dt DateTime('UTC')) SETTINGS json_use_optimized_type_conversion = 1;

SET date_time_input_format = 'basic';

SELECT '{"dt":"2024-01-15 12:30:00"}'::JSON(dt String)::JSON(dt DateTime('UTC')) SETTINGS json_use_optimized_type_conversion = 0;
SELECT '{"dt":"2024-01-15 12:30:00"}'::JSON(dt String)::JSON(dt DateTime('UTC')) SETTINGS json_use_optimized_type_conversion = 1;
