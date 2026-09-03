-- { echo }

SET session_timezone='UTC';

SELECT '2025-12-07T05:17:47.563383Z'::DateTime64 = '2025-12-07T05:17:47.563383Z'
SETTINGS cast_string_to_date_time_mode='best_effort';

SELECT '2025-12-07T05:17:47.563383Z' = '2025-12-07T05:17:47.563383Z'::DateTime64
SETTINGS cast_string_to_date_time_mode='best_effort';

SELECT '2025-12-07T05:17:47Z'::DateTime < '2025-12-07T05:17:48Z'
SETTINGS cast_string_to_date_time_mode='best_effort';

SELECT '2025-12-07T05:17:47.563383Z'::DateTime64(6) = '2025-12-07T05:17:47.563383Z'
SETTINGS cast_string_to_date_time_mode='best_effort';

SELECT '01/02/2025 20:00:00Z'::DateTime = '01/02/2025 20:00:00Z'
SETTINGS cast_string_to_date_time_mode='best_effort_us';

SELECT '2025-12-07T05:17:47.563383Z'::DateTime64 = '2025-12-07T05:17:47.563383Z'
SETTINGS cast_string_to_date_time_mode='basic'; -- { serverError CANNOT_PARSE_TEXT }

SELECT CAST(1 AS Bool) = 'yess'; -- { serverError CANNOT_PARSE_BOOL }

SELECT CAST(1 AS Bool) = 'yess' SETTINGS bool_true_representation='yess', bool_false_representation='noo';

SELECT CAST(1 AS Bool) = 'yes' SETTINGS bool_true_representation='yess', bool_false_representation='noo';

SELECT CAST(0 AS Bool) = 'noo' SETTINGS bool_true_representation='yess', bool_false_representation='noo';

SELECT CAST(0 AS Bool) = 'no' SETTINGS bool_true_representation='yess', bool_false_representation='noo';

SELECT tuple(1, 0) = '(1, NULL)' SETTINGS input_format_null_as_default=0; -- { serverError CANNOT_PARSE_INPUT_ASSERTION_FAILED }

SELECT tuple(1, 0) = '(1, NULL)' SETTINGS input_format_null_as_default=1;
