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
SETTINGS cast_string_to_date_time_mode='basic'; -- {serverError CANNOT_PARSE_TEXT}
