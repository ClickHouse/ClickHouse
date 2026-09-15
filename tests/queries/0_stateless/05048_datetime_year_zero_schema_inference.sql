SET session_timezone = 'UTC';
SET input_format_try_infer_datetimes = 1;
SET schema_inference_make_columns_nullable = 0;

SELECT toTypeName(c1), c1
FROM format(TSV, '0000-00-00 00:00:00')
SETTINGS date_time_input_format = 'basic';

SELECT toTypeName(c1), c1
FROM format(TSV, '0000-01-00 00:00:00')
SETTINGS date_time_input_format = 'basic';

SELECT toTypeName(c1), c1
FROM format(TSV, '0000-00-01 00:00:00')
SETTINGS date_time_input_format = 'basic';

-- Real year-0 calendar date, not a placeholder. Out of range for DateTime,
-- and DateTime64(9) cannot represent it, so schema inference leaves it as String.
SELECT toTypeName(c1), c1
FROM format(TSV, '0000-01-01 00:00:00')
SETTINGS date_time_input_format = 'basic';

SELECT toTypeName(c1), c1
FROM format(TSV, '0000-00-00 00:00:00')
SETTINGS date_time_input_format = 'best_effort';

SELECT toTypeName(c1), c1
FROM format(TSV, '0000-01-00 00:00:00')
SETTINGS date_time_input_format = 'best_effort';

SELECT toTypeName(c1), c1
FROM format(TSV, '0000-00-01 00:00:00')
SETTINGS date_time_input_format = 'best_effort';

-- Matches the basic parser's behavior, since this is a valid zero-date.
SELECT toTypeName(c1), c1
FROM format(TSV, '0000-01-01 00:00:00')
SETTINGS date_time_input_format = 'best_effort';

SELECT toTypeName(c1), c1
FROM format(TSV, '0000')
SETTINGS date_time_input_format = 'best_effort';

SELECT toTypeName(c1), c1
FROM format(TSV, '0000-05')
SETTINGS date_time_input_format = 'best_effort';
