-- Accessing the json.d subcolumn requires the analyzer.
SET enable_analyzer = 1;

-- Test that accurateCastOrDefault respects format settings like input_format_try_infer_dates
SELECT 'accurateCastOrDefault with input_format_try_infer_dates=0';
SELECT accurateCastOrDefault('{"d" : "2020-01-01"}', 'JSON') AS json, dynamicType(json.d) SETTINGS input_format_try_infer_dates=0, input_format_try_infer_datetimes=0;

SELECT 'accurateCastOrDefault with input_format_try_infer_dates=1';
SELECT accurateCastOrDefault('{"d" : "2020-01-01"}', 'JSON') AS json, dynamicType(json.d) SETTINGS input_format_try_infer_dates=1;

SELECT 'accurateCastOrNull with input_format_try_infer_dates=0';
SELECT accurateCastOrNull('{"d" : "2020-01-01"}', 'JSON') AS json, dynamicType(json.d) SETTINGS input_format_try_infer_dates=0, input_format_try_infer_datetimes=0;

SELECT 'CAST with input_format_try_infer_dates=0';
SELECT CAST('{"d" : "2020-01-01"}', 'JSON') AS json, dynamicType(json.d) SETTINGS input_format_try_infer_dates=0, input_format_try_infer_datetimes=0;

-- Test that accurateCastOrDefault applies timezone substitution from DateTime source
SELECT 'accurateCastOrDefault timezone substitution';
SELECT toTypeName(accurateCastOrDefault(toDateTime('2020-01-01', 'Europe/Moscow'), 'DateTime'));
SELECT toTypeName(accurateCastOrDefault(toDateTime('2020-01-01', 'Europe/Moscow'), 'DateTime64'));

-- Test that accurateCastOrDefault respects DataTypeValidationSettings (forbidden types)
SELECT accurateCastOrDefault('hello', 'FixedString(1000)') SETTINGS allow_suspicious_fixed_string_types=0; -- { serverError ILLEGAL_COLUMN }
SELECT length(accurateCastOrDefault('hello', 'FixedString(1000)')) SETTINGS allow_suspicious_fixed_string_types=1;
