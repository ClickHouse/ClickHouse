-- Test that accurateCastOrDefault respects format settings like input_format_try_infer_dates
SELECT 'accurateCastOrDefault with input_format_try_infer_dates=0';
SELECT accurateCastOrDefault('{"d" : "2020-01-01"}', 'JSON') AS json, dynamicType(json.d) SETTINGS input_format_try_infer_dates=0, input_format_try_infer_datetimes=0;

SELECT 'accurateCastOrDefault with input_format_try_infer_dates=1';
SELECT accurateCastOrDefault('{"d" : "2020-01-01"}', 'JSON') AS json, dynamicType(json.d) SETTINGS input_format_try_infer_dates=1;

SELECT 'accurateCastOrNull with input_format_try_infer_dates=0';
SELECT accurateCastOrNull('{"d" : "2020-01-01"}', 'JSON') AS json, dynamicType(json.d) SETTINGS input_format_try_infer_dates=0, input_format_try_infer_datetimes=0;

SELECT 'CAST with input_format_try_infer_dates=0';
SELECT CAST('{"d" : "2020-01-01"}', 'JSON') AS json, dynamicType(json.d) SETTINGS input_format_try_infer_dates=0, input_format_try_infer_datetimes=0;
