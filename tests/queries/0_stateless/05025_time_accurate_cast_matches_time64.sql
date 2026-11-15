-- `Time` and `Time64(0)` hold the same values, so `accurateCast` must accept and reject exactly the
-- same conversions from both: widening an exact `Time` value to `Time64(0)` cannot make a lossy
-- conversion lossless.

-- The `DateTime` results are rendered in the session time zone, so pin it.
SET session_timezone = 'UTC';

SELECT 'accurateCastOrNull to Date';
SELECT toString(accurateCastOrNull(CAST(v, 'Time'), 'Date')), toString(accurateCastOrNull(CAST(v, 'Time64(0)'), 'Date'))
FROM (SELECT arrayJoin([0, 3723, 86400, -1, -86400]) AS v) ORDER BY v;

SELECT 'accurateCastOrNull to Date32';
SELECT toString(accurateCastOrNull(CAST(v, 'Time'), 'Date32')), toString(accurateCastOrNull(CAST(v, 'Time64(0)'), 'Date32'))
FROM (SELECT arrayJoin([0, 3723, 86400, -1, -86400]) AS v) ORDER BY v;

SELECT 'accurateCastOrNull to DateTime';
SELECT toString(accurateCastOrNull(CAST(v, 'Time'), 'DateTime')), toString(accurateCastOrNull(CAST(v, 'Time64(0)'), 'DateTime'))
FROM (SELECT arrayJoin([0, 3723, -1]) AS v) ORDER BY v;

SELECT 'accurateCast throws on a lossy conversion';
SELECT accurateCast(CAST(3723, 'Time'), 'Date'); -- { serverError CANNOT_CONVERT_TYPE }
SELECT accurateCast(CAST(-1, 'Time'), 'DateTime'); -- { serverError CANNOT_CONVERT_TYPE }

SELECT 'accurateCast accepts an exact conversion';
SELECT accurateCast(CAST(86400, 'Time'), 'Date'), accurateCast(CAST(3723, 'Time'), 'DateTime');
