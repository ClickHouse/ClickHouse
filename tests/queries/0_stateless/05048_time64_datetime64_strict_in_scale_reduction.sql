-- IN uses strict (exact) Field conversion for its set elements. A Time64 element whose
-- fractional part does not survive rescaling to a lower-scale DateTime64 must not match.
SELECT 'scale-reducing, inexact';
SELECT toDateTime64('1970-01-01 00:00:00', 0, 'UTC') IN (CAST('00:00:00.1', 'Time64(1)'));
SELECT toDateTime64('1970-01-01 00:00:00', 0, 'UTC') IN (CAST('00:00:00.9', 'Time64(1)'));
SELECT toDateTime64('1970-01-01 00:00:01', 0, 'UTC') IN (CAST('00:00:01.001', 'Time64(3)'));
SELECT toDateTime64('1970-01-01 00:00:00.10', 2, 'UTC') IN (CAST('00:00:00.123', 'Time64(3)'));
SELECT 'scale-reducing, exact';
SELECT toDateTime64('1970-01-01 00:00:00', 0, 'UTC') IN (CAST('00:00:00.0', 'Time64(1)'));
SELECT toDateTime64('1970-01-01 00:00:01', 0, 'UTC') IN (CAST('00:00:01.000', 'Time64(3)'));
SELECT toDateTime64('1970-01-01 00:00:00.10', 2, 'UTC') IN (CAST('00:00:00.100', 'Time64(3)'));
SELECT 'scale-widening and equal scale';
SELECT toDateTime64('1970-01-01 00:00:00.100', 3, 'UTC') IN (CAST('00:00:00.1', 'Time64(1)'));
SELECT toDateTime64('1970-01-01 00:00:00.1', 1, 'UTC') IN (CAST('00:00:00.1', 'Time64(1)'));
SELECT toDateTime64('1970-01-01 00:00:00.2', 1, 'UTC') IN (CAST('00:00:00.1', 'Time64(1)'));
SELECT 'multiple elements: only the exactly representable one matches';
SELECT toDateTime64('1970-01-01 00:00:02', 0, 'UTC') IN (CAST('00:00:00.5', 'Time64(1)'), CAST('00:00:02.0', 'Time64(1)'));
SELECT toDateTime64('1970-01-01 00:00:00', 0, 'UTC') IN (CAST('00:00:00.5', 'Time64(1)'), CAST('00:00:02.0', 'Time64(1)'));
