-- A NaN converted to `DateTime64` or `Time64` fails in every `date_time_overflow_behavior` mode.

SET enable_time_time64_type = 1;

SELECT CAST(nan AS DateTime64(3, 'UTC')); -- { serverError DECIMAL_OVERFLOW }
SELECT toDateTime64(materialize(nan::Float32), 9, 'UTC') SETTINGS date_time_overflow_behavior = 'saturate'; -- { serverError DECIMAL_OVERFLOW }
SELECT CAST(nan::BFloat16 AS DateTime64(0, 'UTC')) SETTINGS date_time_overflow_behavior = 'throw'; -- { serverError DECIMAL_OVERFLOW }
SELECT CAST(nan AS Time64(3)); -- { serverError DECIMAL_OVERFLOW }
SELECT toTime64(materialize(nan::Float32), 6) SETTINGS date_time_overflow_behavior = 'saturate'; -- { serverError DECIMAL_OVERFLOW }
SELECT CAST(nan::BFloat16 AS Time64(1)) SETTINGS date_time_overflow_behavior = 'throw'; -- { serverError DECIMAL_OVERFLOW }
SELECT toDateTime64(x, 3, 'UTC') FROM (SELECT arrayJoin([1.5, nan]) AS x); -- { serverError DECIMAL_OVERFLOW }
SELECT CAST(1.5 AS DateTime64(3, 'UTC')), CAST(-1.5 AS Time64(3));
