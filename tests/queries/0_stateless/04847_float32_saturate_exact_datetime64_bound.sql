-- A copy of the scale-dependent `DateTime64` bound in the source floating-point type is rounded: at scale 3
-- the bound 253402300799 rounds to 253402300416 in `Float32`. Saturation must return the exact type boundary,
-- not a clamp computed in the source type (which used to give '9999-12-31 23:53:36' instead of '9999-12-31 23:59:59').

SET session_timezone = 'UTC';

SELECT '-- saturate';
SET date_time_overflow_behavior = 'saturate';
SELECT toDateTime64(1e30::Float32, 3), CAST(1e30::Float32, 'DateTime64(3)');
SELECT toDateTime64(-1e30::Float32, 3);
SELECT toDateTime64(1e30::Float32, 0), toDateTime64(1e30::Float32, 9);
SELECT toTime64(1e30::Float32, 3), toTime64(-1e30::Float32, 3);
SELECT toDateTime64(x, 3) FROM (SELECT materialize(1e30::Float32) AS x);
SELECT toTime64(x, 3) FROM (SELECT materialize(-1e30::Float32) AS x);

SELECT '-- ignore clamps to the same exact boundary';
SET date_time_overflow_behavior = 'ignore';
SELECT toDateTime64(1e30::Float32, 3), toTime64(1e30::Float32, 3);

SELECT '-- throw';
SET date_time_overflow_behavior = 'throw';
SELECT toDateTime64(1e30::Float32, 3); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT toTime64(1e30::Float32, 3); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }

SELECT '-- the exact boundary converts in every mode';
SELECT toDateTime64(253402300799::Float64, 3), toTime64(3599999::Float64, 3);
SET date_time_overflow_behavior = 'saturate';
SELECT toDateTime64(253402300799::Float64, 3), toTime64(3599999::Float64, 3);
