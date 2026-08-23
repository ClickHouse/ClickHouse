SELECT CAST(1111111111.222 AS DateTime64(3, 'Asia/Istanbul'));
SELECT toDateTime(1111111111.222, 3, 'Asia/Istanbul');
SELECT toDateTime64(1111111111.222, 3, 'Asia/Istanbul');

SELECT toDateTime64(0.0, 9, 'UTC') ;
SELECT toDateTime64(0, 9, 'UTC');

SELECT toDateTime64(-2200000000.0, 9, 'UTC'); -- 1900-01-01 < value
SELECT toDateTime64(-2200000000, 9, 'UTC');

SELECT toDateTime64(-2300000000.0, 9, 'UTC'); -- value < 1900-01-01
SELECT toDateTime64(-2300000000, 9, 'UTC');

-- value far below the scale-9 tick range: saturates to the lowest representable tick (ticks are stored in Int64).
-- As on the upper side, the float source saturates to the full sub-second minimum while the integer source keeps
-- the legacy whole-second clamp.
SELECT toDateTime64(-999999999999.0, 9, 'UTC');
SELECT toDateTime64(-999999999999, 9, 'UTC');

SELECT toDateTime64(9200000000.0, 9, 'UTC'); -- value < 2262-04-11
SELECT toDateTime64(9200000000, 9, 'UTC');

-- 2262-04-11 < value: under the default `ignore` behavior the value saturates to the highest representable
-- tick. The float source saturates to the full sub-second maximum, while the integer source keeps the legacy
-- whole-second clamp.
SELECT toDateTime64(9300000000.0, 9, 'UTC');
SELECT toDateTime64(9300000000, 9, 'UTC');

