SELECT CAST(1111111111.222 AS DateTime64(3, 'Asia/Istanbul'));
SELECT toDateTime(1111111111.222, 3, 'Asia/Istanbul');
SELECT toDateTime64(1111111111.222, 3, 'Asia/Istanbul');

SELECT toDateTime64(0.0, 9, 'UTC') ;
SELECT toDateTime64(0, 9, 'UTC');

SELECT toDateTime64(-2200000000.0, 9, 'UTC'); -- 1900-01-01 < value
SELECT toDateTime64(-2200000000, 9, 'UTC');

SELECT toDateTime64(-2300000000.0, 9, 'UTC'); -- value < 1900-01-01
SELECT toDateTime64(-2300000000, 9, 'UTC');

SELECT toDateTime64(-999999999999.0, 9, 'UTC'); -- value << 1900-01-01
SELECT toDateTime64(-999999999999, 9, 'UTC');

SELECT toDateTime64(9200000000.0, 9, 'UTC'); -- value < 2262-04-11
SELECT toDateTime64(9200000000, 9, 'UTC');

SELECT toDateTime64(9300000000.0, 9, 'UTC'); -- { serverError DECIMAL_OVERFLOW } # 2262-04-11 < value
SELECT toDateTime64(9300000000, 9, 'UTC'); -- { serverError DECIMAL_OVERFLOW }

