-- date_time_overflow_behavior was ignored when the value came from text instead of a typed column
SET session_timezone = 'UTC';

SELECT 'saturate';
SET date_time_overflow_behavior = 'saturate';
SELECT toDate('9999-12-31'), toDate('1969-12-31'), toDateTime('2106-02-07 06:28:16'), toDateTime('1969-12-31 23:59:59');
SELECT CAST('9999-12-31' AS Date), CAST(materialize('9999-12-31') AS Date), CAST('9999-12-31'::FixedString(10) AS Date);
SELECT toDateOrNull('9999-12-31'), toDateOrZero('9999-12-31');

SELECT 'ignore';
SET date_time_overflow_behavior = 'ignore';
SELECT toDate('9999-12-31'), toDate('1969-12-31'), toDateTime('2106-02-07 06:28:16'), toDateTime('1969-12-31 23:59:59');

SELECT 'throw';
SET date_time_overflow_behavior = 'throw';
SELECT toDate('2149-06-07'); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT toDate('1969-12-31'); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT toDateTime('2106-02-07 06:28:16'); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT toDateTime('1969-12-31 23:59:59'); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT CAST('2149-06-07' AS Date); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT CAST(materialize('2149-06-07') AS Date); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT CAST('2149-06-07'::FixedString(10) AS Date); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT CAST(materialize('2106-02-07 06:28:16') AS DateTime); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }

SELECT 'throw, in range';
SELECT toDate('2149-06-06'), toDate('1970-01-01'), toDateTime('2106-02-07 06:28:15'), toDateTime('1970-01-01 00:00:00');
-- Date32 and DateTime64 are not covered yet: a high-scale DateTime64 text parse still raises DECIMAL_OVERFLOW
-- in every mode, because the tick range is only checked in DecimalUtils
SELECT toDate32('2299-12-31'), toDate32('1900-01-01'), toDateTime64('2299-12-31 23:59:59.999', 3);

SELECT 'throw, OrNull and OrZero still fall back';
SELECT toDateOrNull('2149-06-07'), toDateOrZero('2149-06-07'), toDateTimeOrNull('2106-02-07 06:28:16'), toDateTimeOrZero('1969-12-31 23:59:59');

SELECT 'throw, input formats';
SELECT * FROM format(CSV, 'v Date', '2150-12-31'); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT * FROM format(CSV, 'v Date', '1960-01-01'); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT * FROM format(TSV, 'v Date', '2150-12-31'); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT * FROM format(JSONEachRow, 'v Date', '{"v":"2150-12-31"}'); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT * FROM format(CSV, 'v DateTime', '2106-02-07 06:28:16'); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT * FROM format(TSV, 'v DateTime', '1960-01-01 00:00:00'); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT * FROM format(JSONEachRow, 'v DateTime', '{"v":"2106-02-07 06:28:16"}'); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT * FROM format(CSV, 'v Date', '2149-06-06');
SELECT * FROM format(TSV, 'v DateTime', '2106-02-07 06:28:15');
