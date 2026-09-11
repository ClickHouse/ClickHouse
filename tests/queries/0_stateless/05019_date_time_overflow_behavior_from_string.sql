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

SELECT 'throw, tentative parsers must not accept a clamped value';
SELECT v, variantElement(v, 'Date') AS d, variantElement(v, 'String') AS s
FROM format(CSV, 'v Variant(Date, String)', '2150-12-31') SETTINGS allow_experimental_variant_type = 1;
SELECT v FROM format(CSV, 'v Variant(Date, String)', '2149-06-06') SETTINGS allow_experimental_variant_type = 1;

SELECT 'throw, a digit-only timestamp is text too';
SELECT toDateTime('4294967296'); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT CAST('4294967296' AS DateTime); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT CAST(materialize('4294967296') AS DateTime); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT * FROM format(JSONEachRow, 'v DateTime', '{"v":"4294967296"}'); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT * FROM format(Values, 'v DateTime', '(\'4294967296\')'); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT toDateTime('4294967295'), toDateTime('1700000000'), toDateTimeOrNull('4294967296'), toDateTimeOrZero('4294967296');

SELECT 'throw, Date32 rejects what it cannot represent instead of substituting a default';
SELECT toDate32('2000-13-01'); -- { serverError CANNOT_PARSE_DATE }
SELECT toDate32('99999999'); -- { serverError CANNOT_PARSE_DATE }
SELECT CAST(materialize('2000-13-01') AS Date32); -- { serverError CANNOT_PARSE_DATE }
SELECT * FROM format(CSV, 'v Date32', '2000-13-01'); -- { serverError CANNOT_PARSE_DATE }
SELECT * FROM format(TSV, 'v Date32', '2000-13-01'); -- { serverError CANNOT_PARSE_DATE }
SELECT * FROM format(JSONEachRow, 'v Date32', '{"v":"2000-13-01"}'); -- { serverError CANNOT_PARSE_DATE }
SELECT toDate32OrNull('2000-13-01'), toDate32('2299-12-31'), toDate32('1900-01-01');
SELECT v FROM format(CSV, 'v Variant(Date32, String)', '2000-13-01') SETTINGS allow_experimental_variant_type = 1;

SELECT 'throw, an unquoted numeric token is checked too';
SELECT * FROM format(JSONEachRow, 'v DateTime', '{"v":4294967296}'); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT * FROM format(JSONEachRow, 'v DateTime', '{"v":-1}'); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT * FROM format(Values, 'v DateTime', '(4294967296)'); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT * FROM format(JSONEachRow, 'v DateTime', '{"v":1700000000}');
SELECT * FROM format(Values, 'v DateTime', '(4294967295)');
SELECT * FROM format(JSONEachRow, 'v DateTime', '{"v":1703363853.5}');

SELECT 'throw, the last second keeps its fractional ticks';
SELECT * FROM format(JSONEachRow, 'v DateTime64(3)', '{"v":253402300799.5}');
SELECT * FROM format(JSONEachRow, 'v DateTime64(3)', '{"v":253402300799.999}');
SELECT * FROM format(Values, 'v DateTime64(3)', '(253402300799.5)');

SELECT 'throw, the range is checked after the timezone offset is applied';
SELECT parseDateTimeBestEffort('2106-02-07 07:28:15+01:00', 'UTC'), parseDateTimeBestEffort('1969-12-31 23:00:00-01:00', 'UTC');
SELECT parseDateTimeBestEffortOrNull('2106-02-07 07:28:15+01:00', 'UTC'), toDateTime('2106-02-07 07:28:15+01:00', 'UTC');
SELECT parseDateTimeBestEffort('2106-02-07 08:28:15+01:00', 'UTC'); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }

SELECT 'throw, typed JSON columns are checked like declared ones';
SELECT j.d FROM format(JSONEachRow, 'j JSON(d Date)', '{"j":{"d":"2150-12-31"}}'); -- { serverError INCORRECT_DATA }
SELECT j.d FROM format(JSONEachRow, 'j JSON(d DateTime)', '{"j":{"d":"2106-02-07 06:28:16"}}'); -- { serverError INCORRECT_DATA }
SELECT j.d FROM format(JSONEachRow, 'j JSON(d DateTime)', '{"j":{"d":"2106-02-07 06:28:16"}}') SETTINGS date_time_input_format = 'basic'; -- { serverError INCORRECT_DATA }
SELECT j.d FROM format(JSONEachRow, 'j JSON(d DateTime)', '{"j":{"d":4294967296}}'); -- { serverError INCORRECT_DATA }
SELECT j.d FROM format(JSONEachRow, 'j JSON(d Date)', '{"j":{"d":"2149-06-06"}}');
SELECT j.d FROM format(JSONEachRow, 'j JSON(d DateTime64(3))', '{"j":{"d":"2299-12-31 23:59:59.999"}}');

SELECT 'throw, JSONExtract keeps returning a default or NULL';
SELECT JSONExtract('{"d":"2150-12-31"}', 'd', 'Date'), JSONExtract('{"d":"2150-12-31"}', 'd', 'Nullable(Date)'), JSONExtract('{"d":"2149-06-06"}', 'd', 'Date');

SELECT 'throw, an explicitly written year 0000 is not silently replaced';
SELECT parseDateTimeBestEffort('0000-01-01 00:00:00'); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT parseDateTimeBestEffort('00000101'); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT parseDateTimeBestEffortUS('01/01/0000'); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT toDateTime('0000-01-01 00:00:00'); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT * FROM format(JSONEachRow, 'v DateTime', '{"v":"0000-01-01 00:00:00"}'); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT parseDateTimeBestEffortOrNull('0000-01-01 00:00:00'), parseDateTimeBestEffortOrZero('0000-01-01 00:00:00');
-- An absent year is a documented best-effort feature, not an overflow
SELECT toMonth(parseDateTimeBestEffort('Mar  3 01:33:48'));
