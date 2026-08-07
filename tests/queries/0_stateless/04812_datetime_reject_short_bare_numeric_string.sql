-- Plain DateTime in basic mode must keep rejecting short bare numeric strings (fewer than five digits):
-- they are neither a plausible unix timestamp nor a date. Short and fractional unix timestamps are
-- meaningful only for DateTime64, where the small-decimal-timestamp fix accepts them.
-- https://github.com/ClickHouse/ClickHouse/pull/86431

SET date_time_input_format = 'basic', cast_string_to_date_time_mode = 'basic';

SELECT toDateTime('2018'); -- { serverError CANNOT_PARSE_DATETIME }
SELECT toDateTime('123'); -- { serverError CANNOT_PARSE_DATETIME }
SELECT toDateTime('0'); -- { serverError CANNOT_PARSE_DATETIME }
SELECT toDateTime('-123'); -- { serverError CANNOT_PARSE_DATETIME }
SELECT CAST('2018' AS DateTime); -- { serverError CANNOT_PARSE_DATETIME }
SELECT toDateTimeOrNull('2018');
SELECT toDateTimeOrZero('2018', 'UTC');

-- Five digits and more remain a valid unix timestamp.
SELECT toDateTime('12345', 'UTC');

-- A short number followed by other characters is not a bare short number: the numeric prefix is
-- parsed and the cast reports the trailing characters with the whole value in the message.
SELECT CAST('2021-1-01 00:00:00' AS DateTime); -- { serverError CANNOT_PARSE_TEXT }
SELECT CAST('01-02-2020 20:00:00Z' AS DateTime); -- { serverError CANNOT_PARSE_TEXT }

-- The value is also rejected when the rest of the input keeps the whole broken-down date and time
-- length visible in the read buffer (the optimistic parsing path).
SELECT d FROM format(TSV, 'd DateTime, s String', '2018\tpadding padding padding'); -- { serverError CANNOT_PARSE_DATETIME }

-- DateTime64 accepts short and fractional unix timestamps: that is the point of the fix.
SELECT toDateTime64('2018', 3, 'UTC');
SELECT toDateTime64('1234.5', 3, 'UTC');
SELECT toDateTime64OrNull('2018', 3, 'UTC');
