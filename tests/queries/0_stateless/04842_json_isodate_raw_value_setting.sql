SET session_timezone = 'Etc/UTC';
SET input_format_read_datetime_number_as_raw_value = 1;

-- `input_format_read_datetime_number_as_raw_value` governs only how an actual number is
-- interpreted, so the mongodb shell `ISODate(...)` syntax must still be recognized.
SELECT * FROM format(JSONEachRow, 'ts DateTime64(3)', '{"ts": ISODate("2024-05-29T23:16:12.256Z")}');
SELECT * FROM format(JSONEachRow, 'ts DateTime64(3)', '{"ts": new ISODate("2024-05-29T23:16:12.256Z")}');
SELECT * FROM format(JSONEachRow, 'ts Nullable(DateTime64(3))', '{"ts": ISODate("2024-05-29T23:16:12.256Z")}');
SELECT * FROM format(JSONEachRow, 'ts Nullable(DateTime64(3))', '{"ts": null}');

-- A number is still read as the raw scaled value (ticks), not as a Unix timestamp.
SELECT * FROM format(JSONEachRow, 'ts DateTime64(3)', '{"ts": 1716938172256}');

-- The quoted form keeps working, and mixed rows are parsed together.
SELECT * FROM format(JSONEachRow, 'ts DateTime64(3)', $$
{"ts": "2024-05-29 23:16:12.256"}
{"ts": ISODate("2024-05-29T23:16:12.256Z")}
{"ts": 1716938172256}
$$);

-- A malformed near-miss is still rejected instead of falling back to the raw value of `123`.
SELECT * FROM format(JSONEachRow, 'ts DateTime64(3)', '{"ts": ISODate123}'); -- { serverError CANNOT_PARSE_NUMBER }
