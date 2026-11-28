-- With `date_time_input_format = 'basic'` the `ISODate("...")` wrapper still treats a trailing `Z` as UTC
-- (the `basic` parser itself has no notion of it) and keeps a value without suffix in the column time zone.
-- Anything else after the date-time is rejected. `Nullable` and `Variant` go through the `try` path.

SET date_time_input_format = 'basic';
SET session_timezone = 'Asia/Kolkata';

SELECT 'DateTime64';
SELECT ts FROM format(JSONEachRow, 'ts DateTime64(3)', $$
{"ts": ISODate("2024-05-29T23:16:12.256Z")}
{"ts": new ISODate("2024-05-29T23:16:12.256Z")}
{"ts": ISODate("2024-05-29T23:16:12.256")}
{"ts": new ISODate("2024-05-29T23:16:12.256")}
$$);

SELECT 'Nullable(DateTime64)';
SELECT ts FROM format(JSONEachRow, 'ts Nullable(DateTime64(3))', $$
{"ts": ISODate("2024-05-29T23:16:12.256Z")}
{"ts": null}
{"ts": new ISODate("2024-05-29T23:16:12.256")}
$$);

SELECT 'Variant(DateTime64, String)';
SELECT v, variantType(v) FROM format(JSONEachRow, 'v Variant(DateTime64(3), String)', $$
{"v": ISODate("2024-05-29T23:16:12.256Z")}
{"v": new ISODate("2024-05-29T23:16:12.256")}
{"v": "plain string"}
$$);

SELECT 'Rejected';
SELECT * FROM format(JSONEachRow, 'ts DateTime64(3)', '{"ts": ISODate("2024-05-29T23:16:12.256ZZ")}'); -- { serverError UNEXPECTED_DATA_AFTER_PARSED_VALUE }
SELECT * FROM format(JSONEachRow, 'ts DateTime64(3)', '{"ts": ISODate("2024-05-29T23:16:12.256+05:30")}'); -- { serverError UNEXPECTED_DATA_AFTER_PARSED_VALUE }
