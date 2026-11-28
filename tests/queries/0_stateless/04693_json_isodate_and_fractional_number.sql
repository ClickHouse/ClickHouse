SET session_timezone = 'Etc/UTC';

-- The non-quoted JSON path handles both the mongodb shell `ISODate(...)` syntax and a plain
-- numeric timestamp, so a fractional number must keep working next to `ISODate`.
SELECT * FROM format(JSONEachRow, 'ts DateTime64(3)', '{"ts": 1716938172.256}');
SELECT * FROM format(JSONEachRow, 'ts DateTime64(3)', '{"ts": 1716938172}');
SELECT * FROM format(JSONEachRow, 'ts DateTime64(3)', '{"ts": -1.5}');
SELECT * FROM format(JSONEachRow, 'ts Nullable(DateTime64(3))', '{"ts": 1716938172.256}');
SELECT * FROM format(JSONEachRow, 'ts Nullable(DateTime64(3))', '{"ts": null}');

-- The raw-value compatibility path must not be intercepted by the `ISODate` probe either.
SELECT * FROM format(JSONEachRow, 'ts DateTime64(3)', '{"ts": 1716938172256}')
    SETTINGS input_format_read_datetime_number_as_raw_value = 1;

-- Mixed rows: `ISODate`, `new ISODate` and a fractional number in one stream.
SELECT * FROM format(JSONEachRow, 'ts DateTime64(3)', $$
{"ts": ISODate("2024-05-29T23:16:12.256Z")}
{"ts": new ISODate("2024-05-29T23:16:12.256Z")}
{"ts": 1716938172.256}
$$);
