-- The MongoDB shell `ISODate("...")` / `new ISODate("...")` wrapper is accepted by a declared `DateTime64`
-- target, and by a `DateTime64` arm of `Nullable` or `Variant`. A non-UTC time zone makes the time zone
-- handling visible: a trailing `Z` is UTC, no suffix is the column time zone.

SET session_timezone = 'Asia/Kolkata';

SELECT 'DateTime64';
SELECT ts FROM format(JSONEachRow, 'ts DateTime64(3)', $$
{"ts": ISODate("2024-05-29T23:16:12.256Z")}
{"ts": new ISODate("2024-05-29T23:16:12.256Z")}
{"ts": ISODate("2024-05-29T23:16:12.256")}
{"ts": new ISODate("2024-05-29T23:16:12.256")}
{"ts": "2024-05-29T23:16:12.256Z"}
$$);

SELECT 'DateTime64 with an explicit time zone';
SELECT ts FROM format(JSONEachRow, 'ts DateTime64(3, \'UTC\')', $$
{"ts": ISODate("2024-05-29T23:16:12.256Z")}
{"ts": new ISODate("2024-05-29T23:16:12.256")}
$$);

SELECT 'Nullable(DateTime64)';
SELECT ts FROM format(JSONEachRow, 'ts Nullable(DateTime64(3))', $$
{"ts": ISODate("2024-05-29T23:16:12.256Z")}
{"ts": null}
{"ts": new ISODate("2024-05-29T23:16:12.256Z")}
$$);

SELECT 'Variant(DateTime64, String)';
SELECT v, variantType(v) FROM format(JSONEachRow, 'v Variant(DateTime64(3), String)', $$
{"v": ISODate("2024-05-29T23:16:12.256Z")}
{"v": new ISODate("2024-05-29T23:16:12.256")}
{"v": "plain string"}
$$);

SELECT 'INSERT into a table';
DROP TABLE IF EXISTS test_isodate;
CREATE TABLE test_isodate (id UInt8, ts DateTime64(3, 'UTC'), n Nullable(DateTime64(3, 'UTC'))) ENGINE = Memory;
INSERT INTO test_isodate FORMAT JSONEachRow {"id": 1, "ts": ISODate("2024-05-29T23:16:12.256Z"), "n": new ISODate("2024-05-29T23:16:12.256Z")};

INSERT INTO test_isodate FORMAT JSONEachRow {"id": 2, "ts": new ISODate("2024-05-29T23:16:12.256Z"), "n": null};

SELECT * FROM test_isodate ORDER BY id;
DROP TABLE test_isodate;
