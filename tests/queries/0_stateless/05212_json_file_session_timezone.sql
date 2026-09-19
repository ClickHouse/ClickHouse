-- Tags: no-fasttest
-- Tag no-fasttest: needs `SimdJSON` and `RapidJSON`, as 03246_json_simd_rapid_parsers.

-- `File` tables store `FormatSettings` at CREATE; JSON parsing must still follow the reading query.
CREATE TABLE json_file_timezone (j JSON(d DateTime)) ENGINE = File(JSONEachRow);
INSERT INTO json_file_timezone SELECT '{"d":"2024-01-01 12:00:00"}'
SETTINGS session_timezone = 'UTC';
SELECT toUnixTimestamp(j.d) FROM json_file_timezone
SETTINGS session_timezone = 'Asia/Tokyo';
SELECT toUnixTimestamp(j.d) FROM json_file_timezone
SETTINGS session_timezone = 'UTC';

-- `SimdJSON` rejects this nesting and `RapidJSON` accepts it, so this checks `allow_simdjson` without thread reuse.
INSERT INTO FUNCTION file(currentDatabase(), RawBLOB)
SELECT concat('{"x":', repeat('[', 1024), '0', repeat(']', 1024), '}\n')
SETTINGS engine_file_truncate_on_insert = 1;
CREATE TABLE json_file_parser (j JSON(SKIP x)) ENGINE = File(TSV, {CLICKHOUSE_DATABASE:String});
SELECT j FROM json_file_parser SETTINGS allow_simdjson = 0;
SELECT j FROM json_file_parser SETTINGS allow_simdjson = 1; -- { serverError INCORRECT_DATA }

SELECT countIf(j.a = repeat(repeat(toString(number % 10), 2), 1000000))
FROM
(
    SELECT CAST(concat('{"a":"', repeat(repeat(toString(number % 10), 2), 1000000), '"}'), 'JSON(a String)') AS j, number
    FROM numbers(3)
);
