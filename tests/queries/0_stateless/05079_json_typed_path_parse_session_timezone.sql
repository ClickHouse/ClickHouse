SET session_timezone = 'UTC';
CREATE TABLE json_parse_timezone
(
    id UInt8,
    j JSON(d DateTime, n Array(Nullable(DateTime64(3))), fixed DateTime('UTC'), nested JSON(d DateTime))
) ENGINE = Memory;

INSERT INTO json_parse_timezone FORMAT JSONEachRow
{"id":1,"j":{"d":"2024-01-01 12:00:00","n":["2024-01-01 12:00:00.123",null],"fixed":"2024-01-01 12:00:00","nested":{"d":"2024-01-01 12:00:00"}}}

SET session_timezone = 'Asia/Tokyo';
INSERT INTO json_parse_timezone FORMAT JSONEachRow
{"id":2,"j":{"d":"2024-01-01 12:00:00","n":["2024-01-01 12:00:00.123",null],"fixed":"2024-01-01 12:00:00","nested":{"d":"2024-01-01 12:00:00"}}}

SET session_timezone = 'Europe/Amsterdam';
INSERT INTO json_parse_timezone FORMAT JSONEachRow
{"id":3,"j":{"d":"2024-01-01 12:00:00","n":["2024-01-01 12:00:00.123",null],"fixed":"2024-01-01 12:00:00","nested":{"d":"2024-01-01 12:00:00"}}}

SET session_timezone = 'UTC';
SELECT id, toUnixTimestamp(j.d), arrayMap(x -> toUnixTimestamp64Milli(x), j.n), toUnixTimestamp(j.fixed), toUnixTimestamp(j.nested.d)
FROM json_parse_timezone ORDER BY id;
DROP TABLE json_parse_timezone;
