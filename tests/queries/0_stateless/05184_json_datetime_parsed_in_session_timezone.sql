-- A date/time written without a zone is parsed in the reading session's zone. `JSON` parsing builds
-- an extract tree whose nodes are cached by type name and reused by later queries, so a node must
-- take the zone from the format settings on each use rather than keep the one it was built with.
-- Every statement pins `session_timezone`, which the test runner randomizes.

DROP TABLE IF EXISTS t_json_datetime_parse;
DROP TABLE IF EXISTS t_json_datetime_parse_explicit;

CREATE TABLE t_json_datetime_parse (j JSON(ts DateTime)) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t_json_datetime_parse_explicit (j JSON(ts DateTime('Europe/Berlin'))) ENGINE = MergeTree
    ORDER BY tuple();

-- The same text inserted under two zones is two different instants, so the stored timestamps differ.
INSERT INTO t_json_datetime_parse SELECT '{"ts": "1970-01-01 09:00:00"}'
    SETTINGS session_timezone = 'Asia/Tokyo';
INSERT INTO t_json_datetime_parse SELECT '{"ts": "1970-01-01 09:00:00"}'
    SETTINGS session_timezone = 'UTC';

SELECT 'parsed in tokyo then utc', arraySort(groupArray(toUnixTimestamp(j.ts))) FROM t_json_datetime_parse;

-- Same pair in the opposite order: a node built by the first insert must not decide the second.
TRUNCATE TABLE t_json_datetime_parse;
INSERT INTO t_json_datetime_parse SELECT '{"ts": "1970-01-01 09:00:00"}'
    SETTINGS session_timezone = 'UTC';
INSERT INTO t_json_datetime_parse SELECT '{"ts": "1970-01-01 09:00:00"}'
    SETTINGS session_timezone = 'Asia/Tokyo';

SELECT 'parsed in utc then tokyo', arraySort(groupArray(toUnixTimestamp(j.ts))) FROM t_json_datetime_parse;

-- A declared zone belongs to the value, so the inserting session does not change it.
INSERT INTO t_json_datetime_parse_explicit SELECT '{"ts": "1970-01-01 09:00:00"}'
    SETTINGS session_timezone = 'Asia/Tokyo';
INSERT INTO t_json_datetime_parse_explicit SELECT '{"ts": "1970-01-01 09:00:00"}'
    SETTINGS session_timezone = 'UTC';

SELECT 'declared zone', arraySort(groupArray(toUnixTimestamp(j.ts))) FROM t_json_datetime_parse_explicit;

DROP TABLE t_json_datetime_parse;
DROP TABLE t_json_datetime_parse_explicit;
