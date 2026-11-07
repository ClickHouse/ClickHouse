SET session_timezone = 'UTC';
CREATE TABLE json_context_source (j JSON(d DateTime)) ENGINE = Null;
CREATE TABLE json_context_target (j JSON(d DateTime)) ENGINE = Memory;
CREATE MATERIALIZED VIEW json_context_view TO json_context_target
AS SELECT CAST(toJSONString(j), 'JSON(d DateTime)') AS j FROM json_context_source;

INSERT INTO json_context_source VALUES ('{"d":"2024-01-01 12:00:00"}');
SET session_timezone = 'Asia/Tokyo';
INSERT INTO json_context_source VALUES ('{"d":"2024-01-01 12:00:00"}');
SET session_timezone = 'UTC';
SELECT toUnixTimestamp(j.d) FROM json_context_target ORDER BY j.d;

DROP VIEW json_context_view;
DROP TABLE json_context_source;
DROP TABLE json_context_target;
