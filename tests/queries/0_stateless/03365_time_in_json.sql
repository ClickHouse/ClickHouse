-- Test Time and Time64 types in JSON

SET enable_time_time64_type = 1;

-- Clean up
DROP TABLE IF EXISTS json_time_test;
DROP TABLE IF EXISTS json_time64_test;
DROP TABLE IF EXISTS json_splits;

-- Time type in JSON
CREATE TABLE json_time_test
(
    `json` JSON(`time_value` Time, `id` String)
)
ENGINE = Memory;

INSERT INTO json_time_test VALUES ('{"time_value": "12:30:45", "id": "1"}');
INSERT INTO json_time_test VALUES ('{"time_value": "01:05:10", "id": "2"}');
INSERT INTO json_time_test VALUES ('{"time_value": "23:59:59", "id": "3"}');

SELECT json.time_value, json.id FROM json_time_test ORDER BY json.id;

-- Time64 type in JSON
CREATE TABLE json_time64_test
(
    `json` JSON(`time_value` Time64(3), `id` String)
)
ENGINE = Memory;

INSERT INTO json_time64_test VALUES ('{"time_value": "12:30:45.123", "id": "1"}');
INSERT INTO json_time64_test VALUES ('{"time_value": "01:05:10.456", "id": "2"}');
INSERT INTO json_time64_test VALUES ('{"time_value": "23:59:59.999", "id": "3"}');

SELECT json.time_value, json.id FROM json_time64_test ORDER BY json.id;

-- #82267
CREATE TABLE json_splits
(
    `json` JSON(`metric.moving_time` Time, `id` String)
)
ORDER BY json.id;

INSERT INTO json_splits VALUES ('{"metric":{"moving_time":"01:23:45"}, "id": "1"}');
INSERT INTO json_splits VALUES ('{"metric":{"moving_time":"02:10:30"}, "id": "2"}');

SELECT json.`metric.moving_time`, json.id FROM json_splits ORDER BY json.id;
