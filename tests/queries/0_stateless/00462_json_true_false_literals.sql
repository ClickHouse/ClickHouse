-- Tags: no-fasttest

DROP TABLE IF EXISTS json;
CREATE TABLE json (x UInt8, title String) ENGINE = Memory;
INSERT INTO json FORMAT JSONEachRow {"x": true, "title": "true"}, {"x": false, "title": "false"}, {"x": 0, "title": "0"}, {"x": 1, "title": "1"};

SELECT * FROM json ORDER BY title;
DROP TABLE IF EXISTS json;
