-- Tags: no-fasttest

DROP TABLE IF EXISTS json;
CREATE TABLE json (x Enum8('browser' = 1, 'mobile' = 2), y String) ENGINE = Memory;

INSERT INTO json (y) VALUES ('Hello');
SELECT * FROM json ORDER BY y;

INSERT INTO json (y) FORMAT JSONEachRow {"y": "World 1"};
SELECT * FROM json ORDER BY y;

INSERT INTO json (x, y) FORMAT JSONEachRow {"y": "World 2"};
SELECT * FROM json ORDER BY y;

DROP TABLE json;
