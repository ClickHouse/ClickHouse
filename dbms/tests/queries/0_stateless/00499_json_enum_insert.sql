DROP TABLE IF EXISTS test.json;
CREATE TABLE test.json (x Enum8('browser' = 1, 'mobile' = 2), y String) ENGINE = Memory;

INSERT INTO test.json (y) VALUES ('Hello');
SELECT * FROM test.json ORDER BY y;

INSERT INTO test.json (y) FORMAT JSONEachRow {"y": "World 1"};
SELECT * FROM test.json ORDER BY y;

INSERT INTO test.json (x, y) FORMAT JSONEachRow {"y": "World 2"};
SELECT * FROM test.json ORDER BY y;

DROP TABLE test.json;
