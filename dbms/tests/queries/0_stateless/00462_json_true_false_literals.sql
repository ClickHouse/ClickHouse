DROP TABLE IF EXISTS test.json;
CREATE TABLE test.json (x UInt8, title String) ENGINE = Memory;
INSERT INTO test.json FORMAT JSONEachRow {"x": true, "title": "true"}, {"x": false, "title": "false"}, {"x": 0, "title": "0"}, {"x": 1, "title": "1"}

SELECT * FROM test.json ORDER BY title;
DROP TABLE IF EXISTS test.json;
