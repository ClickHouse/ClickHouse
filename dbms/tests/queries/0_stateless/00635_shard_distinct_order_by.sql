DROP TABLE IF EXISTS test.data;
CREATE TABLE test.data (s String, x Int8, y Int8) ENGINE = MergeTree ORDER BY tuple();

INSERT INTO test.data VALUES ('hello', 0, 0), ('world', 0, 0), ('hello', 1, -1), ('world', -1, 1);

SELECT DISTINCT s FROM remote('127.0.0.{1,2}', test.data) ORDER BY x + y, s;

DROP TABLE test.data;
