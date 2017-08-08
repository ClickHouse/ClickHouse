DROP TABLE IF EXISTS test.non_ascii;
CREATE TABLE test.non_ascii (`привет` String, `мир` String) ENGINE = TinyLog;
INSERT INTO test.non_ascii VALUES ('hello', 'world');
SELECT `привет` FROM test.non_ascii;
SELECT * FROM test.non_ascii;
DROP TABLE test.non_ascii;
