USE test;
DROP TABLE IF EXISTS test;
CREATE TABLE test (date Date, keys Array(Nullable(String))) ENGINE = MergeTree(date, date, 1);
INSERT INTO test VALUES ('2017-09-10', ['a', 'b']);
SELECT * FROM test LIMIT 1;
DROP TABLE test;
