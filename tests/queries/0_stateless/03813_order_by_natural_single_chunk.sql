DROP TABLE IF EXISTS test1;
CREATE TABLE test1(key String) ENGINE=MergeTree ORDER BY tuple();
INSERT INTO test1 SELECT concat('item', toString(number)) FROM numbers(20);

SELECT * FROM test1 ORDER BY key NATURAL;

DROP TABLE test1;
