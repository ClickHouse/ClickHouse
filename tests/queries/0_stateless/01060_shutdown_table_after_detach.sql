DROP TABLE IF EXISTS test;
CREATE TABLE test Engine = MergeTree ORDER BY number AS SELECT number, toString(rand()) x from numbers(10000000);

SELECT count() FROM test;

ALTER TABLE test DETACH PARTITION tuple();

SELECT count() FROM test;

DETACH TABLE test;
ATTACH TABLE test;

ALTER TABLE test ATTACH PARTITION tuple();

SELECT count() FROM test;

DROP TABLE test;
