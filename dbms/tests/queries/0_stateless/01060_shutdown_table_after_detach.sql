CREATE TABLE IF NOT EXISTS test Engine = MergeTree ORDER BY number AS SELECT number, toString(rand()) x from numbers(10000000);

ALTER TABLE test detach partition tuple();

SELECT count() FROM test;

DETACH TABLE test;
ATTACH TABLE test;

ALTER TABLE test ATTACH PARTITION tuple();

SELECT count() FROM test;

DROP TABLE test;
