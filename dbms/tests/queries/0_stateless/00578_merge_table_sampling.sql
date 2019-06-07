DROP TABLE IF EXISTS test.numbers1;
DROP TABLE IF EXISTS test.numbers2;

CREATE TABLE test.numbers1 ENGINE = Memory AS SELECT number FROM numbers(1000);
CREATE TABLE test.numbers2 ENGINE = Memory AS SELECT number FROM numbers(1000);

SELECT * FROM merge(test, '^numbers\\d+$') SAMPLE 0.1; -- { serverError 141 }

DROP TABLE test.numbers1;
DROP TABLE test.numbers2;

CREATE TABLE test.numbers1 ENGINE = MergeTree ORDER BY intHash32(number) SAMPLE BY intHash32(number) AS SELECT number FROM numbers(1000);
CREATE TABLE test.numbers2 ENGINE = MergeTree ORDER BY intHash32(number) SAMPLE BY intHash32(number) AS SELECT number FROM numbers(1000);

SELECT * FROM merge(test, '^numbers\\d+$') SAMPLE 0.01;

DROP TABLE test.numbers1;
DROP TABLE test.numbers2;
