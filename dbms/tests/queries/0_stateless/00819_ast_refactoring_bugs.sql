CREATE TABLE IF NOT EXISTS test.sign (Sign Int8, Arr Array(Int8)) ENGINE = Memory;

SELECT arrayMap(x -> x * Sign, Arr) FROM test.sign;

DROP TABLE test.sign;
