DROP TABLE IF EXISTS test.multidimensional;
CREATE TABLE test.multidimensional ENGINE = MergeTree ORDER BY number AS SELECT number, arrayMap(x -> (x, [x], [[x]], (x, toString(x))), arrayMap(x -> range(x), range(number % 10))) AS value FROM system.numbers LIMIT 100000;

SELECT sum(cityHash64(toString(value))) FROM test.multidimensional;

DROP TABLE test.multidimensional;
