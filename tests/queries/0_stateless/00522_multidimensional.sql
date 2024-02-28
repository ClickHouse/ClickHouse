DROP TABLE IF EXISTS multidimensional;
CREATE TABLE multidimensional ENGINE = MergeTree ORDER BY number SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi' AS SELECT number, arrayMap(x -> (x, [x], [[x]], (x, toString(x))), arrayMap(x -> range(x), range(number % 10))) AS value FROM system.numbers LIMIT 100000;

SELECT sum(cityHash64(toString(value))) FROM multidimensional;

DROP TABLE multidimensional;
