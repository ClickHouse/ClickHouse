SET allow_experimental_low_cardinality_type = 1;
DROP TABLE IF EXISTS test.test_low_null_float;
DROP TABLE IF EXISTS test.dist;

CREATE TABLE test.test_low_null_float (a LowCardinality(Nullable(Float64))) ENGINE = Memory;
CREATE TABLE test.dist (a LowCardinality(Nullable(Float64))) ENGINE = Distributed('test_cluster_two_shards_localhost', 'test', 'test_low_null_float', rand());

INSERT INTO test.dist (a) SELECT number FROM system.numbers LIMIT 1000000;
SELECT a, count() FROM test.dist GROUP BY a ORDER BY a ASC, count() ASC LIMIT 10;

DROP TABLE IF EXISTS test.test_low_null_float;
DROP TABLE IF EXISTS test.dist;
