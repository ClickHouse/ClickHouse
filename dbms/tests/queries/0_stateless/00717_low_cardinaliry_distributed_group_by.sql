set allow_suspicious_low_cardinality_types = 1;
DROP TABLE IF EXISTS test.test_low_null_float;
DROP TABLE IF EXISTS test.dist;

CREATE TABLE test_low_null_float (a LowCardinality(Nullable(Float64))) ENGINE = Memory;
CREATE TABLE dist (a LowCardinality(Nullable(Float64))) ENGINE = Distributed('test_cluster_two_shards_localhost', 'default', 'test_low_null_float', rand());

INSERT INTO dist (a) SELECT number FROM system.numbers LIMIT 1000000;
SELECT a, count() FROM dist GROUP BY a ORDER BY a ASC, count() ASC LIMIT 10;
