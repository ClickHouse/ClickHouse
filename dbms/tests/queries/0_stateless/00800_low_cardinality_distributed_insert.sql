SET allow_experimental_low_cardinality_type = 1;
DROP TABLE IF EXISTS test.low_cardinality;
DROP TABLE IF EXISTS test.low_cardinality_all;

CREATE TABLE test.low_cardinality (d Date, x UInt32, s LowCardinality(String)) ENGINE = MergeTree(d, x, 8192);
CREATE TABLE test.low_cardinality_all (d Date, x UInt32, s LowCardinality(String)) ENGINE = Distributed(test_shard_localhost, test, low_cardinality, sipHash64(s));

INSERT INTO test.low_cardinality_all (d,x,s) VALUES ('2018-11-12',1,'123');
SELECT s FROM test.low_cardinality_all;

DROP TABLE IF EXISTS test.low_cardinality;
DROP TABLE IF EXISTS test.low_cardinality_all;
