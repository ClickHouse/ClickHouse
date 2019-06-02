CREATE TABLE low_cardinality (d Date, x UInt32, s LowCardinality(String)) ENGINE = MergeTree(d, x, 8192);
CREATE TABLE low_cardinality_all (d Date, x UInt32, s LowCardinality(String)) ENGINE = Distributed(test_shard_localhost, default, low_cardinality, sipHash64(s));

INSERT INTO low_cardinality_all (d,x,s) VALUES ('2018-11-12',1,'123');
SELECT s FROM low_cardinality_all;
