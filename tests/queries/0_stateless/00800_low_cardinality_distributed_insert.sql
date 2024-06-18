-- Tags: distributed

SET distributed_foreground_insert = 1;

DROP TABLE IF EXISTS low_cardinality;
DROP TABLE IF EXISTS low_cardinality_all;

set allow_deprecated_syntax_for_merge_tree=1;
CREATE TABLE low_cardinality (d Date, x UInt32, s LowCardinality(String)) ENGINE = MergeTree(d, x, 8192);
CREATE TABLE low_cardinality_all (d Date, x UInt32, s LowCardinality(String)) ENGINE = Distributed(test_shard_localhost, currentDatabase(), low_cardinality, sipHash64(s));

INSERT INTO low_cardinality_all (d,x,s) VALUES ('2018-11-12',1,'123');
SELECT s FROM low_cardinality_all;

DROP TABLE IF EXISTS low_cardinality;
DROP TABLE IF EXISTS low_cardinality_all;
