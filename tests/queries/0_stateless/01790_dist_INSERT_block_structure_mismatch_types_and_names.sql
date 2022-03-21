DROP TABLE IF EXISTS tmp_01781;
DROP TABLE IF EXISTS dist_01781;

SET prefer_localhost_replica=0;

CREATE TABLE tmp_01781 (n LowCardinality(String)) ENGINE=Memory;
CREATE TABLE dist_01781 (n LowCardinality(String)) Engine=Distributed(test_cluster_two_shards, currentDatabase(), tmp_01781, cityHash64(n));

SET insert_distributed_sync=1;
INSERT INTO dist_01781 VALUES ('1'),('2');
-- different LowCardinality size
INSERT INTO dist_01781 SELECT * FROM numbers(1000);

SET insert_distributed_sync=0;
SYSTEM STOP DISTRIBUTED SENDS dist_01781;
INSERT INTO dist_01781 VALUES ('1'),('2');
-- different LowCardinality size
INSERT INTO dist_01781 SELECT * FROM numbers(1000);
SYSTEM FLUSH DISTRIBUTED dist_01781;

DROP TABLE tmp_01781;
DROP TABLE dist_01781;
