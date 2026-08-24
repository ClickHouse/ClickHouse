-- test detach distributed table with pending files
CREATE TABLE test_02536 (n Int8) ENGINE=MergeTree() ORDER BY tuple();
CREATE TABLE test_dist_02536 (n Int8) ENGINE=Distributed(test_cluster_two_shards, currentDatabase(), test_02536, rand());
SYSTEM STOP DISTRIBUTED SENDS test_dist_02536;

INSERT INTO test_dist_02536 SELECT number FROM numbers(5) SETTINGS prefer_localhost_replica=0;
SELECT count(n), sum(n) FROM test_dist_02536; -- 0 0

DETACH TABLE test_dist_02536;
ATTACH TABLE test_dist_02536;

SYSTEM FLUSH DISTRIBUTED test_dist_02536;

SELECT count(n), sum(n) FROM test_dist_02536; -- 10 20
DROP TABLE test_02536;
DROP TABLE test_dist_02536;
