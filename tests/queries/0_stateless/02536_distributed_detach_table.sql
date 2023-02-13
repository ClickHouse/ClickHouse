-- test detach distributed table with pending files
CREATE TABLE test_02536 (n UInt64) ENGINE=MergeTree() ORDER BY tuple();
CREATE TABLE test_dist_02536 (n UInt64) ENGINE=Distributed(test_shard_localhost, currentDatabase(), test_02536);
INSERT INTO test_dist_02536 SELECT number FROM numbers(5);
SYSTEM FLUSH DISTRIBUTED test_dist_02536;
SELECT count(n), sum(n) FROM test_dist_02536;
DETACH TABLE test_dist_02536;
-- TODO: check that there is no table
ATTACH TABLE test_dist_02536;
SELECT count(n), sum(n) FROM test_dist_02536;
DROP TABLE test_02536;
DROP TABLE test_dist_02536


-- test detach distributed table with pending files
CREATE TABLE test_02536 (n Int8) ENGINE=MergeTree() ORDER BY tuple();
CREATE TABLE test_dist_02536 (n Int8) ENGINE=Distributed(test_shard_localhost, currentDatabase(), test_02536) SETTINGS bytes_to_delay_insert=1;
SYSTEM STOP DISTRIBUTED SENDS test_dist_02536;

INSERT INTO test_dist_02536 SELECT number FROM numbers(5) SETTINGS prefer_localhost_replica=0;
SELECT count(n), sum(n) FROM test_dist_02536;

DETACH TABLE test_dist_02536;
-- TODO: check table not exists
ATTACH TABLE test_dist_02536;

SYSTEM START DISTRIBUTED SENDS test_dist_02536;
SYSTEM FLUSH DISTRIBUTED test_dist_02536;

SELECT count(n), sum(n) FROM test_dist_02536;
DROP TABLE test_02536" && echo "dropped MergeTree table;
DROP TABLE test_dist_02536" && echo "dropped Distributed table;

