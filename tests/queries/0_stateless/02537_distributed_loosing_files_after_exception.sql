CREATE DATABASE IF NOT EXISTS shard_0;
CREATE DATABASE IF NOT EXISTS shard_1;

DROP TABLE IF EXISTS dist_02537;
DROP TABLE IF EXISTS shard_0.test_02537;
DROP TABLE IF EXISTS shard_1.test_02537;

CREATE TABLE shard_0.test_02537 (n UInt64) ENGINE=MergeTree() ORDER BY tuple();
CREATE TABLE dist_02537 (n UInt64) ENGINE=Distributed('test_cluster_two_shards_different_databases', /* default_database= */ '', test_02537, rand());

SYSTEM STOP DISTRIBUTED SENDS dist_02537;
INSERT INTO dist_02537 SELECT number FROM numbers(5) SETTINGS prefer_localhost_replica=0;

SYSTEM START DISTRIBUTED SENDS dist_02537;

-- should be only one file record for this table
SELECT throwIf(sum(error_count) < 1), throwIf(sum(data_files) != 1) FROM system.distribution_queue WHERE database = 'default' AND table = 'dist_02537' AND is_blocked = 0;

CREATE TABLE shard_1.test_02537 (n UInt64) ENGINE=MergeTree() ORDER BY tuple();

SYSTEM FLUSH DISTRIBUTED dist_02537;

SELECT count(n), sum(n) FROM dist_02537; -- 5 10
