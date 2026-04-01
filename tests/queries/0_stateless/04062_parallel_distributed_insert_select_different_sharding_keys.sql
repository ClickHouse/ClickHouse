-- Tags: shard
-- https://github.com/ClickHouse/ClickHouse/issues/100788
-- parallel_distributed_insert_select=2 does not reshard data when source
-- and target distributed tables have different sharding keys.

SET distributed_ddl_output_mode = 'none';

DROP DATABASE IF EXISTS 04062_test ON CLUSTER test_cluster_two_shards SYNC;

CREATE DATABASE 04062_test ON CLUSTER test_cluster_two_shards;
USE DATABASE 04062_test;

DROP TABLE IF EXISTS dist_tgt_04062;
DROP TABLE IF EXISTS dist_src_04062;
DROP TABLE IF EXISTS local_tgt_04062 ON CLUSTER test_cluster_two_shards SYNC;
DROP TABLE IF EXISTS local_src_04062 ON CLUSTER test_cluster_two_shards SYNC;

CREATE TABLE local_src_04062 ON CLUSTER test_cluster_two_shards
(PrimaryId UInt64, SecondaryId UInt64, Value UInt64)
ENGINE = MergeTree() ORDER BY PrimaryId;

CREATE TABLE local_tgt_04062 ON CLUSTER test_cluster_two_shards
(PrimaryId UInt64, SecondaryId UInt64, Value UInt64)
ENGINE = MergeTree() ORDER BY SecondaryId;

CREATE TABLE dist_src_04062 AS local_src_04062
ENGINE = Distributed(test_cluster_two_shards, currentDatabase(), local_src_04062, PrimaryId);

CREATE TABLE dist_tgt_04062 AS local_tgt_04062
ENGINE = Distributed(test_cluster_two_shards, currentDatabase(), local_tgt_04062, SecondaryId);

-- Two rows where PrimaryId and SecondaryId route to opposite shards (modulo 2):
--   PrimaryId=2 -> shard 1, SecondaryId=3 -> shard 2
--   PrimaryId=3 -> shard 2, SecondaryId=2 -> shard 1
INSERT INTO dist_src_04062 VALUES (2, 3, 100), (3, 2, 200);
SYSTEM FLUSH DISTRIBUTED dist_src_04062;

-- Test 1: parallel_distributed_insert_select=2 (default).
-- Each shard copies local_src to local_tgt without resharding by SecondaryId.
INSERT INTO dist_tgt_04062 SELECT * FROM dist_src_04062;

SELECT count() FROM dist_tgt_04062;
SELECT sum(Value) FROM dist_tgt_04062;

-- With optimize_skip_unused_shards, queries route by SecondaryId.
-- If data was correctly resharded, these find the expected rows.
SELECT PrimaryId, SecondaryId, Value FROM dist_tgt_04062
    WHERE SecondaryId = 2
    SETTINGS optimize_skip_unused_shards = 1;
SELECT PrimaryId, SecondaryId, Value FROM dist_tgt_04062
    WHERE SecondaryId = 3
    SETTINGS optimize_skip_unused_shards = 1;

-- Reset target for next test.
TRUNCATE TABLE local_tgt_04062 ON CLUSTER test_cluster_two_shards SYNC;

-- Test 2: parallel_distributed_insert_select=0 (workaround, reshards correctly).
SELECT 'parallel_distributed_insert_select=0';
INSERT INTO dist_tgt_04062 SELECT * FROM dist_src_04062
    SETTINGS parallel_distributed_insert_select = 0;
SYSTEM FLUSH DISTRIBUTED dist_tgt_04062;

SELECT count() FROM dist_tgt_04062;
SELECT sum(Value) FROM dist_tgt_04062;

SELECT PrimaryId, SecondaryId, Value FROM dist_tgt_04062
    WHERE SecondaryId = 2
    SETTINGS optimize_skip_unused_shards = 1;
SELECT PrimaryId, SecondaryId, Value FROM dist_tgt_04062
    WHERE SecondaryId = 3
    SETTINGS optimize_skip_unused_shards = 1;

DROP TABLE dist_tgt_04062;
DROP TABLE dist_src_04062;
DROP TABLE local_tgt_04062 ON CLUSTER test_cluster_two_shards SYNC;
DROP TABLE local_src_04062 ON CLUSTER test_cluster_two_shards SYNC;
DROP DATABASE 04062_test ON CLUSTER test_cluster_two_shards SYNC;
