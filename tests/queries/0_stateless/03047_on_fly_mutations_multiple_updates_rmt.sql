-- Tags: no-random-merge-tree-settings, no-random-settings, no-fasttest, no-parallel-replicas, no-parallel
-- no-parallel-replicas: reading from s3 ('S3GetObject' event) can happened on any "replica", so we can see no 'S3GetObject' on initiator
-- no-parallel: SYSTEM DROP MARK CACHE is used.

DROP TABLE IF EXISTS t_lightweight_mut_5;

SET apply_mutations_on_fly = 1;
SET enable_filesystem_cache = 0;
SET read_through_distributed_cache=0;

CREATE TABLE t_lightweight_mut_5 (id UInt64, s1 String, s2 String)
ENGINE = ReplicatedMergeTree('/clickhouse/zktest/tables/{database}/t_lightweight_mut_1', '1') ORDER BY id
SETTINGS min_bytes_for_wide_part = 0,
    min_bytes_for_full_part_storage = 0,
    primary_key_lazy_load = 0,
    serialization_info_version = 'basic',
    storage_policy = 's3_cache';

SYSTEM STOP MERGES t_lightweight_mut_5;

INSERT INTO t_lightweight_mut_5 VALUES (1, 'a', 'b');
ALTER TABLE t_lightweight_mut_5 UPDATE s1 = 'x', s2 = 'y' WHERE id = 1;

SYSTEM SYNC REPLICA t_lightweight_mut_5 PULL;

SYSTEM DROP MARK CACHE;
SELECT s1 FROM t_lightweight_mut_5 ORDER BY id;

SYSTEM DROP MARK CACHE;
SELECT s2 FROM t_lightweight_mut_5 ORDER BY id;

SYSTEM DROP MARK CACHE;
SELECT s1, s2 FROM t_lightweight_mut_5 ORDER BY id;

SYSTEM FLUSH LOGS query_log;

SELECT query, ProfileEvents['S3GetObject'] FROM system.query_log
WHERE
    current_database = currentDatabase()
    AND query ILIKE 'SELECT%FROM t_lightweight_mut_5%'
    AND type = 'QueryFinish'
ORDER BY event_time_microseconds;

DROP TABLE t_lightweight_mut_5;
