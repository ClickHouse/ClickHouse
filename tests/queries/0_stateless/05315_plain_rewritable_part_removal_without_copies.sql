-- Tags: no-fasttest, no-replicated-database
-- Tag no-fasttest: requires S3
-- Tag no-replicated-database: plain rewritable should not be shared between replicas

-- Removing parts from a `plain_rewritable` disk must not copy their files.

DROP TABLE IF EXISTS t_prr_plain;
DROP TABLE IF EXISTS t_prr_encrypted;

CREATE TABLE t_prr_plain (a UInt64, s String) ENGINE = MergeTree ORDER BY a
SETTINGS disk = 's3_plain_rewritable', old_parts_lifetime = 0, merge_tree_clear_old_parts_interval_seconds = 100000;
SYSTEM STOP MERGES t_prr_plain;
SYSTEM STOP CLEANUP t_prr_plain;
INSERT INTO t_prr_plain SELECT number, toString(number) FROM numbers(100);
INSERT INTO t_prr_plain SELECT number, toString(number) FROM numbers(100, 100);
INSERT INTO t_prr_plain SELECT number, toString(number) FROM numbers(200, 100);
TRUNCATE TABLE t_prr_plain;

CREATE TABLE t_prr_encrypted (a UInt64, s String) ENGINE = MergeTree ORDER BY a
SETTINGS disk = 'encrypted_s3_plain_rewritable_cache', old_parts_lifetime = 0, merge_tree_clear_old_parts_interval_seconds = 100000;
SYSTEM STOP MERGES t_prr_encrypted;
SYSTEM STOP CLEANUP t_prr_encrypted;
INSERT INTO t_prr_encrypted SELECT number, toString(number) FROM numbers(100);
INSERT INTO t_prr_encrypted SELECT number, toString(number) FROM numbers(100, 100);
INSERT INTO t_prr_encrypted SELECT number, toString(number) FROM numbers(200, 100);
TRUNCATE TABLE t_prr_encrypted;

SYSTEM FLUSH LOGS query_log;

SELECT ProfileEvents['DiskS3CopyObject'] + ProfileEvents['DiskS3UploadPartCopy'] AS copies,
       ProfileEvents['DiskS3DeleteObjects'] > 0 AS removed_in_query
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND event_date >= yesterday()
  AND query LIKE 'TRUNCATE TABLE t_prr_plain%';
SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_prr_plain' AND rows > 0;

SELECT ProfileEvents['DiskS3CopyObject'] + ProfileEvents['DiskS3UploadPartCopy'] AS copies,
       ProfileEvents['DiskS3DeleteObjects'] > 0 AS removed_in_query
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND event_date >= yesterday()
  AND query LIKE 'TRUNCATE TABLE t_prr_encrypted%';
SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_prr_encrypted' AND rows > 0;

DROP TABLE t_prr_plain;
DROP TABLE t_prr_encrypted;
