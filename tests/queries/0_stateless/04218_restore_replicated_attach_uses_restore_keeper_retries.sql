-- Tags: no-parallel
-- Uses a global one-shot failpoint in `ReplicatedMergeTreeSink::commitPart`.

SYSTEM DISABLE FAILPOINT replicated_merge_tree_restore_attach_retry;

DROP TABLE IF EXISTS src_04218 SYNC;
DROP TABLE IF EXISTS dst_04218_fail SYNC;
DROP TABLE IF EXISTS dst_04218_ok SYNC;

CREATE TABLE src_04218 (x UInt64)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/04218_restore_retries/src', 'r1')
ORDER BY x;

INSERT INTO src_04218 VALUES (1), (2);

BACKUP TABLE src_04218 TO Memory('04218_restore_retries') FORMAT Null;

CREATE TABLE dst_04218_fail (x UInt64)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/04218_restore_retries/dst_fail', 'r1')
ORDER BY x;

SYSTEM ENABLE FAILPOINT replicated_merge_tree_restore_attach_retry;

RESTORE TABLE src_04218 AS dst_04218_fail FROM Memory('04218_restore_retries')
SETTINGS allow_different_table_def = 1, backup_restore_keeper_max_retries = 0; -- { serverError TABLE_IS_READ_ONLY }

DROP TABLE dst_04218_fail SYNC;

CREATE TABLE dst_04218_ok (x UInt64)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/04218_restore_retries/dst_ok', 'r1')
ORDER BY x;

SYSTEM ENABLE FAILPOINT replicated_merge_tree_restore_attach_retry;

RESTORE TABLE src_04218 AS dst_04218_ok FROM Memory('04218_restore_retries')
SETTINGS allow_different_table_def = 1,
         backup_restore_keeper_max_retries = 1,
         backup_restore_keeper_retry_initial_backoff_ms = 1,
         backup_restore_keeper_retry_max_backoff_ms = 1
FORMAT Null;

SELECT count() FROM dst_04218_ok;

DROP TABLE src_04218 SYNC;
DROP TABLE dst_04218_ok SYNC;
