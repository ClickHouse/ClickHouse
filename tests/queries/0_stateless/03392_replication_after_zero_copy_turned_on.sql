-- Tags: no-parallel, no-fasttest

DROP TABLE IF EXISTS test03392_zero_copy_replication_r1;
DROP TABLE IF EXISTS test03392_zero_copy_replication_r2;
DROP TABLE IF EXISTS test03392_zero_copy_replication_r3;

CREATE TABLE test03392_zero_copy_replication_r1 (x UInt64)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/test03392_zero_copy_replication','1')
    ORDER BY x
    SETTINGS storage_policy='s3_cache', allow_remote_fs_zero_copy_replication=0;

INSERT INTO test03392_zero_copy_replication_r1 VALUES (1);

CREATE TABLE test03392_zero_copy_replication_r2 (x UInt64)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/test03392_zero_copy_replication','2')
    ORDER BY x
    SETTINGS storage_policy='s3_cache', allow_remote_fs_zero_copy_replication=0;

SYSTEM SYNC REPLICA test03392_zero_copy_replication_r2;

ALTER TABLE test03392_zero_copy_replication_r1 MODIFY SETTING allow_remote_fs_zero_copy_replication=1;
ALTER TABLE test03392_zero_copy_replication_r2 MODIFY SETTING allow_remote_fs_zero_copy_replication=1;

CREATE TABLE test03392_zero_copy_replication_r3 (x UInt64)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/test03392_zero_copy_replication','3')
    ORDER BY x
    SETTINGS storage_policy='s3_cache', allow_remote_fs_zero_copy_replication=1;

SYSTEM SYNC REPLICA test03392_zero_copy_replication_r3;

SELECT count() FROM test03392_zero_copy_replication_r1;
SELECT count() FROM test03392_zero_copy_replication_r2;
SELECT count() FROM test03392_zero_copy_replication_r3;

SYSTEM START MERGES;

DROP TABLE IF EXISTS test03392_zero_copy_replication_r1;
DROP TABLE IF EXISTS test03392_zero_copy_replication_r2;
DROP TABLE IF EXISTS test03392_zero_copy_replication_r3;
