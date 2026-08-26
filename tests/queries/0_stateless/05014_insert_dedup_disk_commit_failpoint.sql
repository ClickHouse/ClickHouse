-- Tags: zookeeper, no-fasttest, no-parallel
-- no-fasttest: needs an object-storage disk (storage_policy 's3_cache').
-- no-parallel: enables a server-global failpoint on the part disk-transaction commit.

DROP TABLE IF EXISTS t_dedup_disk_commit SYNC;

CREATE TABLE t_dedup_disk_commit (k UInt64, v String)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_dedup_disk_commit', 'r1')
ORDER BY k
SETTINGS storage_policy = 's3_cache';

SYSTEM ENABLE FAILPOINT part_storage_fail_commit_transaction;

-- The disk-storage commit of the inserted part fails. The part must NOT be registered in Keeper:
-- before the fix the disk commit ran only in MergeTreeData::Transaction::commit, AFTER the Keeper
-- multi had durably created the block_id dedup znode, so this failure left a phantom dedup token.
INSERT INTO t_dedup_disk_commit SETTINGS insert_deduplicate = 1, insert_keeper_fault_injection_probability = 0 VALUES (1, 'x'); -- { serverError FAULT_INJECTED }

SYSTEM DISABLE FAILPOINT part_storage_fail_commit_transaction;

-- Byte-identical retry of the failed INSERT: it must really insert. Before the fix it silently
-- deduplicated against the phantom block_id ("already exists ... ignoring it") and was acked with
-- zero rows written — the acked-then-lost data loss.
INSERT INTO t_dedup_disk_commit SETTINGS insert_deduplicate = 1, insert_keeper_fault_injection_probability = 0 VALUES (1, 'x');

SELECT count() FROM t_dedup_disk_commit;

DROP TABLE t_dedup_disk_commit SYNC;
