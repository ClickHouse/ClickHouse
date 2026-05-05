-- Test: exercises the NOT_IMPLEMENTED throw path in `MergeTreeData::movePartitionToTable`
-- when the destination table is not a MergeTreeData-derived storage.
-- Covers: src/Storages/MergeTree/MergeTreeData.cpp:6572 — `if (!dest_storage_merge_tree)` branch
DROP TABLE IF EXISTS src_mt_04202;
DROP TABLE IF EXISTS dst_memory_04202;
DROP TABLE IF EXISTS dst_log_04202;
DROP TABLE IF EXISTS dst_null_04202;

CREATE TABLE src_mt_04202 (x UInt64) ENGINE = MergeTree ORDER BY x PARTITION BY x;
CREATE TABLE dst_memory_04202 (x UInt64) ENGINE = Memory;
CREATE TABLE dst_log_04202 (x UInt64) ENGINE = Log;
CREATE TABLE dst_null_04202 (x UInt64) ENGINE = Null;

INSERT INTO src_mt_04202 VALUES (1), (2);

-- Memory engine destination: should throw NOT_IMPLEMENTED (code 48)
ALTER TABLE src_mt_04202 MOVE PARTITION 1 TO TABLE dst_memory_04202; -- { serverError NOT_IMPLEMENTED }

-- Log engine destination: should throw NOT_IMPLEMENTED
ALTER TABLE src_mt_04202 MOVE PARTITION 1 TO TABLE dst_log_04202; -- { serverError NOT_IMPLEMENTED }

-- Null engine destination: should throw NOT_IMPLEMENTED
ALTER TABLE src_mt_04202 MOVE PARTITION 1 TO TABLE dst_null_04202; -- { serverError NOT_IMPLEMENTED }

-- Source partition should still be intact (the throw happened before any move)
SELECT count() FROM src_mt_04202;

DROP TABLE src_mt_04202;
DROP TABLE dst_memory_04202;
DROP TABLE dst_log_04202;
DROP TABLE dst_null_04202;
