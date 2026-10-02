-- Tags: no-replicated-database
-- ^ The clone fill of a replicated target does not veto a UNIQUE KEY source, only a non-replicated
-- `StorageMergeTree` target does, so this test is not meaningful under a Replicated database.

-- A `CREATE ... CLONE AS` on a non-replicated target fills via `StorageMergeTree::replacePartitionFrom`,
-- which rejects a UNIQUE KEY source before the structure check. That rejection used to fire only after
-- the destination table was already published, leaving an orphan empty table behind. The UNIQUE KEY veto
-- must run before publication: the clone is rejected with the same error and no table is left.

SET enable_unique_key = 1;

DROP TABLE IF EXISTS src_uk;
DROP TABLE IF EXISTS dst_uk_refused;

CREATE TABLE src_uk (id UInt32, val String)
ENGINE = MergeTree ORDER BY id UNIQUE KEY id;

-- Inserting into a UNIQUE KEY table requires rocksdb (USE_ROCKSDB=1), which most
-- CI builds ship without, so no data is inserted: the veto rejects the clone based
-- on the source having a UNIQUE KEY, not on its contents.

CREATE TABLE dst_uk_refused CLONE AS src_uk ENGINE = MergeTree ORDER BY id; -- { serverError 344 }

-- No orphan table was left behind, so a retry does not hit `TABLE_ALREADY_EXISTS`.
SELECT count() FROM system.tables WHERE database = currentDatabase() AND name = 'dst_uk_refused';

CREATE TABLE dst_uk_refused (id UInt32, val String) ENGINE = MergeTree ORDER BY id;

DROP TABLE dst_uk_refused;
DROP TABLE src_uk;