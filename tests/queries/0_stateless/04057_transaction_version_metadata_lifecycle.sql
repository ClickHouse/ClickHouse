-- Tags: no-ordinary-database, no-encrypted-storage
-- Test: version metadata (creation_csn, removal_tid, removal_csn) is correctly
-- reflected in system.parts after transactional operations.
-- This covers the refactored VersionMetadata class hierarchy and its persistence.

DROP TABLE IF EXISTS t;
CREATE TABLE t (n Int64) ENGINE = MergeTree ORDER BY n
    SETTINGS old_parts_lifetime=3600;
SYSTEM STOP MERGES t;
SET throw_on_unsupported_query_inside_transaction=0;

-- 1. Non-transactional insert: creation_csn = NonTransactionalCSN = 1,
--    creation_tid = NonTransactionalTID = (1,1,'00000000-...')
INSERT INTO t VALUES (1);
SELECT 'non_txn_creation_csn_is_1',
    creation_csn = 1
FROM system.parts
WHERE database = currentDatabase() AND table = 't'
    AND creation_tid = (1, 1, '00000000-0000-0000-0000-000000000000')
    AND active;

-- 2. Transactional insert: creation_csn = 0 (unknown) inside transaction,
--    then becomes the commit CSN after commit
BEGIN TRANSACTION;
INSERT INTO t VALUES (2);
SELECT 'in_txn_creation_csn_is_0',
    creation_csn = 0
FROM system.parts
WHERE database = currentDatabase() AND table = 't'
    AND creation_tid != (1, 1, '00000000-0000-0000-0000-000000000000')
    AND active;
COMMIT;

SELECT 'committed_creation_csn_positive',
    creation_csn > 1
FROM system.parts
WHERE database = currentDatabase() AND table = 't'
    AND creation_tid != (1, 1, '00000000-0000-0000-0000-000000000000')
    AND active;

-- 3. Transactional insert rolled back: creation_csn = RolledBackCSN = 18446744073709551615
BEGIN TRANSACTION;
INSERT INTO t VALUES (3);
ROLLBACK;

SELECT 'rolled_back_creation_csn',
    creation_csn = 18446744073709551615
FROM system.parts
WHERE database = currentDatabase() AND table = 't'
    AND creation_csn = 18446744073709551615;

-- 4. Transactional removal: removal_tid is set and removal_csn = 0 during transaction,
--    removal_csn > 1 after commit
BEGIN TRANSACTION;
ALTER TABLE t DROP PARTITION ID 'all';
-- Inside the removal transaction: removal_tid is set, removal_csn is still 0
SELECT 'in_removal_txn_removal_tid_set', removal_tid != (0, 0, '00000000-0000-0000-0000-000000000000'),
    removal_csn = 0
FROM system.parts
WHERE database = currentDatabase() AND table = 't'
    AND active = 0
    AND removal_csn = 0
    AND removal_tid != (0, 0, '00000000-0000-0000-0000-000000000000')
ORDER BY name;
COMMIT;

-- After commit: removal_csn should be the commit's CSN (> 1)
SELECT 'committed_removal_csn_positive',
    removal_csn > 1
FROM system.parts
WHERE database = currentDatabase() AND table = 't'
    AND removal_csn > 1
ORDER BY name;

-- 5. Transactional removal rolled back: removal_tid and removal_csn are reset to empty/0
INSERT INTO t VALUES (10);
BEGIN TRANSACTION;
ALTER TABLE t DROP PARTITION ID 'all';
ROLLBACK;

-- After rollback: removal_tid = empty, removal_csn = 0 for the active part
SELECT 'rollback_removal_tid_empty',
    removal_tid = (0, 0, '00000000-0000-0000-0000-000000000000'),
    removal_csn = 0
FROM system.parts
WHERE database = currentDatabase() AND table = 't' AND active
ORDER BY name;

DROP TABLE t;
