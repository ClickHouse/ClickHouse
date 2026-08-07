-- Tags: no-ordinary-database, no-encrypted-storage, no-parallel-replicas
-- Test: non-transactional OPTIMIZE TABLE FINAL sets correct removal metadata on source
-- parts: removal_tid = NonTransactionalTID = (1,1,'...') and removal_csn = NonTransactionalCSN = 1.
-- This exercises setAndStoreNonTransactionalRemovalTID and the isVisible fast-path in
-- VersionInfo::isVisible (removal_tid.isNonTransactional() → immediately invisible).

DROP TABLE IF EXISTS t;
CREATE TABLE t (n Int64) ENGINE = MergeTree ORDER BY n
    SETTINGS old_parts_lifetime=3600;
SYSTEM STOP MERGES t;

INSERT INTO t VALUES (1);
INSERT INTO t VALUES (2);

-- Two separate parts created non-transactionally
SELECT 'parts_before_merge', count()
FROM system.parts
WHERE database = currentDatabase() AND table = 't' AND active;

SYSTEM START MERGES t;
OPTIMIZE TABLE t FINAL;

-- After non-transactional merge, source parts must have:
--   removal_tid = NonTransactionalTID = (1,1,'00000000-0000-0000-0000-000000000000')
--   removal_csn = NonTransactionalCSN = 1
SELECT 'source_removal_tid_is_non_transactional',
    removal_tid = (1, 1, '00000000-0000-0000-0000-000000000000'),
    removal_csn = 1
FROM system.parts
WHERE database = currentDatabase() AND table = 't'
    AND active = 0
    AND removal_tid = (1, 1, '00000000-0000-0000-0000-000000000000')
ORDER BY name;

-- The merged result part must also use NonTransactionalTID / NonTransactionalCSN
SELECT 'merged_creation_is_non_transactional',
    creation_tid = (1, 1, '00000000-0000-0000-0000-000000000000'),
    creation_csn = 1
FROM system.parts
WHERE database = currentDatabase() AND table = 't' AND active;

-- Source parts are invisible via the isVisible fast-path: removal_tid.isNonTransactional()
-- returns false without further CSN lookup.  Only the merged result should be visible.
-- A non-transactional read still sees only the merged data (2 rows, not 4).
SELECT 'visible_row_count', count() FROM t;

-- Same inside a transaction: the source parts must not be double-counted
SET throw_on_unsupported_query_inside_transaction=0;
BEGIN TRANSACTION;
SELECT 'txn_visible_row_count', count() FROM t;
COMMIT;

DROP TABLE t;
