-- Tags: no-ordinary-database, no-encrypted-storage, no-parallel-replicas
-- Test: MVCC snapshot isolation for INSERT and DROP operations.
-- Verifies that:
--   1. Uncommitted inserts are not visible at snapshot 1 (NonTransactionalCSN).
--   2. After commit, new parts become visible to subsequent transactions.
--   3. Rolled-back parts (creation_csn = RolledBackCSN) are never visible.
--   4. Parts removed inside a transaction are invisible to the same transaction.

DROP TABLE IF EXISTS t;
CREATE TABLE t (n Int64) ENGINE = MergeTree ORDER BY n
    SETTINGS old_parts_lifetime=3600;
SYSTEM STOP MERGES t;
SET throw_on_unsupported_query_inside_transaction=0;

-- Non-transactional inserts land at creation_csn = NonTransactionalCSN = 1
INSERT INTO t VALUES (1);
INSERT INTO t VALUES (2);

-- ------------------------------------------------------------------
-- Case 1: snapshot 1 sees only non-transactional parts
-- ------------------------------------------------------------------
BEGIN TRANSACTION;
-- Commit a transactional insert; its part will have creation_csn > 1
INSERT INTO t VALUES (3);
COMMIT;

-- Current snapshot (after the commit above) should see 3 rows
SELECT 'rows_after_commit', count() FROM t;

BEGIN TRANSACTION;
-- Rewind snapshot to 1 (NonTransactionalCSN): only non-txn parts are visible
SET TRANSACTION SNAPSHOT 1;
SELECT 'rows_at_snapshot_1', count() FROM t;
COMMIT;

-- ------------------------------------------------------------------
-- Case 2: rolled-back insert is invisible after rollback
-- ------------------------------------------------------------------
BEGIN TRANSACTION;
INSERT INTO t VALUES (100);
ROLLBACK;

-- Rolled-back part has creation_csn = RolledBackCSN; must be invisible
SELECT 'rows_after_rollback', count() FROM t;

-- The rolled-back part still appears in system.parts with RolledBackCSN
SELECT 'rolled_back_part_csn',
    creation_csn = 18446744073709551615
FROM system.parts
WHERE database = currentDatabase() AND table = 't'
    AND creation_csn = 18446744073709551615;

-- ------------------------------------------------------------------
-- Case 3: parts dropped inside a transaction are invisible to that same session
-- ------------------------------------------------------------------
BEGIN TRANSACTION;
INSERT INTO t VALUES (4);
-- The just-inserted part is visible within the transaction
SELECT 'rows_in_txn_before_drop', count() FROM t;
ALTER TABLE t DROP PARTITION ID 'all';
-- After dropping, all parts (including the new insert) should be invisible
SELECT 'rows_in_txn_after_drop', count() FROM t;
ROLLBACK;

-- After rollback, the dropped parts are restored
SELECT 'rows_after_drop_rollback', count() FROM t;

DROP TABLE t;
