-- Tags: no-ordinary-database, no-fasttest, no-async-insert, no-parallel-replicas
-- Transactions are only supported with MergeTree engines on non-Ordinary databases,
-- and are disabled in the fast-test config. They also don't compose with async
-- inserts (an INSERT inside a transaction is NOT_IMPLEMENTED under async insert)
-- or with reading from parallel replicas (the in-transaction SELECT does not see
-- the MVCC snapshot consistently), so exclude those CI profiles too.

-- `RESET SESSION` must refuse to run while a transaction is open. Silently
-- rolling back would discard work without warning, and pretending to reset
-- while leaving the transaction in place would let a pooled connection
-- inherit the previous snapshot and uncommitted writes.

SET send_logs_level = 'fatal';

DROP TABLE IF EXISTS reset_session_txn;
CREATE TABLE reset_session_txn (n Int) ENGINE = MergeTree ORDER BY n;

-- A rejected `RESET SESSION` must leave the transaction completely intact:
-- the caller can still read its uncommitted writes, and crucially can still
-- COMMIT them. (Earlier, the generic transaction exception hook ran
-- `txn->onException` on the `INVALID_TRANSACTION` throw, silently poisoning
-- the transaction so a later COMMIT would lose the writes.)
BEGIN TRANSACTION;
INSERT INTO reset_session_txn VALUES (1);
RESET SESSION; -- { serverError INVALID_TRANSACTION }
-- Transaction is still active; the row is still visible from inside it.
SELECT count() FROM reset_session_txn;
-- And the transaction can still be committed — the write must persist.
COMMIT;
SELECT 'committed after rejected reset:', count() FROM reset_session_txn;

-- A second open transaction, this time rolled back after a rejected reset.
BEGIN TRANSACTION;
INSERT INTO reset_session_txn VALUES (2);
RESET SESSION; -- { serverError INVALID_TRANSACTION }
ROLLBACK;
-- The rolled-back row must be gone; only the committed (1) remains.
SELECT 'after rollback:', count() FROM reset_session_txn;

-- With no open transaction, RESET SESSION works again.
RESET SESSION;
SELECT 'reset after rollback ok';

DROP TABLE reset_session_txn;
