-- Tags: no-ordinary-database, no-fasttest
-- Transactions are only supported with MergeTree engines on non-Ordinary databases,
-- and are disabled in the fast-test config.

-- `RESET SESSION` must refuse to run while a transaction is open. Silently
-- rolling back would discard work without warning, and pretending to reset
-- while leaving the transaction in place would let a pooled connection
-- inherit the previous snapshot and uncommitted writes.

SET send_logs_level = 'fatal';

DROP TABLE IF EXISTS reset_session_txn;
CREATE TABLE reset_session_txn (n Int) ENGINE = MergeTree ORDER BY n;

BEGIN TRANSACTION;
INSERT INTO reset_session_txn VALUES (1);
RESET SESSION; -- { serverError INVALID_TRANSACTION }
-- Transaction is still active; the row is still visible from inside it.
SELECT count() FROM reset_session_txn;
ROLLBACK;

-- After rolling back, RESET SESSION works again.
RESET SESSION;
SELECT 'reset after rollback ok';

DROP TABLE reset_session_txn;
