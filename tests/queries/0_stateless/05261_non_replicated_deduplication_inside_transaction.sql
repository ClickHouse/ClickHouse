-- Tags: no-ordinary-database
-- no-ordinary-database: transactions are not supported in Ordinary databases.

-- The deduplication log of a non-replicated MergeTree is not aware of transactions: the block IDs of a part inserted
-- inside a transaction outlived its ROLLBACK, and a retry of the same insert was deduplicated against rows that do not
-- exist. Deduplicated inserts are rejected inside transactions.
-- https://github.com/ClickHouse/ClickHouse/issues/122275

SET async_insert = 0;
SET deduplicate_insert = 'enable';

DROP TABLE IF EXISTS t_dedup_txn;
DROP TABLE IF EXISTS t_no_dedup_txn;

CREATE TABLE t_dedup_txn (x UInt64) ENGINE = MergeTree ORDER BY x
    SETTINGS non_replicated_deduplication_window = 100;

BEGIN TRANSACTION;
-- When an `INSERT` fails, the client reconnects, so the transaction ends with the old session and needs no `ROLLBACK`.
INSERT INTO t_dedup_txn VALUES (1), (2); -- { serverError NOT_IMPLEMENTED }

INSERT INTO t_dedup_txn SETTINGS implicit_transaction = 1 VALUES (1), (2); -- { serverError NOT_IMPLEMENTED }

SELECT 'after the rejected inserts', count() FROM t_dedup_txn;

-- Reload the deduplication log from the disk.
DETACH TABLE t_dedup_txn;
ATTACH TABLE t_dedup_txn;

-- The same insert outside of a transaction is not deduplicated against the rejected ones, and its retry is.
INSERT INTO t_dedup_txn VALUES (1), (2);
INSERT INTO t_dedup_txn VALUES (1), (2);
SELECT 'outside of a transaction', arraySort(groupArray(x)) FROM t_dedup_txn;

-- An insert without deduplication is allowed inside a transaction, and it is rolled back with it.
BEGIN TRANSACTION;
INSERT INTO t_dedup_txn SETTINGS deduplicate_insert = 'disable' VALUES (3);
SELECT 'inside a transaction', arraySort(groupArray(x)) FROM t_dedup_txn;
ROLLBACK;
SELECT 'after the rollback', arraySort(groupArray(x)) FROM t_dedup_txn;

-- A table without the deduplication window is not affected.
CREATE TABLE t_no_dedup_txn (x UInt64) ENGINE = MergeTree ORDER BY x;
BEGIN TRANSACTION;
INSERT INTO t_no_dedup_txn VALUES (1), (2);
COMMIT;
SELECT 'without the deduplication window', arraySort(groupArray(x)) FROM t_no_dedup_txn;

DROP TABLE t_dedup_txn;
DROP TABLE t_no_dedup_txn;
