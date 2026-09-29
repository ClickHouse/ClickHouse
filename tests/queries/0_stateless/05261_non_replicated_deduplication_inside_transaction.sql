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

-- `INSERT SELECT` is used inside the explicit transaction, because the client keeps the connection after its error,
-- so the failed transaction stays attached to the session until `ROLLBACK`. After a failed `INSERT ... VALUES` the
-- client disconnects when it sent the data itself, and the transaction is lost with the session, but it keeps the
-- connection when the server parsed the data (`send_table_structure_on_insert_with_inline_data = 0`, randomized in
-- the tests). The token makes the deduplication of `INSERT SELECT` independent of whether the `SELECT` is sorted.
BEGIN TRANSACTION;
INSERT INTO t_dedup_txn SETTINGS deduplicate_insert_select = 'force_enable', insert_deduplication_token = 'rejected'
    SELECT number + 1 FROM numbers(2); -- { serverError NOT_IMPLEMENTED }
ROLLBACK;

-- The rejection does not depend on `throw_on_unsupported_query_inside_transaction`: running the insert would lose rows.
BEGIN TRANSACTION;
INSERT INTO t_dedup_txn SETTINGS deduplicate_insert_select = 'force_enable', insert_deduplication_token = 'rejected',
    throw_on_unsupported_query_inside_transaction = 0 SELECT number + 1 FROM numbers(2); -- { serverError NOT_IMPLEMENTED }
ROLLBACK;

-- The implicit transaction is rolled back by the server with its query, whether or not the client reconnects.
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
