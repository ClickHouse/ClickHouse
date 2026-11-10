-- Tags: no-ordinary-database, no-replicated-database

DROP TABLE IF EXISTS t_async_insert_select_txn;
CREATE TABLE t_async_insert_select_txn (n UInt64) ENGINE = MergeTree ORDER BY n;

SET async_insert = 1, wait_for_async_insert = 1;

-- An async `INSERT ... SELECT` inside a transaction is unsupported, just like `INSERT ... VALUES`.
INSERT INTO t_async_insert_select_txn SELECT number FROM numbers(3) SETTINGS implicit_transaction = 1; -- { serverError NOT_IMPLEMENTED }

BEGIN TRANSACTION;
INSERT INTO t_async_insert_select_txn SELECT number FROM numbers(3); -- { serverError NOT_IMPLEMENTED }
ROLLBACK;

-- Opting out of the exception falls back to the synchronous insert.
INSERT INTO t_async_insert_select_txn SELECT number FROM numbers(3) SETTINGS implicit_transaction = 1, throw_on_unsupported_query_inside_transaction = 0;
SELECT count() FROM t_async_insert_select_txn;

DROP TABLE t_async_insert_select_txn;
