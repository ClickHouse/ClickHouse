-- Tags: no-parallel, no-fasttest, no-replicated-database, no-ordinary-database
-- no-parallel: the failpoint is server-wide.

-- A rolled back transaction must take the part it created out of the active set even when the
-- version metadata of its parts cannot be written. Otherwise the created part stays active next to
-- the part it replaced: a read returns every row twice, and a non-transactional OPTIMIZE fails with
-- `Part ... contains previous part ...`.

-- Lets `SYSTEM ... FAILPOINT` run inside the transaction.
SET throw_on_unsupported_query_inside_transaction = 0;
SET optimize_throw_if_noop = 0;
-- The rollback logs each injected failure as an error.
SET send_logs_level = 'fatal';

DROP TABLE IF EXISTS t_rollback_store_fail;
CREATE TABLE t_rollback_store_fail (n Int32) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_rollback_store_fail VALUES (1), (2);

BEGIN TRANSACTION;
OPTIMIZE TABLE t_rollback_store_fail FINAL;
SELECT 'in transaction', name FROM system.parts WHERE database = currentDatabase() AND table = 't_rollback_store_fail' AND active;
SYSTEM ENABLE FAILPOINT version_metadata_on_disk_store_fail;
ROLLBACK;
SYSTEM DISABLE FAILPOINT version_metadata_on_disk_store_fail;

-- The writes did fail, for this table.
SELECT 'store failed', count() FROM system.errors
WHERE name = 'FAULT_INJECTED' AND last_error_message LIKE '%' || currentDatabase() || '.t_rollback_store_fail%';

SELECT 'after rollback', name FROM system.parts WHERE database = currentDatabase() AND table = 't_rollback_store_fail' AND active ORDER BY name;
SELECT 'rows', n FROM t_rollback_store_fail ORDER BY n;

-- Outside a transaction, so that merge selection takes the active parts as they are.
OPTIMIZE TABLE t_rollback_store_fail;
SELECT 'after optimize', name FROM system.parts WHERE database = currentDatabase() AND table = 't_rollback_store_fail' AND active ORDER BY name;

DROP TABLE t_rollback_store_fail;
