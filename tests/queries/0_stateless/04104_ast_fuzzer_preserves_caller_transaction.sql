-- Tags: no-ordinary-database, no-encrypted-storage
-- Regression test for a TSAN data race in `executeASTFuzzerQueries` (STID: 2604-385d).
--
-- `executeASTFuzzerQueries` used to reset the transaction on the CALLER's query and
-- session context before creating fuzz copies:
--
--     context->getQueryContext()->getSessionContext()->setCurrentTransaction(...)
--     context->setCurrentTransaction(...)
--
-- That mutation was unsynchronized, racing with concurrent readers of the same context
-- (for example, `RESTORE ASYNC` background workers calling `Context::createCopy` under
-- the shared `Context::mutex`). It also had the surprising side effect of silently
-- clearing the user's active transaction.
--
-- The reset now happens on the fuzz session context COPY (after `makeSessionContext`),
-- which is not visible to any other thread, so the caller's transaction state is
-- preserved. This test locks that behavior in.

-- Make sure the test itself controls where the fuzzer runs (stress test profile sets
-- `ast_fuzzer_runs=5`; we pin our baseline so only queries that explicitly opt in fire
-- the finish callback).
SET ast_fuzzer_runs = 0;
SET ast_fuzzer_any_query = 0;

-- Async inserts are not supported inside transactions; disable so the test does not
-- depend on `disable_async_inserts.xml` being applied to the server config.
SET async_insert = 0;

-- Suppress error-level log messages from fuzzed queries that fail expectedly.
SET send_logs_level = 'fatal';

DROP TABLE IF EXISTS mt_txn_fuzz;
CREATE TABLE mt_txn_fuzz (n Int64) ENGINE = MergeTree ORDER BY n;

-- Start a transaction, run a SELECT with `ast_fuzzer_runs > 0` (which fires
-- `executeASTFuzzerQueries` in the finish callback), then verify the transaction
-- is still alive by doing an INSERT + COMMIT. With the buggy behavior, the fuzzer
-- would clear the session's `merge_tree_transaction` and the subsequent INSERT
-- would run outside the transaction (or COMMIT would fail with INVALID_TRANSACTION).
BEGIN TRANSACTION;
INSERT INTO mt_txn_fuzz VALUES (1);
SELECT n FROM mt_txn_fuzz ORDER BY n SETTINGS ast_fuzzer_runs = 3;
INSERT INTO mt_txn_fuzz VALUES (2);
COMMIT;

-- Both inserts should be visible (the transaction survived the fuzzer).
SELECT 'committed', arraySort(groupArray(n)) FROM mt_txn_fuzz;

-- A subsequent transaction must also work (the session isn't stuck in some
-- weird state left over from the fuzzer's mutation of the parent context).
BEGIN TRANSACTION;
INSERT INTO mt_txn_fuzz VALUES (3);
SELECT n FROM mt_txn_fuzz ORDER BY n SETTINGS ast_fuzzer_runs = 3;
ROLLBACK;

-- Rollback must have dropped the value 3 (the transaction was real, not a
-- no-op because the fuzzer ate it).
SELECT 'rolled-back', arraySort(groupArray(n)) FROM mt_txn_fuzz;

DROP TABLE mt_txn_fuzz;
