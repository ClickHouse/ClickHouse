-- Tags: no-distributed-cache, no-encrypted-storage
-- Like 04316, the executor falls back on the distributed cache and decryption
-- (which can't be disabled from the test), so its metrics would not be emitted on
-- those storage configs. Skip them; the test still runs on local disk and plain
-- object storage where the executor engages.
--
-- Checks that the experimental ReaderExecutor emits its observability metrics.
-- Reads a MergeTree table with `use_reader_executor = 1` and verifies, via the
-- per-query ProfileEvents in `system.query_log`, that the live counters moved,
-- the KPI inputs hold their invariants, and the not-yet-implemented inputs
-- (cache / connection reuse) stay 0. The exact modeled-cost formula and the KPI
-- ratio are checked deterministically in the gtest (single executor); here the
-- per-executor integer rounding makes only sign / relational checks reliable.

DROP TABLE IF EXISTS t_reader_executor_metrics;

CREATE TABLE t_reader_executor_metrics
(
    id UInt64,
    v UInt64,
    s String
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 8192;

INSERT INTO t_reader_executor_metrics
SELECT number, number * 2, concat('row_', toString(number))
FROM numbers(300000);

SET use_reader_executor = 1;
-- Disable the stages the executor does not implement so it actually runs on
-- object storage too (no-ops on local disk).
SET remote_filesystem_read_method = 'read';
SET enable_filesystem_cache = 0;

-- Full scan; marked so the check below finds it by query id. Its result is
-- irrelevant (it only drives the executor), so discard it with FORMAT Null.
SELECT count(), sum(id), sum(v), sum(length(s)) FROM t_reader_executor_metrics
SETTINGS log_comment = '04327_reader_executor_metrics_probe' FORMAT Null;

SYSTEM FLUSH LOGS query_log;

-- Per-query ProfileEvents for the marked query, scoped to this test's database.
-- Columns (all expected 1):
--   1: source requests happened
--   2: bytes were read from source
--   3: requested bytes == source bytes (no over-read / cache divergence yet)
--   4: total work time was recorded
--   5: modeled cost >= 30ms-per-source-request floor (the byte term only adds to it)
--   6,7,8: cache get / cache populate / incomplete connections stay 0 (not implemented)
SELECT
    ProfileEvents['ReaderExecutorSourceRequests'] > 0,
    ProfileEvents['ReaderExecutorBytesFromSource'] > 0,
    ProfileEvents['ReaderExecutorRequestedBytes'] = ProfileEvents['ReaderExecutorBytesFromSource'],
    ProfileEvents['ReaderExecutorWorkMicroseconds'] > 0,
    ProfileEvents['ReaderExecutorModeledCostMicroseconds'] >= 30000 * ProfileEvents['ReaderExecutorSourceRequests'],
    ProfileEvents['ReaderExecutorCacheGetRequests'] = 0,
    ProfileEvents['ReaderExecutorCachePopulateRequests'] = 0,
    ProfileEvents['ReaderExecutorIncompleteConnections'] = 0
FROM system.query_log
WHERE log_comment = '04327_reader_executor_metrics_probe'
  AND type = 'QueryFinish'
  AND current_database = currentDatabase()
ORDER BY event_time_microseconds DESC
LIMIT 1;

DROP TABLE t_reader_executor_metrics;
