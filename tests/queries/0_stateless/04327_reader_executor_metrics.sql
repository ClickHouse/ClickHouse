-- Tags: no-distributed-cache, no-encrypted-storage
-- Like 04316, the executor falls back on the distributed cache and decryption
-- (which can't be disabled from the test), so its metrics would not be emitted on
-- those storage configs. Skip them; the test still runs on local disk and plain
-- object storage where the executor engages.
--
-- Checks that the experimental ReaderExecutor emits its observability metrics.
-- Reads a MergeTree table with `use_reader_executor = 1` and verifies, via the
-- per-query ProfileEvents in `system.query_log`, that the live counters moved and
-- the KPI inputs hold their invariants. Long connections are off here so the
-- base-executor metrics stay deterministic (no gap-bridging over-read, no held
-- connections to drop) under randomized settings like `max_threads`; the reuse
-- path is covered by 04341/04342 and the gtests. The exact modeled-cost formula
-- and the KPI ratio are checked deterministically in the gtest (single executor);
-- here the per-executor integer rounding makes only sign / relational checks reliable.

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
-- Base-executor metrics only: long-connection reuse (covered by 04341/04342) would add
-- gap-bridging over-read and incomplete-connection counts that swing with max_threads.
SET reader_executor_use_long_connections = 0;
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
--   3: requested bytes == source bytes (no over-read: gap-bridging needs long connections, off here)
--   4: total work time was recorded
--   5: modeled cost >= 30ms-per-source-request floor (the byte term only adds to it)
--   6,7: cache get / cache populate stay 0 (not implemented)
--   8: incomplete connections stay 0 (no held connections to drop with long connections off)
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
