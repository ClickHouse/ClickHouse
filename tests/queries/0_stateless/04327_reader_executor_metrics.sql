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

-- Per-query ProfileEvents for the marked query, summed over every row of that query.
-- The counters are incremented on whichever replica reads the mark, so under parallel replicas they
-- land on secondary rows whose `current_database` is `default`. Resolve the initiator rows by
-- `current_database`, then sum over every row of those queries via `initial_query_id`.
-- Columns (all expected 1):
--   1: source requests happened
--   2: bytes were read from source
--   3: requested bytes == source bytes (no over-read: gap-bridging needs long connections, off here)
--   4: total work time was recorded
--   5: modeled cost >= 30ms-per-source-request floor (the byte term only adds to it)
--   6,7: cache get / cache populate stay 0 (not implemented)
--   8: incomplete connections stay 0 (no held connections to drop with long connections off)
WITH initial_query_ids AS
(
    SELECT query_id
    FROM system.query_log
    WHERE event_date >= yesterday() AND event_time >= now() - 600
      AND current_database = currentDatabase()
      AND type = 'QueryFinish'
      AND is_initial_query = 1
      AND log_comment = '04327_reader_executor_metrics_probe'
)
SELECT
    sum(ProfileEvents['ReaderExecutorSourceRequests']) > 0,
    sum(ProfileEvents['ReaderExecutorBytesFromSource']) > 0,
    sum(ProfileEvents['ReaderExecutorRequestedBytes']) = sum(ProfileEvents['ReaderExecutorBytesFromSource']),
    sum(ProfileEvents['ReaderExecutorWorkMicroseconds']) > 0,
    sum(ProfileEvents['ReaderExecutorModeledCostMicroseconds']) >= 30000 * sum(ProfileEvents['ReaderExecutorSourceRequests']),
    sum(ProfileEvents['ReaderExecutorCacheGetRequests']) = 0,
    sum(ProfileEvents['ReaderExecutorCachePopulateRequests']) = 0,
    sum(ProfileEvents['ReaderExecutorIncompleteConnections']) = 0
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600
  AND type = 'QueryFinish'
  AND initial_query_id IN (SELECT query_id FROM initial_query_ids);

DROP TABLE t_reader_executor_metrics;
