-- Tags: no-distributed-cache, no-encrypted-storage, no-parallel-replicas
-- Like 04316, the executor falls back on the distributed cache and decryption
-- (which can't be disabled from the test), so its metrics would not be emitted on
-- those storage configs. Skip them; the test still runs on local disk and plain
-- object storage where the executor engages.
--   no-parallel-replicas: the counters are incremented on whichever replica reads the
--   mark, so the initiator's `query_log` row does not carry them.
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
-- Wide part, one file per column. In a Compact part on object storage the multi-buffer reader reads each column
-- with its own stream, and its request map spans the chunks of the other columns, so the stream reads them too.
SETTINGS index_granularity = 8192, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, min_bytes_for_full_part_storage = 0;

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
-- One thread: the request map is per part, so a thread fetches past its share into the next one.
SELECT count(), sum(id), sum(v), sum(length(s)) FROM t_reader_executor_metrics
SETTINGS log_comment = '04327_reader_executor_metrics_probe', max_threads = 1 FORMAT Null;

SYSTEM FLUSH LOGS query_log;

-- Per-query ProfileEvents for the marked query, scoped to this test's database. Every check must be 1.
-- The modeled cost has a 30 ms floor per source request; its byte term only adds to it.
-- Cache get / populate are not implemented. With long connections off, no held connection is dropped.
SELECT
    ProfileEvents['ReaderExecutorSourceRequests'] > 0 AS made_source_requests,
    ProfileEvents['ReaderExecutorBytesFromSource'] > 0 AS read_bytes_from_source,
    ProfileEvents['ReaderExecutorDeliveredBytes'] = ProfileEvents['ReaderExecutorBytesFromSource'] AS delivered_every_source_byte,
    ProfileEvents['ReaderExecutorWorkMicroseconds'] > 0 AS recorded_work_time,
    ProfileEvents['ReaderExecutorModeledCostMicroseconds'] >= 30000 * ProfileEvents['ReaderExecutorSourceRequests']
        AS modeled_cost_at_least_30ms_per_request,
    ProfileEvents['ReaderExecutorCacheGetRequests'] = 0 AS no_cache_gets,
    ProfileEvents['ReaderExecutorCachePopulateRequests'] = 0 AS no_cache_populates,
    ProfileEvents['ReaderExecutorIncompleteConnections'] = 0 AS no_dropped_connections
FROM system.query_log
WHERE log_comment = '04327_reader_executor_metrics_probe'
  AND type = 'QueryFinish'
  AND current_database = currentDatabase()
ORDER BY event_time_microseconds DESC
LIMIT 1
FORMAT Vertical;

DROP TABLE t_reader_executor_metrics;
