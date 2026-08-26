-- Tags: no-fasttest, no-distributed-cache, no-encrypted-storage
-- The experimental ReaderExecutor's long-connection reuse: on a sequential scan of an
-- object-storage table (storage_policy='s3_no_cache', so the executor actually engages), a held
-- source connection is opened and reused across windows -- emitting the ReaderExecutorLongConnection* metrics --
-- and the reuse is gated by `reader_executor_use_long_connections`. Two separate tables keep both
-- scans cold (a re-scan of the same table would be served from cache and never reach the source).
--   no-fasttest: needs minio (object storage).
--   no-distributed-cache / no-encrypted-storage: the executor falls back on those stages
--   (which can't be disabled from the test), so its metrics would not be emitted there.

DROP TABLE IF EXISTS t_reader_executor_lc_on;
DROP TABLE IF EXISTS t_reader_executor_lc_off;

CREATE TABLE t_reader_executor_lc_on (id UInt64, v UInt64, s String)
ENGINE = MergeTree ORDER BY id
SETTINGS storage_policy = 's3_no_cache', index_granularity = 8192, min_bytes_for_wide_part = 0;

CREATE TABLE t_reader_executor_lc_off AS t_reader_executor_lc_on;

INSERT INTO t_reader_executor_lc_on SELECT number, number * 2, repeat('x', 64) FROM numbers(500000);
INSERT INTO t_reader_executor_lc_off SELECT number, number * 2, repeat('x', 64) FROM numbers(500000);

SET use_reader_executor = 1;
SET remote_filesystem_read_method = 'read';   -- avoid the async-prefetch stage
SET enable_filesystem_cache = 0;               -- avoid the filesystem-cache stage so the executor engages
SET max_read_buffer_size = 65536;              -- small windows -> many sequential reads per object

-- Keep the scan contiguous so reuse is deterministic. The `ReaderExecutorLongConnectionHits > 0` assertion below
-- needs the held connection to serve more than one window, which only happens on contiguous forward
-- access (the `canContinue` gate). Two randomized settings can fragment the access pattern and leave
-- `ReaderExecutorLongConnectionHits` at 0: `max_threads` > 1 stripes non-adjacent mark ranges across per-thread
-- readers, and the range-split injection reorders mark ranges within a single reader. Pin both.
SET max_threads = 1;
SET merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0;

-- Long connections ON: a sequential column scan opens a held source connection and reuses it.
SELECT sum(id) + sum(v) + sum(length(s)) FROM t_reader_executor_lc_on
SETTINGS reader_executor_use_long_connections = 1, log_comment = '04341_long_conn_on' FORMAT Null;

-- Long connections OFF: the same scan takes the stateless one-shot-per-window path.
SELECT sum(id) + sum(v) + sum(length(s)) FROM t_reader_executor_lc_off
SETTINGS reader_executor_use_long_connections = 0, log_comment = '04341_long_conn_off' FORMAT Null;

SYSTEM FLUSH LOGS query_log;

-- ON: a long connection was opened and reused (a held connection serves more than one window), the
-- held path served real bytes, and over-read is expected -- gap-bridging reads past the requested
-- window, so source bytes are at least the requested bytes (the inverse of 04327's strict equality).
SELECT
    ProfileEvents['ReaderExecutorLongConnectionOpened'] > 0,
    ProfileEvents['ReaderExecutorLongConnectionHits'] > 0,
    ProfileEvents['ReaderExecutorLongConnectionBytes'] > 0,
    ProfileEvents['ReaderExecutorBytesFromSource'] >= ProfileEvents['ReaderExecutorRequestedBytes']
FROM system.query_log
WHERE log_comment = '04341_long_conn_on' AND type = 'QueryFinish' AND current_database = currentDatabase()
ORDER BY event_time_microseconds DESC LIMIT 1;

-- OFF: the executor still read from the source, but the setting gated long connections off.
SELECT
    ProfileEvents['ReaderExecutorSourceRequests'] > 0,
    ProfileEvents['ReaderExecutorLongConnectionOpened'] = 0
FROM system.query_log
WHERE log_comment = '04341_long_conn_off' AND type = 'QueryFinish' AND current_database = currentDatabase()
ORDER BY event_time_microseconds DESC LIMIT 1;

DROP TABLE t_reader_executor_lc_on;
DROP TABLE t_reader_executor_lc_off;
