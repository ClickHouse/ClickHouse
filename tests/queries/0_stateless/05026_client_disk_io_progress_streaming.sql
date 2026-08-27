-- Tags: no-parallel-replicas, no-object-storage, no-darwin
-- no-darwin: min_bytes_to_use_direct_io relies on O_DIRECT, which is not supported on macOS, and
--   the OS-level bytes counters OSReadBytes/OSWriteBytes come from Linux taskstats only.

-- Verifies the data behind clickhouse-client's live IO progress meters
-- (https://github.com/ClickHouse/ClickHouse/issues/116565): the OS-level `OSReadBytes`/`OSWriteBytes`
-- counters must reach the client in the streamed ProfileEvents. ProgressIndication turns these
-- per-interval deltas into the live `<rate>/s read IO` / `<rate>/s write IO` in the progress line.
-- The TTY rendering itself is not portable to assert here, so this pins the server-side counter
-- increment the feature rides on.

SET log_queries = 1, log_query_threads = 1;

DROP TABLE IF EXISTS client_io_progress;

-- min_bytes_for_full_part_storage=0 forces full storage so the read goes through the
-- pread_threadpool/O_DIRECT path this test checks.
CREATE TABLE client_io_progress (key UInt32, val String) ENGINE = MergeTree ORDER BY key
    SETTINGS min_bytes_for_full_part_storage = 0
AS SELECT number, 'val-' || number FROM numbers(1000000);

-- A direct-I/O read bypasses the page cache, so OSReadBytes increments regardless of cache warmth.
SELECT * FROM client_io_progress FORMAT Null
SETTINGS
    local_filesystem_read_method = 'pread_threadpool',
    min_bytes_to_use_direct_io = 1,
    use_uncompressed_cache = 0;

SYSTEM FLUSH LOGS query_log, query_thread_log;

WITH queries AS (
    SELECT query_id
    FROM system.query_log
    WHERE event_date >= yesterday() AND event_time >= now() - 600 AND type = 'QueryFinish'
        AND current_database = currentDatabase()
        AND query_kind = 'Select'
        AND Settings['min_bytes_to_use_direct_io'] = '1'
)
SELECT 'direct-io read produced OSReadBytes',
    sum(qtl.ProfileEvents['OSReadBytes']) > 0
FROM system.query_thread_log qtl
WHERE qtl.event_date >= yesterday() AND qtl.event_time >= now() - 600
    AND current_database = currentDatabase()
    AND qtl.query_id IN (SELECT query_id FROM queries);

DROP TABLE client_io_progress;
