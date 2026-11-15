-- Tags: no-object-storage, no-parallel, no-fasttest
-- no-object-storage: extra S3 threads affect peak_threads_usage
-- no-parallel: peak_threads_usage is sensitive to concurrent queries
-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/106845

SET max_threads = 10;
SET max_block_size = 10;

DROP TABLE IF EXISTS pvp_source;
DROP TABLE IF EXISTS pvp_target;

CREATE TABLE pvp_source (n UInt64) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE pvp_target (n UInt64, mv UInt8, s UInt8) ENGINE = MergeTree ORDER BY tuple();

CREATE MATERIALIZED VIEW pvp_mv1 TO pvp_target AS SELECT n, 1 AS mv, sleep(0.1) AS s FROM pvp_source;
CREATE MATERIALIZED VIEW pvp_mv2 TO pvp_target AS SELECT n, 2 AS mv, sleep(0.1) AS s FROM pvp_source;
CREATE MATERIALIZED VIEW pvp_mv3 TO pvp_target AS SELECT n, 3 AS mv, sleep(0.1) AS s FROM pvp_source;

-- serial: parallel_view_processing=0
INSERT INTO pvp_source SETTINGS
    log_queries = 1,
    async_insert = 0,
    insert_deduplication_token = 'pvp_test_serial_values',
    parallel_view_processing = 0
VALUES (1);

-- parallel: parallel_view_processing=1
INSERT INTO pvp_source SETTINGS
    log_queries = 1,
    async_insert = 0,
    insert_deduplication_token = 'pvp_test_parallel_values',
    parallel_view_processing = 1
VALUES (2);

-- INSERT ... SELECT with parallel_view_processing=0 → serial
INSERT INTO pvp_source SETTINGS
    log_queries = 1,
    async_insert = 0,
    insert_deduplication_token = 'pvp_test_serial_select',
    parallel_view_processing = 0
SELECT number FROM numbers(1);

-- INSERT ... SELECT with parallel_view_processing=1 → parallel
INSERT INTO pvp_source SETTINGS
    log_queries = 1,
    async_insert = 0,
    insert_deduplication_token = 'pvp_test_parallel_select',
    parallel_view_processing = 1
SELECT number FROM numbers(1);

SYSTEM FLUSH LOGS system.query_log;

-- `peak_threads_usage` is an exact count of threads that concurrently attached to the query's
-- thread group (see `ThreadGroup::linkThread`), not a sampled or timed value, so unlike
-- wall-clock duration it is not affected by interpreter/sanitizer overhead on debug/ASan/MSan/TSan
-- builds: serial (views run one after another) attaches 1-2 threads, parallel (3 views fanned
-- out concurrently) attaches at least 3.
SELECT
    Settings['insert_deduplication_token'] AS token,
    if(max(peak_threads_usage) >= 3, 'parallel', 'serial') AS mode
FROM system.query_log
WHERE
    event_date >= yesterday()
    AND event_time >= now() - 600
    AND current_database = currentDatabase()
    AND type != 'QueryStart'
    AND query_kind = 'Insert'
    AND Settings['insert_deduplication_token'] IN (
        'pvp_test_serial_values', 'pvp_test_parallel_values',
        'pvp_test_serial_select', 'pvp_test_parallel_select')
GROUP BY token
ORDER BY token;

DROP VIEW pvp_mv1;
DROP VIEW pvp_mv2;
DROP VIEW pvp_mv3;
DROP TABLE pvp_target;
DROP TABLE pvp_source;
