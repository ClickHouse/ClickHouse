-- Tags: no-parallel, no-random-settings, no-object-storage

SET enable_analyzer = 1;
DROP TABLE IF EXISTS t_index_hint;

CREATE TABLE t_index_hint (a UInt64, b UInt64)
ENGINE = MergeTree ORDER BY a
SETTINGS index_granularity = 1, min_bytes_for_wide_part = 0, serialization_info_version = 'basic';

INSERT INTO t_index_hint SELECT number, number FROM numbers(1000);

SYSTEM DROP MARK CACHE;
SELECT sum(b) FROM t_index_hint WHERE b >= 100 AND b < 200 SETTINGS max_threads = 1;

SYSTEM DROP MARK CACHE;
SELECT sum(b) FROM t_index_hint WHERE a >= 100 AND a < 200 AND b >= 100 AND b < 200 SETTINGS max_threads = 1, force_primary_key = 1;

SYSTEM DROP MARK CACHE;
SELECT sum(b) FROM t_index_hint WHERE indexHint(a >= 100 AND a < 200) AND b >= 100 AND b < 200 SETTINGS max_threads = 1, force_primary_key = 1;

SYSTEM FLUSH LOGS query_log;

SELECT
    ProfileEvents['FileOpen'],
    read_rows,
    arraySort(arrayMap(x -> splitByChar('.', x)[-1], columns))
FROM system.query_log
WHERE type = 'QueryFinish'
    AND current_database = currentDatabase()
    AND query LIKE '%SELECT sum(b) FROM t_index_hint%'
ORDER BY event_time_microseconds;

DROP TABLE IF EXISTS t_index_hint;

CREATE TABLE t_index_hint
(
    a UInt64,
    s String,
    s_tokens Array(String) MATERIALIZED arrayDistinct(splitByWhitespace(s)),
    INDEX idx_tokens s_tokens TYPE bloom_filter(0.01) GRANULARITY 1,
)
ENGINE = MergeTree ORDER BY a
SETTINGS index_granularity = 1, min_bytes_for_wide_part = 0, serialization_info_version = 'basic';

INSERT INTO t_index_hint (a, s) VALUES (1, 'Text with my_token') (2, 'Another text');

SYSTEM DROP MARK CACHE;
SYSTEM DROP INDEX MARK CACHE;
SELECT count() FROM t_index_hint WHERE s LIKE '%my_token%' SETTINGS max_threads = 1;

SYSTEM DROP MARK CACHE;
SYSTEM DROP INDEX MARK CACHE;
SELECT count() FROM t_index_hint WHERE has(s_tokens, 'my_token') AND s LIKE '%my_token%' SETTINGS max_threads = 1, force_data_skipping_indices = 'idx_tokens';

SYSTEM DROP MARK CACHE;
SYSTEM DROP INDEX MARK CACHE;
SELECT count() FROM t_index_hint WHERE indexHint(has(s_tokens, 'my_token')) AND s LIKE '%my_token%' SETTINGS max_threads = 1, force_data_skipping_indices = 'idx_tokens';

SYSTEM FLUSH LOGS query_log;

SELECT
    ProfileEvents['FileOpen'],
    read_rows,
    arraySort(arrayMap(x -> splitByChar('.', x)[-1], columns))
FROM system.query_log
WHERE type = 'QueryFinish'
    AND current_database = currentDatabase()
    AND query LIKE '%SELECT count() FROM t_index_hint%'
ORDER BY event_time_microseconds;

DROP TABLE t_index_hint;
