-- Tags: no-random-settings

SET enable_analyzer = 1;
SET log_queries = 1;
SET optimize_read_in_order = 1;
SET parallel_replicas_local_plan = 1;
SET query_plan_optimize_lazy_materialization = 1;
SET query_plan_max_limit_for_lazy_materialization = 10000;

DROP TABLE IF EXISTS lazy_materialization_uncompressed_cache;

CREATE TABLE lazy_materialization_uncompressed_cache
(
    id UInt64,
    payload String
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO lazy_materialization_uncompressed_cache
SELECT number, repeat('x', 1024)
FROM numbers(50000);

SYSTEM DROP UNCOMPRESSED CACHE;

SELECT count() > 0
FROM
(
    SELECT trimLeft(explain) AS explain
    FROM
    (
        EXPLAIN PLAN actions=1
        SELECT id, payload
        FROM lazy_materialization_uncompressed_cache
        WHERE id >= 0
        ORDER BY id
        LIMIT 5000
        SETTINGS max_threads = 1
    )
)
WHERE explain LIKE '%LazilyRead%'
   OR explain LIKE '%Lazily read columns:%';

SELECT id, payload
FROM lazy_materialization_uncompressed_cache
WHERE id >= 0
ORDER BY id
LIMIT 5000
FORMAT Null
SETTINGS max_threads = 1, log_comment = '04041_lazy_materialization_uncompressed_cache_run_1';

SYSTEM FLUSH LOGS query_log;

SELECT ProfileEvents['UncompressedCacheMisses'] > 0
FROM system.query_log
WHERE event_date >= yesterday()
  AND event_time >= now() - INTERVAL 10 MINUTE
  AND type = 'QueryFinish'
  AND current_database = currentDatabase()
  AND log_comment = '04041_lazy_materialization_uncompressed_cache_run_1'
ORDER BY event_time_microseconds DESC
LIMIT 1;

SELECT id, payload
FROM lazy_materialization_uncompressed_cache
WHERE id >= 0
ORDER BY id
LIMIT 5000
FORMAT Null
SETTINGS max_threads = 1, log_comment = '04041_lazy_materialization_uncompressed_cache_run_2';

SYSTEM FLUSH LOGS query_log;

SELECT ProfileEvents['UncompressedCacheHits'] > 0
FROM system.query_log
WHERE event_date >= yesterday()
  AND event_time >= now() - INTERVAL 10 MINUTE
  AND type = 'QueryFinish'
  AND current_database = currentDatabase()
  AND log_comment = '04041_lazy_materialization_uncompressed_cache_run_2'
ORDER BY event_time_microseconds DESC
LIMIT 1;

DROP TABLE lazy_materialization_uncompressed_cache;
