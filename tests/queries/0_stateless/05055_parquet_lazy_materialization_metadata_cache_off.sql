-- Tags: no-fasttest, no-parallel, no-parallel-replicas, no-random-settings
-- no-fasttest: Parquet support is not built in fast-test images
-- no-parallel: the Parquet metadata cache is system-wide and concurrent tests would race on hits/misses
-- no-parallel-replicas: profile events are not available on the second replica
-- no-random-settings: we need a stable interaction of cache-related settings

-- `use_parquet_metadata_cache = 0` must hold for every pass of the read, including the
-- deferred pass of lazy materialization for `File`, which reopens the file on its own.
-- Otherwise the meaning of the setting would depend on the shape of the query plan.

SET log_queries = 1;
SET engine_file_truncate_on_insert = 1;
SET use_cache_for_count_from_files = 0;
SET enable_analyzer = 1;
SET query_plan_optimize_lazy_materialization = 1;
SET query_plan_max_limit_for_lazy_materialization = 0;
SET query_plan_optimize_lazy_materialization_for_file = 1;

INSERT INTO FUNCTION file(currentDatabase() || '_05052.parquet', Parquet, 'k UInt64, payload String')
SELECT number, repeat(toString(number), 32) FROM numbers(1000)
SETTINGS output_format_parquet_row_group_size = 100;

-- The deferred pass is what this test is about, so assert the plan really takes it.
SELECT count() > 0
FROM (EXPLAIN SELECT k, payload FROM file(currentDatabase() || '_05052.parquet', Parquet, 'k UInt64, payload String') ORDER BY intHash64(k) LIMIT 5)
WHERE explain LIKE '%LazilyReadFromFile%';

SYSTEM DROP PARQUET METADATA CACHE;

-- q1: the cache is disabled - neither the main nor the deferred pass may consult or populate it.
SELECT k, payload FROM file(currentDatabase() || '_05052.parquet', Parquet, 'k UInt64, payload String')
ORDER BY intHash64(k) LIMIT 5
SETTINGS log_comment = '05052-q1', use_parquet_metadata_cache = 0
FORMAT Null;

-- q2: the same query with the cache enabled does consult it - the control for q1.
SELECT k, payload FROM file(currentDatabase() || '_05052.parquet', Parquet, 'k UInt64, payload String')
ORDER BY intHash64(k) LIMIT 5
SETTINGS log_comment = '05052-q2', use_parquet_metadata_cache = 1
FORMAT Null;

SYSTEM FLUSH LOGS query_log;

SELECT ProfileEvents['ParquetMetadataCacheHits'] + ProfileEvents['ParquetMetadataCacheMisses']
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = '05052-q1';

SELECT ProfileEvents['ParquetMetadataCacheHits'] + ProfileEvents['ParquetMetadataCacheMisses'] > 0
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = '05052-q2';
