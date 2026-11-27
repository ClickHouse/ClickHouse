-- Tags: no-fasttest, no-parallel, no-parallel-replicas, no-random-settings
-- no-fasttest: Parquet support is not built in fast-test images
-- no-parallel: cache is system-wide and concurrent tests would race on hits/misses
-- no-parallel-replicas: profile events are not available on the second replica
-- no-random-settings: we need a stable interaction of cache-related settings

-- The Parquet metadata cache must also be consulted for local files read through
-- the `file` table function, not only for object-storage backends.

SET log_queries = 1;
SET engine_file_truncate_on_insert = 1;
-- Pin parallelism so the hit/miss counts are deterministic. With file-bucket
-- splitting (#104251), a single Parquet file can be read by `max_threads`
-- sources, so this also guards us from per-machine variance in core count.
SET max_threads = 1;
-- `StorageFile` keeps a per-path row-count schema cache; with `count()` the
-- second query is served from that cache and never reaches the Parquet input
-- format, so the metadata-cache hit/miss counters would stay at 0/0. Disable
-- it so every query goes through `getInputWithMetadata` and the Parquet
-- metadata cache is actually consulted.
SET use_cache_for_count_from_files = 0;

INSERT INTO FUNCTION file(currentDatabase() || '_04207_local.parquet', Parquet, 'id UInt64, name String')
SELECT number, toString(number) FROM numbers(100);

SYSTEM DROP PARQUET METADATA CACHE;

-- q1: fresh cache -> expect a miss
SELECT count() FROM file(currentDatabase() || '_04207_local.parquet')
    SETTINGS log_comment = '04207-q1', use_parquet_metadata_cache = 1
    FORMAT Null;

-- q2: same file -> expect a hit
SELECT count() FROM file(currentDatabase() || '_04207_local.parquet')
    SETTINGS log_comment = '04207-q2', use_parquet_metadata_cache = 1
    FORMAT Null;

-- q3: rewrite the file in place -> expect a miss again, because the cache key
-- includes sub-second mtime, inode and size, so an in-place rewrite invalidates
-- the entry even when the new file has the same length and is written within
-- the same wall-clock second.
INSERT INTO FUNCTION file(currentDatabase() || '_04207_local.parquet', Parquet, 'id UInt64, name String')
SELECT number + 1000, toString(number) FROM numbers(100);

SELECT count() FROM file(currentDatabase() || '_04207_local.parquet')
    SETTINGS log_comment = '04207-q3', use_parquet_metadata_cache = 1
    FORMAT Null;

SYSTEM FLUSH LOGS query_log;

SELECT ProfileEvents['ParquetMetadataCacheHits'], ProfileEvents['ParquetMetadataCacheMisses']
    FROM system.query_log
    WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = '04207-q1';

SELECT ProfileEvents['ParquetMetadataCacheHits'], ProfileEvents['ParquetMetadataCacheMisses']
    FROM system.query_log
    WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = '04207-q2';

SELECT ProfileEvents['ParquetMetadataCacheHits'], ProfileEvents['ParquetMetadataCacheMisses']
    FROM system.query_log
    WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = '04207-q3';
