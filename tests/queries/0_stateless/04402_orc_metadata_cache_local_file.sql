-- Tags: no-fasttest, no-parallel, no-parallel-replicas, no-random-settings
-- no-fasttest: ORC support is not built in fast-test images
-- no-parallel: cache is system-wide and concurrent tests would race on hits/misses
-- no-parallel-replicas: profile events are not available on the second replica
-- no-random-settings: we need a stable interaction of cache-related settings

-- The ORC metadata cache must also be consulted for local files read through
-- the `file` table function, not only for object-storage backends.

SET log_queries = 1;
SET engine_file_truncate_on_insert = 1;
-- Pin parallelism so the hit/miss counts are deterministic.
SET max_threads = 1;
-- `StorageFile` keeps a per-path row-count schema cache; with `count()` the
-- second query is served from that cache and never reaches the ORC input
-- format, so the metadata-cache hit/miss counters would stay at 0/0. Disable
-- it so every query goes through `getInputWithMetadata` and the ORC metadata
-- cache is actually consulted.
SET use_cache_for_count_from_files = 0;

INSERT INTO FUNCTION file(currentDatabase() || '_04402_local.orc', ORC, 'id UInt64, name String')
SELECT number, toString(number) FROM numbers(100);

SYSTEM DROP ORC METADATA CACHE;

-- q1: fresh cache -> expect a miss
SELECT count() FROM file(currentDatabase() || '_04402_local.orc')
    SETTINGS log_comment = '04402-q1', use_orc_metadata_cache = 1
    FORMAT Null;

-- q2: same file -> expect a hit
SELECT count() FROM file(currentDatabase() || '_04402_local.orc')
    SETTINGS log_comment = '04402-q2', use_orc_metadata_cache = 1
    FORMAT Null;

-- q3: rewrite the file in place -> expect a miss again, because the cache key
-- includes sub-second mtime, inode and size, so an in-place rewrite invalidates
-- the entry even when the new file has the same length and is written within
-- the same wall-clock second.
INSERT INTO FUNCTION file(currentDatabase() || '_04402_local.orc', ORC, 'id UInt64, name String')
SELECT number + 1000, toString(number) FROM numbers(100);

SELECT count() FROM file(currentDatabase() || '_04402_local.orc')
    SETTINGS log_comment = '04402-q3', use_orc_metadata_cache = 1
    FORMAT Null;

-- q4: cache disabled at query level -> the cache must not be consulted at all,
-- so both the hit and miss counters stay at 0 even though a valid entry exists.
SELECT count() FROM file(currentDatabase() || '_04402_local.orc')
    SETTINGS log_comment = '04402-q4', use_orc_metadata_cache = 0
    FORMAT Null;

SYSTEM DROP ORC METADATA CACHE;

-- q5: non-seekable read path (input_format_allow_seeks = 0 forces the
-- load-the-whole-file-into-memory fallback). On a cache miss the footer is read
-- from a buffer that gets fully drained, so the reader must be built exactly once
-- and reused; otherwise a second construction would fail with "Not an ORC file".
-- Expect a miss here.
SELECT count() FROM file(currentDatabase() || '_04402_local.orc')
    SETTINGS log_comment = '04402-q5', use_orc_metadata_cache = 1, input_format_allow_seeks = 0
    FORMAT Null;

-- q6: same file on the non-seekable path -> expect a hit.
SELECT count() FROM file(currentDatabase() || '_04402_local.orc')
    SETTINGS log_comment = '04402-q6', use_orc_metadata_cache = 1, input_format_allow_seeks = 0
    FORMAT Null;

SYSTEM FLUSH LOGS query_log;

SELECT ProfileEvents['ORCMetadataCacheHits'], ProfileEvents['ORCMetadataCacheMisses']
    FROM system.query_log
    WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = '04402-q1';

SELECT ProfileEvents['ORCMetadataCacheHits'], ProfileEvents['ORCMetadataCacheMisses']
    FROM system.query_log
    WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = '04402-q2';

SELECT ProfileEvents['ORCMetadataCacheHits'], ProfileEvents['ORCMetadataCacheMisses']
    FROM system.query_log
    WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = '04402-q3';

SELECT ProfileEvents['ORCMetadataCacheHits'], ProfileEvents['ORCMetadataCacheMisses']
    FROM system.query_log
    WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = '04402-q4';

SELECT ProfileEvents['ORCMetadataCacheHits'], ProfileEvents['ORCMetadataCacheMisses']
    FROM system.query_log
    WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = '04402-q5';

SELECT ProfileEvents['ORCMetadataCacheHits'], ProfileEvents['ORCMetadataCacheMisses']
    FROM system.query_log
    WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = '04402-q6';
