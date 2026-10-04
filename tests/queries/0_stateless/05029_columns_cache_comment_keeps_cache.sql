-- Tags: no-parallel, no-random-settings, no-random-merge-tree-settings, no-replicated-database
-- The columns cache identifies the schema of an entry by the column list of the metadata
-- snapshot the reader uses, so that data deserialized under one schema is never served to a
-- query running with another one. That identity must not react to metadata changes which
-- leave the columns themselves alone: a `COMMENT COLUMN` keeps every cached entry usable.

SET use_columns_cache = 1;
SYSTEM DROP COLUMNS CACHE;

DROP TABLE IF EXISTS t_columns_cache_comment;

CREATE TABLE t_columns_cache_comment (id UInt64, a UInt64)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, index_granularity = 1000;

INSERT INTO t_columns_cache_comment SELECT number, number + 1000 FROM numbers(3000);

-- Populate the cache with the column `a`.
SELECT sum(a), count() FROM t_columns_cache_comment;

SELECT count() FROM system.columns_cache
WHERE database = currentDatabase() AND table = 't_columns_cache_comment' AND column = 'a';

ALTER TABLE t_columns_cache_comment COMMENT COLUMN a 'the answer';

-- The comment changed nothing about the data, so the entry is still there and still served.
SELECT count() FROM system.columns_cache
WHERE database = currentDatabase() AND table = 't_columns_cache_comment' AND column = 'a';

SELECT sum(a), count() FROM t_columns_cache_comment
SETTINGS log_comment = '05029_read_after_comment';

SYSTEM FLUSH LOGS query_log;

SELECT ProfileEvents['ColumnsCacheHits'] > 0
FROM system.query_log
WHERE current_database = currentDatabase()
    AND type = 'QueryFinish'
    AND log_comment = '05029_read_after_comment'
ORDER BY event_time_microseconds
LIMIT 1;

DROP TABLE t_columns_cache_comment;
