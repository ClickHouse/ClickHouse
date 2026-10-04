-- Tags: no-parallel, no-random-settings, no-random-merge-tree-settings, no-replicated-database
-- - no-parallel -- `SYSTEM DROP COLUMNS CACHE` is server-wide
-- - no-random-settings, no-random-merge-tree-settings -- the read has to be one task, so that
--   the hit oracle below counts one range
-- - no-replicated-database -- the cache is per server

-- A column that the read does not produce from the part is synthesized afterwards by
-- `fillMissingColumns`: one added by an `ALTER` after the part was written has no stream in the
-- part at all, and a `Nested` member added the same way has only the offsets of its sibling.
-- Neither ever gets a cache entry, so the lookup must not require one for them. It used to, and
-- the effect was that a table holding such a column could never be served from the cache: every
-- repeated read missed and re-read the whole range from disk, however well the columns that are
-- in the part had been cached.
--
-- The values matter as much as the hits here: a range served from the cache leaves these columns
-- for `fillMissingColumns` exactly as a range read from disk does, so all three reads below have
-- to agree, and agree with the default expression and with the sibling's offsets.

DROP TABLE IF EXISTS t_cc_added;

CREATE TABLE t_cc_added (id UInt64, s String, n Nested(a UInt64))
ENGINE = MergeTree
ORDER BY id
SETTINGS
    min_bytes_for_wide_part = 0,
    min_rows_for_wide_part = 0,
    index_granularity = 8192,
    index_granularity_bytes = 0;

INSERT INTO t_cc_added SELECT number, repeat('x', 50), range(number % 5) FROM numbers(30000);

OPTIMIZE TABLE t_cc_added FINAL;

-- Metadata-only: the part keeps no stream for `added`, and only `n.a`'s offsets for `n.b`.
ALTER TABLE t_cc_added ADD COLUMN added UInt64 DEFAULT id * 2;
ALTER TABLE t_cc_added ADD COLUMN n.b Array(UInt64);

SYSTEM DROP COLUMNS CACHE;

SELECT sum(id), sum(added), sum(arraySum(n.a)), sum(arraySum(n.b)), sum(length(n.b))
FROM t_cc_added
SETTINGS
    use_columns_cache = 1,
    enable_writes_to_columns_cache = 1,
    enable_reads_from_columns_cache = 1,
    max_threads = 1,
    log_comment = '05155_read_1';

SELECT sum(id), sum(added), sum(arraySum(n.a)), sum(arraySum(n.b)), sum(length(n.b))
FROM t_cc_added
SETTINGS
    use_columns_cache = 1,
    enable_writes_to_columns_cache = 1,
    enable_reads_from_columns_cache = 1,
    max_threads = 1,
    log_comment = '05155_read_2';

SELECT sum(id), sum(added), sum(arraySum(n.a)), sum(arraySum(n.b)), sum(length(n.b))
FROM t_cc_added
SETTINGS
    use_columns_cache = 1,
    enable_writes_to_columns_cache = 1,
    enable_reads_from_columns_cache = 1,
    max_threads = 1,
    log_comment = '05155_read_3';

SYSTEM FLUSH LOGS query_log;

-- The first read populates the cache and misses; the two after it are served from it. Asserting
-- the miss of the first read too keeps the test from passing on a build where the cache is never
-- engaged at all.
SELECT
    log_comment,
    ProfileEvents['ColumnsCacheHits'] > 0 AS served_from_cache,
    ProfileEvents['ColumnsCacheMisses'] > 0 AS read_from_disk
FROM system.query_log
WHERE current_database = currentDatabase()
  AND log_comment LIKE '05155_read_%'
  AND type = 'QueryFinish'
  AND event_time >= now() - INTERVAL 5 MINUTE
ORDER BY log_comment;

DROP TABLE t_cc_added;
