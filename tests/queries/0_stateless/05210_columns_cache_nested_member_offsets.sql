-- Tags: no-parallel, no-random-settings, no-random-merge-tree-settings, no-replicated-database
-- - no-parallel -- `SYSTEM DROP COLUMNS CACHE` is server-wide
-- - no-random-settings, no-random-merge-tree-settings -- the read has to be one task, so that the
--   hit oracle below counts one range
-- - no-replicated-database -- the cache is per server

-- A `Nested` member added by an `ALTER` after the part was written keeps no elements in the part,
-- only the offsets of the group it shares them with. Reading it therefore does produce data:
-- `fillMissingColumns` discards its values but takes those offsets to size every re-added member
-- of the group. The cache cannot hold such a column - the copy would have offsets indexing past
-- the end of its empty elements - so it has to be read from the part even when the rest of the
-- range is served from the cache. Serving the range and leaving it null instead gave empty arrays
-- for every re-added member, and only from the second read on, once the range was in the cache.
--
-- The read below asks for nothing but re-added members, so `injectRequiredColumns` adds the
-- smallest column of the part to it to carry the row count - and that column is what the cache
-- has an entry for, which is what turns the second read into a hit.

DROP TABLE IF EXISTS t_cc_nested_offsets;

CREATE TABLE t_cc_nested_offsets
(
    tiny UInt8,
    id UInt64,
    `arr.id` Array(UInt64),
    `arr.nested` Array(Tuple(a String, b Float64)),
    `arr.n` Array(Nullable(String))
)
ENGINE = MergeTree
ORDER BY id
SETTINGS
    share_nested_offsets = 1,
    min_bytes_for_wide_part = 0,
    min_rows_for_wide_part = 0,
    index_granularity = 8192,
    index_granularity_bytes = 0;

INSERT INTO t_cc_nested_offsets
SELECT 0, number, [number], [(toString(number), number)], [toString(number)] FROM numbers(20000);

-- Metadata-only: the part keeps no elements for either member, only the shared `arr` offsets.
ALTER TABLE t_cc_nested_offsets DROP COLUMN `arr.nested`;
ALTER TABLE t_cc_nested_offsets ADD COLUMN `arr.nested` Array(Tuple(a String, b Float64));
ALTER TABLE t_cc_nested_offsets DROP COLUMN `arr.n`;
ALTER TABLE t_cc_nested_offsets ADD COLUMN `arr.n` Array(Nullable(String));

SYSTEM DROP COLUMNS CACHE;

-- Every row holds one element in the group, so each length is 20000 and every element is a
-- default. `max_block_size` puts several block boundaries inside the range, because the members
-- are read while the range is being served block by block.
SELECT
    sum(length(`arr.nested`.b)),
    sum(length(`arr.nested`)),
    sum(length(`arr.n`.null)),
    sum(arraySum(`arr.nested`.b)),
    uniqExact(`arr.nested`.a[1])
FROM t_cc_nested_offsets
SETTINGS
    use_columns_cache = 1,
    enable_writes_to_columns_cache = 1,
    enable_reads_from_columns_cache = 1,
    max_block_size = 1000,
    max_threads = 1,
    log_comment = '05210_read_1';

SELECT
    sum(length(`arr.nested`.b)),
    sum(length(`arr.nested`)),
    sum(length(`arr.n`.null)),
    sum(arraySum(`arr.nested`.b)),
    uniqExact(`arr.nested`.a[1])
FROM t_cc_nested_offsets
SETTINGS
    use_columns_cache = 1,
    enable_writes_to_columns_cache = 1,
    enable_reads_from_columns_cache = 1,
    max_block_size = 1000,
    max_threads = 1,
    log_comment = '05210_read_2';

-- A member read next to a column of the group that is in the part: the offsets are then there
-- twice over, and both reads have to agree with the reads above.
SELECT sum(length(`arr.nested`.b)), sum(length(`arr.id`)), countIf(`arr.id` != [id])
FROM t_cc_nested_offsets
SETTINGS
    use_columns_cache = 1,
    enable_writes_to_columns_cache = 1,
    enable_reads_from_columns_cache = 1,
    max_block_size = 1000,
    max_threads = 1,
    log_comment = '05210_read_3';

SELECT sum(length(`arr.nested`.b)), sum(length(`arr.id`)), countIf(`arr.id` != [id])
FROM t_cc_nested_offsets
SETTINGS
    use_columns_cache = 1,
    enable_writes_to_columns_cache = 1,
    enable_reads_from_columns_cache = 1,
    max_block_size = 1000,
    max_threads = 1,
    log_comment = '05210_read_4';

SYSTEM FLUSH LOGS query_log;

-- The first read of each pair populates the cache and misses; the second one is served from it.
-- Asserting the miss of the first read too keeps the test from passing on a build where the cache
-- is never engaged at all, which is the only way the values above can be right by accident.
SELECT
    log_comment,
    ProfileEvents['ColumnsCacheHits'] > 0 AS served_from_cache,
    ProfileEvents['ColumnsCacheMisses'] > 0 AS read_from_disk
FROM system.query_log
WHERE current_database = currentDatabase()
  AND log_comment LIKE '05210_read_%'
  AND type = 'QueryFinish'
  AND event_time >= now() - INTERVAL 5 MINUTE
ORDER BY log_comment;

DROP TABLE t_cc_nested_offsets;
