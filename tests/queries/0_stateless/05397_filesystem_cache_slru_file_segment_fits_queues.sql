-- Tags: no-fasttest
-- no-fasttest: requires a cache disk.

-- The cache of a disk defined inline is released with its last table, so its settings are checked before `DROP TABLE`.

-- With an SLRU policy, a file segment must fit whole into the protected and the probationary queue.
-- A 1 MiB cache with the default `slru_size_ratio` 0.6 has a probationary queue of 419430 bytes.
CREATE TABLE t_05397 (key UInt64) ENGINE = MergeTree ORDER BY key
SETTINGS disk = disk(type = cache, name = 'cache_05397_slru_large', path = 'cache_05397_slru_large', disk = 'local_disk',
    max_size = '1Mi', max_file_segment_size = '512Ki', boundary_alignment = '512Ki'); -- { serverError BAD_ARGUMENTS }

CREATE TABLE t_05397 (key UInt64) ENGINE = MergeTree ORDER BY key
SETTINGS disk = disk(type = cache, name = 'cache_05397_slru_fits', path = 'cache_05397_slru_fits', disk = 'local_disk',
    max_size = '1Mi', max_file_segment_size = '256Ki', boundary_alignment = '256Ki');
SELECT cache_name, max_file_segment_size FROM system.filesystem_cache_settings WHERE cache_name = 'cache_05397_slru_fits';
DROP TABLE t_05397;

-- With `slru_size_ratio` 0.9 the probationary queue is 104858 bytes.
CREATE TABLE t_05397 (key UInt64) ENGINE = MergeTree ORDER BY key
SETTINGS disk = disk(type = cache, name = 'cache_05397_slru_ratio', path = 'cache_05397_slru_ratio', disk = 'local_disk',
    max_size = '1Mi', slru_size_ratio = 0.9, max_file_segment_size = '256Ki', boundary_alignment = '256Ki'); -- { serverError BAD_ARGUMENTS }

-- LRU has one queue: the rule does not apply.
CREATE TABLE t_05397 (key UInt64) ENGINE = MergeTree ORDER BY key
SETTINGS disk = disk(type = cache, name = 'cache_05397_lru', path = 'cache_05397_lru', disk = 'local_disk',
    max_size = '1Mi', cache_policy = 'LRU', max_file_segment_size = '512Ki', boundary_alignment = '512Ki');
SELECT cache_name, max_file_segment_size FROM system.filesystem_cache_settings WHERE cache_name = 'cache_05397_lru';
DROP TABLE t_05397;

-- With a split cache both parts are checked. The system part is 10% of 1 MiB here,
-- so its probationary queue is 41943 bytes, while the data part would fit 64 KiB segments.
CREATE TABLE t_05397 (key UInt64) ENGINE = MergeTree ORDER BY key
SETTINGS disk = disk(type = cache, name = 'cache_05397_split_large', path = 'cache_05397_split_large', disk = 'local_disk',
    max_size = '1Mi', use_split_cache = 1, max_file_segment_size = '64Ki', boundary_alignment = '64Ki'); -- { serverError BAD_ARGUMENTS }

CREATE TABLE t_05397 (key UInt64) ENGINE = MergeTree ORDER BY key
SETTINGS disk = disk(type = cache, name = 'cache_05397_split_fits', path = 'cache_05397_split_fits', disk = 'local_disk',
    max_size = '1Mi', use_split_cache = 1, max_file_segment_size = '32Ki', boundary_alignment = '32Ki');
SELECT cache_name, max_file_segment_size FROM system.filesystem_cache_settings WHERE cache_name = 'cache_05397_split_fits';
DROP TABLE t_05397;
