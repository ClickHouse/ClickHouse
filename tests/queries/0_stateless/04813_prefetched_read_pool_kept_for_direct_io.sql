-- Tags: no-parallel-replicas
-- With 'pread_threadpool' and forced O_DIRECT, the prefetched read pool must stay enabled even on
-- hosts where `preadNoWait` is unavailable (e.g. Darwin): reads with O_DIRECT never look at the
-- page cache and are always performed in the thread pool, so 'pread_threadpool' is not downgraded
-- to 'pread' for them (see `resolveLocalFSReadMethod`).

DROP TABLE IF EXISTS t_prefetch_pool_direct_io;

CREATE TABLE t_prefetch_pool_direct_io (k UInt64, s String) ENGINE = MergeTree ORDER BY k
    SETTINGS index_granularity = 128, min_bytes_for_wide_part = 0;

INSERT INTO t_prefetch_pool_direct_io SELECT number, repeat('x', 200) FROM numbers(100000);

-- The PartsSplitter fault injection is pinned off - it takes precedence in ReadFromMergeTree and
-- reads ReadType::InOrder, which uses no prefetch pool at all. The remote variants of the settings
-- are pinned too, so the test also works on object-storage runs where the parts are not local.
SET merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0;
SET local_filesystem_read_method = 'pread_threadpool';
SET remote_filesystem_read_method = 'threadpool';
SET allow_prefetched_read_pool_for_local_filesystem = 1;
SET allow_prefetched_read_pool_for_remote_filesystem = 1;
SET min_bytes_to_use_direct_io = 1;
SET filesystem_prefetch_step_marks = 1;
SET filesystem_prefetches_limit = 100;
SET merge_tree_min_rows_for_concurrent_read = 1;
SET merge_tree_min_bytes_for_concurrent_read = 1;
SET max_threads = 4;

SELECT count() > 0 FROM (EXPLAIN PIPELINE SELECT sum(k) FROM t_prefetch_pool_direct_io)
WHERE explain LIKE '%PrefetchedReadPool%';

-- And the read itself works with forced O_DIRECT.
SELECT sum(k) FROM t_prefetch_pool_direct_io;

DROP TABLE t_prefetch_pool_direct_io;
