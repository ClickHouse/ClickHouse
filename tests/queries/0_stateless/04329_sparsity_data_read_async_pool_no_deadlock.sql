-- The granule analyzer is reachable from `MergeTreeSource` async-read jobs that
-- run on `getIOThreadPool`. If the analyzer were to fan its per-chunk passes onto
-- the same pool, a saturated pool could deadlock with every worker blocked in the
-- analyzer's wait while the chunk tasks queued behind them.
-- This test exercises the combination `use_sparsity_info_for_pruning = 'data_read'`
-- and `allow_asynchronous_read_from_io_pool_for_merge_tree = 1` across many parts
-- so the analyzer is invoked under the conditions that would expose the bug.

SET enable_analyzer = 1;

DROP TABLE IF EXISTS t_sparse_async_pool;

CREATE TABLE t_sparse_async_pool (id UInt64, x UInt32)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 2048,
         ratio_of_defaults_for_sparse_serialization = 0.5,
         compute_exact_num_defaults_for_sparse_columns = 1,
         serialization_info_version = 'with_types',
         min_bytes_for_wide_part = 0;

SYSTEM STOP MERGES t_sparse_async_pool;

-- 16 separate parts so multiple `MergeTreeSource` workers analyze concurrently.
INSERT INTO t_sparse_async_pool SELECT number,         if(number % 100 = 0, number, 0)::UInt32 FROM numbers(50000);
INSERT INTO t_sparse_async_pool SELECT number + 50000, if(number % 100 = 0, number, 0)::UInt32 FROM numbers(50000);
INSERT INTO t_sparse_async_pool SELECT number + 100000,if(number % 100 = 0, number, 0)::UInt32 FROM numbers(50000);
INSERT INTO t_sparse_async_pool SELECT number + 150000,if(number % 100 = 0, number, 0)::UInt32 FROM numbers(50000);
INSERT INTO t_sparse_async_pool SELECT number + 200000,if(number % 100 = 0, number, 0)::UInt32 FROM numbers(50000);
INSERT INTO t_sparse_async_pool SELECT number + 250000,if(number % 100 = 0, number, 0)::UInt32 FROM numbers(50000);
INSERT INTO t_sparse_async_pool SELECT number + 300000,if(number % 100 = 0, number, 0)::UInt32 FROM numbers(50000);
INSERT INTO t_sparse_async_pool SELECT number + 350000,if(number % 100 = 0, number, 0)::UInt32 FROM numbers(50000);
INSERT INTO t_sparse_async_pool SELECT number + 400000,if(number % 100 = 0, number, 0)::UInt32 FROM numbers(50000);
INSERT INTO t_sparse_async_pool SELECT number + 450000,if(number % 100 = 0, number, 0)::UInt32 FROM numbers(50000);
INSERT INTO t_sparse_async_pool SELECT number + 500000,if(number % 100 = 0, number, 0)::UInt32 FROM numbers(50000);
INSERT INTO t_sparse_async_pool SELECT number + 550000,if(number % 100 = 0, number, 0)::UInt32 FROM numbers(50000);
INSERT INTO t_sparse_async_pool SELECT number + 600000,if(number % 100 = 0, number, 0)::UInt32 FROM numbers(50000);
INSERT INTO t_sparse_async_pool SELECT number + 650000,if(number % 100 = 0, number, 0)::UInt32 FROM numbers(50000);
INSERT INTO t_sparse_async_pool SELECT number + 700000,if(number % 100 = 0, number, 0)::UInt32 FROM numbers(50000);
INSERT INTO t_sparse_async_pool SELECT number + 750000,if(number % 100 = 0, number, 0)::UInt32 FROM numbers(50000);

-- Query must complete (no deadlock) and produce the right count.
SELECT count() FROM t_sparse_async_pool WHERE x = 0
    SETTINGS use_sparsity_info_for_pruning = 'data_read',
             allow_asynchronous_read_from_io_pool_for_merge_tree = 1,
             use_query_condition_cache = 0;

DROP TABLE t_sparse_async_pool;
