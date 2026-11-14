-- Columns that are read only by `PREWHERE` are removed from the header of the reading step,
-- but they still occupy memory while a block is being read, so the block size predictor has to keep
-- counting them. Otherwise `preferred_block_size_bytes` is ignored after the first block and blocks
-- grow up to `max_block_size` rows of wide strings.
-- https://github.com/ClickHouse/ClickHouse/issues/107596

DROP TABLE IF EXISTS t_prewhere_only_columns;

CREATE TABLE t_prewhere_only_columns (k UInt64, x UInt64, s String)
ENGINE = MergeTree ORDER BY k
SETTINGS index_granularity = 256, index_granularity_bytes = 0, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_prewhere_only_columns SELECT number, number, repeat('a', 2000) FROM numbers(20000);

-- `s` is about 2000 bytes per row, so the quota allows about 100 rows per block (rounded up to a granule),
-- far below `max_block_size`. A narrow column is needed after `PREWHERE`, the wide one is not.
SELECT max(blockSize()) < 2048, sum(k)
FROM t_prewhere_only_columns
PREWHERE s LIKE '%a%'
SETTINGS max_threads = 1, max_block_size = 8192, preferred_block_size_bytes = 200000, query_plan_remove_unused_columns = 1;

-- The condition is moved to `PREWHERE` automatically.
SELECT max(blockSize()) < 2048, sum(k)
FROM t_prewhere_only_columns
WHERE s LIKE '%a%'
SETTINGS max_threads = 1, max_block_size = 8192, preferred_block_size_bytes = 200000, optimize_move_to_prewhere = 1, query_plan_remove_unused_columns = 1;

-- Two `PREWHERE` steps, the wide column is read by the second one.
SELECT max(blockSize()) < 2048, sum(x)
FROM t_prewhere_only_columns
PREWHERE k >= 0 AND s LIKE '%a%'
SETTINGS max_threads = 1, max_block_size = 8192, preferred_block_size_bytes = 200000, enable_multiple_prewhere_read_steps = 1, query_plan_remove_unused_columns = 1;

-- Nothing that is read by `PREWHERE` is needed after it.
SELECT max(blockSize()) < 2048, count()
FROM t_prewhere_only_columns
PREWHERE s LIKE '%a%'
SETTINGS max_threads = 1, max_block_size = 8192, preferred_block_size_bytes = 200000, query_plan_remove_unused_columns = 1;

DROP TABLE t_prewhere_only_columns;
