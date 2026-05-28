-- Tags: no-parallel-replicas

-- `STREAM` reshapes the row set under the count (cursor / window semantics),
-- so the trivial-count-with-sparsity-filter rewrite must not answer it from
-- whole-table `num_defaults` / `num_rows` stats. EXPLAIN PLAN should show
-- `ReadFromMergeTree`, never `ReadFromPreparedSource`.

SET allow_experimental_analyzer = 1;
SET enable_streaming_queries = 1;

DROP TABLE IF EXISTS t_sparsity_stream;

CREATE TABLE t_sparsity_stream (a String, b UInt32)
ENGINE = MergeTree ORDER BY a
SETTINGS index_granularity = 8192,
         ratio_of_defaults_for_sparse_serialization = 0.5,
         min_bytes_for_wide_part = 0,
         enable_block_number_column = 1,
         enable_block_offset_column = 1,
         add_minmax_index_for_block_number_column = 1,
         add_minmax_index_for_block_offset_column = 1,
         part_minmax_index_columns = 'with_block_number_offset';

INSERT INTO t_sparsity_stream SELECT 'p', if(number < 4000, 0, 1)::UInt32 FROM numbers(5000);

SELECT 'no_stream',
       countIf(explain LIKE '%Optimized trivial count with sparsity filter%') > 0
FROM (EXPLAIN SELECT count() FROM t_sparsity_stream WHERE b != 0
      SETTINGS optimize_trivial_count_with_sparsity_filter = 1, use_skip_indexes_on_data_read = 0);

SELECT 'stream',
       countIf(explain LIKE '%Optimized trivial count with sparsity filter%')
FROM (EXPLAIN SELECT count() FROM t_sparsity_stream STREAM WHERE b != 0
      SETTINGS optimize_trivial_count_with_sparsity_filter = 1, use_skip_indexes_on_data_read = 0);

DROP TABLE t_sparsity_stream;
