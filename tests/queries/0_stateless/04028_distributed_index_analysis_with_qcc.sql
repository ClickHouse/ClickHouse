-- Tags: no-random-settings, no-random-merge-tree-settings

-- Regression test: distributed index analysis + query condition cache
-- caused an exception because QCC splits mark ranges before distributed
-- index analysis, violating an assertion that expected exactly one range per part.

SET send_logs_level = 'error';

DROP TABLE IF EXISTS t_dia_qcc;

CREATE TABLE t_dia_qcc (key Int64, value Int64)
ENGINE = MergeTree ORDER BY key
SETTINGS index_granularity = 256, min_bytes_for_wide_part = '1G', index_granularity_bytes = '10M',
  distributed_index_analysis_min_parts_to_activate = 0, distributed_index_analysis_min_indexes_bytes_to_activate = 0;

-- Single insert to get a single part with many granules
INSERT INTO t_dia_qcc SELECT number, number FROM numbers(1000000);

-- Step 1: populate query condition cache with a condition that creates a gap in mark ranges
SELECT count() FROM t_dia_qcc WHERE key < 200000 OR key > 800000
SETTINGS use_query_condition_cache = 1;

-- Step 2: same condition with distributed index analysis enabled
-- QCC splits the single [0, N) range into two ranges, which previously triggered a logical error
SELECT count() FROM t_dia_qcc WHERE key < 200000 OR key > 800000
SETTINGS use_query_condition_cache = 1, distributed_index_analysis = 1, distributed_index_analysis_for_non_shared_merge_tree = 1,
  cluster_for_parallel_replicas = 'parallel_replicas', max_parallel_replicas = 10,
  allow_experimental_parallel_reading_from_replicas = 0;

DROP TABLE t_dia_qcc;
