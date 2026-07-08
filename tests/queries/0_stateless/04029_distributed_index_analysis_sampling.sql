-- Tags: no-random-settings, no-random-merge-tree-settings

-- Verify that SAMPLE clause is passed to remote replicas during distributed index analysis,
-- so they can prune granules by the sampling range.

SET send_logs_level = 'error';

DROP TABLE IF EXISTS t_dia_sampling;

CREATE TABLE t_dia_sampling (key UInt64, value UInt64)
ENGINE = MergeTree ORDER BY intHash32(key) SAMPLE BY intHash32(key)
SETTINGS index_granularity = 256,
  min_bytes_for_wide_part = '1G',
  index_granularity_bytes = '10M',
  distributed_index_analysis_min_parts_to_activate = 0,
  distributed_index_analysis_min_indexes_bytes_to_activate = 0;

SYSTEM STOP MERGES t_dia_sampling;
INSERT INTO t_dia_sampling SELECT number, number FROM numbers(100000) SETTINGS max_block_size = 10000, min_insert_block_size_rows = 10000, max_insert_threads = 1;
INSERT INTO t_dia_sampling SELECT number + 100000, number FROM numbers(100000) SETTINGS max_block_size = 10000, min_insert_block_size_rows = 10000, max_insert_threads = 1;

SET cluster_for_parallel_replicas = 'test_cluster_one_shard_two_replicas';
SET max_parallel_replicas = 2;
SET use_query_condition_cache = 0;
SET allow_experimental_parallel_reading_from_replicas = 0;
SET allow_experimental_analyzer = 1;

-- Results must be identical with and without distributed index analysis
SELECT count() FROM t_dia_sampling SAMPLE 0.1 SETTINGS distributed_index_analysis = 0;
SELECT count() FROM t_dia_sampling SAMPLE 0.1 SETTINGS distributed_index_analysis = 1;

-- { echo }
EXPLAIN indexes = 1 SELECT * FROM t_dia_sampling SAMPLE 0.1 SETTINGS distributed_index_analysis = 0;
EXPLAIN indexes = 1 SELECT * FROM t_dia_sampling SAMPLE 0.1 SETTINGS distributed_index_analysis = 1;

-- { echoOff }
DROP TABLE t_dia_sampling;
