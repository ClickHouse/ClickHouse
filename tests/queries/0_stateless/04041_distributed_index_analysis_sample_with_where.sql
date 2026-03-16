-- Tags: no-random-settings, no-random-merge-tree-settings

-- PR #98931 added SAMPLE clause support to distributed index analysis.
-- Test 04029 covers SAMPLE-only queries (the else-if branch in distributedIndexAnalysis.cpp).
-- This test covers the AND-combination branch (lines 266-267): when both a WHERE filter
-- and a SAMPLE filter are present, they are combined via makeASTForLogicalAnd before
-- being sent to remote replicas for granule pruning.

SET send_logs_level = 'error';

DROP TABLE IF EXISTS t_dia_sample_where;

CREATE TABLE t_dia_sample_where (key UInt64, value UInt64)
ENGINE = MergeTree ORDER BY intHash32(key) SAMPLE BY intHash32(key)
SETTINGS index_granularity = 256,
  min_bytes_for_wide_part = '1G',
  index_granularity_bytes = '10M',
  distributed_index_analysis_min_parts_to_activate = 0,
  distributed_index_analysis_min_indexes_bytes_to_activate = 0;

SYSTEM STOP MERGES t_dia_sample_where;
INSERT INTO t_dia_sample_where SELECT number, number FROM numbers(100000) SETTINGS max_block_size = 10000, min_insert_block_size_rows = 10000, max_insert_threads = 1;
INSERT INTO t_dia_sample_where SELECT number + 100000, number FROM numbers(100000) SETTINGS max_block_size = 10000, min_insert_block_size_rows = 10000, max_insert_threads = 1;

SET cluster_for_parallel_replicas = 'test_cluster_one_shard_two_replicas';
SET max_parallel_replicas = 2;
SET use_query_condition_cache = 0;
SET allow_experimental_parallel_reading_from_replicas = 0;
SET allow_experimental_analyzer = 1;

-- Results must be identical with and without distributed index analysis.
-- SAMPLE + WHERE exercises the AND-combination branch in DistributedIndexAnalyzer:
--   if (filter_ast && sampling_filter)
--       filter_ast = makeASTForLogicalAnd({filter_ast, sampling_filter});
SELECT count() FROM t_dia_sample_where SAMPLE 0.1 WHERE value < 50000 SETTINGS distributed_index_analysis = 0;
SELECT count() FROM t_dia_sample_where SAMPLE 0.1 WHERE value < 50000 SETTINGS distributed_index_analysis = 1;

DROP TABLE t_dia_sample_where;
