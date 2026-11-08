-- `GROUP BY ... ORDER BY count() DESC LIMIT n` over a distributed query with memory-bound merging:
-- the in-order merging path does not run the bucket-local Top-K conversion, so the plan drops it
-- and the result must be the same as without the optimization.

DROP TABLE IF EXISTS bucket_top_k_memory_bound;
CREATE TABLE bucket_top_k_memory_bound (k UInt64) ENGINE = MergeTree ORDER BY k;
-- Key `k` occurs `k + 1` times, so all counts are distinct.
INSERT INTO bucket_top_k_memory_bound SELECT number FROM numbers(1000) ARRAY JOIN range(number + 1) AS r;

SET enable_memory_bound_merging_of_aggregation_results = 1;
SET optimize_aggregation_in_order = 1;
SET optimize_read_in_order = 1;
SET prefer_localhost_replica = 1;
SET query_plan_enable_optimizations = 1;
SET query_plan_push_down_limit = 1;
SET query_plan_aggregation_bucket_top_k = 1;
SET enable_parallel_replicas = 0;
-- Otherwise the memory-efficient merging drops `bucket_top_k` before the memory-bound merging is applied.
SET distributed_aggregation_memory_efficient = 0;
SET group_by_two_level_threshold = 1;
SET group_by_two_level_threshold_bytes = 1;

SELECT k, count() AS c FROM remote('127.0.0.{1,2}', currentDatabase(), bucket_top_k_memory_bound)
GROUP BY k ORDER BY c DESC LIMIT 3;

SELECT k, count() AS c FROM remote('127.0.0.{1,2}', currentDatabase(), bucket_top_k_memory_bound)
GROUP BY k ORDER BY c ASC LIMIT 3;

SELECT k, count() AS c FROM remote('127.0.0.{1,2}', currentDatabase(), bucket_top_k_memory_bound)
GROUP BY k ORDER BY c DESC LIMIT 3 SETTINGS query_plan_aggregation_bucket_top_k = 0;

DROP TABLE bucket_top_k_memory_bound;
