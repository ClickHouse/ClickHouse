-- Tags: no-old-analyzer

SET explain_query_plan_default = 'legacy';
-- Distributed aggregation rejects a nonzero global GROUP BY limit.
SET max_rows_to_group_by = 0;
SET enable_parallel_replicas = 0;

DROP TABLE IF EXISTS t_bucket_order;
-- Small granularity so a worker's 25k-row slice still has enough ranges for a multi-stream read.
CREATE TABLE t_bucket_order (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 256;
INSERT INTO t_bucket_order SELECT number % 45000, number FROM numbers(50000);

-- The hint understates the distinct key count on purpose: it must stay below
-- `distributed_plan_max_rows_to_broadcast` so the aggregation is planned as partial aggregation
-- plus a memory-efficient merge, while the real key count is large enough to fill many two-level
-- buckets in every worker. The row count stays above that limit, so the read is split into
-- parallel reader buckets and the partial aggregation runs fused with them: the workers send
-- two-level buckets into one merge.
SET param__internal_join_table_stat_hints = '{"t_bucket_order": {"cardinality": 50000, "distinct_keys": {"k": 15000}}}';
SET use_statistics = 0;

SET make_distributed_plan = 1;
SET distributed_plan_optimize_exchanges = 1;
SET distributed_plan_max_rows_to_broadcast = 20000;
SET distributed_plan_default_reader_bucket_count = 2;
SET distributed_plan_default_shuffle_join_bucket_count = 2;
-- Each worker must aggregate many streams in parallel; without this a 25k-row read is a single
-- stream, the partial aggregation output is a single ordered stream, and the merge input is
-- ordered by accident.
SET merge_tree_min_rows_for_concurrent_read = 1;
SET merge_tree_min_bytes_for_concurrent_read = 1;
-- Force two-level aggregation states in every worker and pin the thread count the timing depends on.
SET group_by_two_level_threshold = 10000;
SET group_by_two_level_threshold_bytes = 1;
SET max_threads = 16;
SET distributed_aggregation_memory_efficient = 1;

EXPLAIN SELECT k, uniqExact(v) FROM t_bucket_order GROUP BY k;

-- Each GROUP BY key must come out exactly once. A merge that mistakes unordered buckets for ordered
-- ones emits some keys twice with split aggregate states. The reorder does not happen on every run,
-- so repeat the check.
SELECT count() - uniqExact(k) FROM (SELECT k, uniqExact(v) AS u FROM t_bucket_order GROUP BY k);
SELECT count() - uniqExact(k) FROM (SELECT k, uniqExact(v) AS u FROM t_bucket_order GROUP BY k);
SELECT count() - uniqExact(k) FROM (SELECT k, uniqExact(v) AS u FROM t_bucket_order GROUP BY k);
SELECT count() - uniqExact(k) FROM (SELECT k, uniqExact(v) AS u FROM t_bucket_order GROUP BY k);
SELECT count() - uniqExact(k) FROM (SELECT k, uniqExact(v) AS u FROM t_bucket_order GROUP BY k);
SELECT count() - uniqExact(k) FROM (SELECT k, uniqExact(v) AS u FROM t_bucket_order GROUP BY k);
SELECT count() - uniqExact(k) FROM (SELECT k, uniqExact(v) AS u FROM t_bucket_order GROUP BY k);
SELECT count() - uniqExact(k) FROM (SELECT k, uniqExact(v) AS u FROM t_bucket_order GROUP BY k);

DROP TABLE t_bucket_order;
