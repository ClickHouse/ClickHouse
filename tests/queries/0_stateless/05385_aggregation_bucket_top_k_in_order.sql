-- The bucket-local Top-K runs in the two-level conversion, which an in-order aggregation does not have,
-- so `EXPLAIN` must not advertise it there.

DROP TABLE IF EXISTS t_bucket_top_k_in_order;
CREATE TABLE t_bucket_top_k_in_order (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_bucket_top_k_in_order SELECT number % 1000, number FROM numbers(100000);

SET serialize_query_plan = 0, query_plan_enable_optimizations = 1, query_plan_push_down_limit = 1, query_plan_aggregation_bucket_top_k = 1, optimize_read_in_order = 1;
-- A parallel replicas plan aggregates on the replicas, so pin a local plan for the parallel replicas suite.
SET enable_parallel_replicas = 0, automatic_parallel_replicas_mode = 0;

SELECT '-- in order';
SELECT count() FROM (EXPLAIN actions = 1 SELECT k, count() AS c FROM t_bucket_top_k_in_order GROUP BY k ORDER BY c DESC LIMIT 3 SETTINGS optimize_aggregation_in_order = 1) WHERE explain ILIKE '%Bucket top-K%';

SELECT '-- not in order';
SELECT count() FROM (EXPLAIN actions = 1 SELECT k, count() AS c FROM t_bucket_top_k_in_order GROUP BY k ORDER BY c DESC LIMIT 3 SETTINGS optimize_aggregation_in_order = 0) WHERE explain ILIKE '%Bucket top-K%';

DROP TABLE t_bucket_top_k_in_order;
