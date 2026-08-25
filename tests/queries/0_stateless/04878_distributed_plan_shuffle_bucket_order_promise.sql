-- Tags: shard, no-old-analyzer

SET enable_parallel_replicas = 0;
SET automatic_parallel_replicas_mode = 0;
SET explain_query_plan_default = 'legacy';
-- Distributed aggregation cannot enforce a global `max_rows_to_group_by`, so pin it to 0.
SET max_rows_to_group_by = 0;

DROP TABLE IF EXISTS t_shuffle_bucket_order;
CREATE TABLE t_shuffle_bucket_order (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY tuple()
    SETTINGS index_granularity = 256, auto_statistics_types = '';
INSERT INTO t_shuffle_bucket_order SELECT number % 50000, number FROM numbers(200000);

SET make_distributed_plan = 1;
SET distributed_plan_execute_locally = 1;
SET distributed_plan_default_shuffle_join_bucket_count = 2;
-- No statistics, so the strategy choice does not depend on an estimated group count.
SET use_statistics = 0;
SET distributed_plan_max_rows_to_broadcast = 0;
-- A shard plan otherwise carries `BlocksMarshallingStep`, which cannot run on a worker, and a plan
-- holding it is executed with its exchanges turned into no-ops instead of being distributed.
SET enable_parallel_blocks_marshalling = 0;
-- Two-level aggregation states in every producer, so the merge consumes several buckets per input.
SET group_by_two_level_threshold = 10000;
SET group_by_two_level_threshold_bytes = 1;
SET max_threads = 16;
SET distributed_aggregation_memory_efficient = 1;

-- Arming, asserted separately from the results below: the initiator merges bucket by bucket, and it
-- does so over more than one producer. Both are required for a duplicated bucket to be observable.
-- The settings sit on the inner query because the wrapper's own `SETTINGS` clause, which keeps the
-- wrapper out of the rewrite, would otherwise apply to the plan being explained as well.
SELECT count() > 0 FROM
    (EXPLAIN PLAN actions = 1 SELECT k, sum(v) FROM remote('127.0.0.{2,3}', currentDatabase(), t_shuffle_bucket_order) GROUP BY k
        SETTINGS distributed_aggregation_memory_efficient = 1)
    WHERE explain ILIKE '%memory-efficient%'
    SETTINGS make_distributed_plan = 0;
SELECT count() > 0 FROM
    (EXPLAIN PLAN SELECT k, sum(v) FROM remote('127.0.0.{2,3}', currentDatabase(), t_shuffle_bucket_order) GROUP BY k
        SETTINGS distributed_aggregation_memory_efficient = 1)
    WHERE explain ILIKE '%ReadFromRemote%'
    SETTINGS make_distributed_plan = 0;

-- The aggregation must complete. The shuffle strategy keeps the promise to produce results in bucket
-- order while each of its instances orders only its own share, so the merge receives a bucket it has
-- already merged and rejects the whole query. Two shapes, because the merge reaches the duplicate
-- from both its ordered and its delayed-bucket push.
SELECT k, sum(v) FROM remote('127.0.0.{2,3}', currentDatabase(), t_shuffle_bucket_order) GROUP BY k FORMAT Null;
SELECT k FROM remote('127.0.0.{2,3}', currentDatabase(), t_shuffle_bucket_order) GROUP BY ALL FORMAT Null;

-- The keys and the aggregate values must match the plain plan, not merely avoid the rejection. A
-- single group keeps the output deterministic without an ORDER BY, which this plan cannot distribute.
SELECT k, sum(v) FROM remote('127.0.0.{2,3}', currentDatabase(), t_shuffle_bucket_order) GROUP BY k HAVING k = 7;
SELECT k, sum(v) FROM remote('127.0.0.{2,3}', currentDatabase(), t_shuffle_bucket_order) GROUP BY k HAVING k = 7
    SETTINGS make_distributed_plan = 0;

-- The shuffle strategy stays available, including under its force setting, when nothing downstream
-- requires bucket order.
SELECT count() > 0 FROM (EXPLAIN PLAN actions = 1 SELECT k, sum(v) FROM t_shuffle_bucket_order GROUP BY k
        SETTINGS make_distributed_plan = 1,
                 distributed_aggregation_memory_efficient = 0,
                 enable_memory_bound_merging_of_aggregation_results = 0)
    WHERE explain ILIKE '%by hash(%'
    SETTINGS make_distributed_plan = 0;
SELECT count() > 0 FROM (EXPLAIN PLAN actions = 1 SELECT k, sum(v) FROM t_shuffle_bucket_order GROUP BY k
        SETTINGS make_distributed_plan = 1,
                 distributed_aggregation_memory_efficient = 0,
                 enable_memory_bound_merging_of_aggregation_results = 0,
                 distributed_plan_force_shuffle_aggregation = 1)
    WHERE explain ILIKE '%by hash(%'
    SETTINGS make_distributed_plan = 0;

DROP TABLE t_shuffle_bucket_order;
