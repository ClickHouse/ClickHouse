-- Tags: shard, no-old-analyzer

-- A shard of a plain distributed query plans its aggregation to the `WithMergeableState` stage, and
-- that aggregation promises the initiator's memory-efficient merge to deliver its two-level buckets
-- in order. The initiator reads each shard as one stream. With `enable_cascades_optimizer = 1` (the
-- stress test runs with it) the shard used to split the aggregation over several nodes and gather
-- the instances into that stream, and each instance orders only its own share, so the initiator
-- received a bucket twice and rejected the query with a logical error.

SET enable_parallel_replicas = 0;
SET automatic_parallel_replicas_mode = 0;
SET explain_query_plan_default = 'legacy';
-- Distributed aggregation cannot enforce a global `max_rows_to_group_by`, so pin it to 0.
SET max_rows_to_group_by = 0;

DROP TABLE IF EXISTS t_cascades_bucket_order;
CREATE TABLE t_cascades_bucket_order (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY tuple()
    SETTINGS index_granularity = 256, auto_statistics_types = 'basic, uniq_v2';
SET materialize_statistics_on_insert = 1;
INSERT INTO t_cascades_bucket_order SELECT number % 5000, number FROM numbers(200000);

SET make_distributed_plan = 1;
SET enable_cascades_optimizer = 1;
SET distributed_plan_execute_locally = 1;
-- Two planning nodes, so the shard has a multi-node partial aggregation to choose from.
SET distributed_plan_workers_num = 2;
-- A measured, exact-coverage NDV pins enough reduction for the distributed two-stage alternative
-- to win. An absent NDV deliberately uses the Cascades 10% fallback, which makes the valid
-- single-node alternative cheaper for this fixture and would stop exercising the bucket-order fix.
SET use_statistics = 1;
-- A shard plan otherwise carries `BlocksMarshallingStep`, which cannot run on a worker, and a plan
-- holding it is executed with its exchanges turned into no-ops instead of being distributed.
SET enable_parallel_blocks_marshalling = 0;
-- Two-level aggregation states in every producer, so the merge consumes several buckets per input.
SET group_by_two_level_threshold = 10000;
SET group_by_two_level_threshold_bytes = 1;
-- Enough to flush a producer's two-level states in parallel. The producers the merge interleaves
-- come from `distributed_plan_workers_num`, so a larger value here only multiplies the flaky
-- check, which runs `nproc - 1` copies of this test with the thread fuzzer on.
SET max_threads = 4;
-- The promise is made from either setting; both are pinned because the runner randomizes them.
SET distributed_aggregation_memory_efficient = 1;
SET enable_memory_bound_merging_of_aggregation_results = 1;
SET serialize_query_plan = 0;

-- The shard plans, as `distributed = 1` prints them below the initiator's own merge. A partial
-- aggregation gathered from several nodes is under a merge that restores the bucket order: every
-- `GatherExchange` in a shard plan has a `MergingAggregated` above it, and the initiator's merge is
-- the one left over. The second row pins that the distributed alternative is still taken rather than
-- the aggregation left on a single node.
SELECT countIf(explain ILIKE '%MergingAggregated%') - 1 = countIf(explain ILIKE '%GatherExchange%'),
       countIf(explain ILIKE '%GatherExchange%') > 0
    FROM (EXPLAIN PLAN distributed = 1 SELECT k, sum(v) FROM remote('127.0.0.{2,3}', currentDatabase(), t_cascades_bucket_order) GROUP BY k
        SETTINGS make_distributed_plan = 1, enable_cascades_optimizer = 1, distributed_plan_workers_num = 2,
                 distributed_aggregation_memory_efficient = 1, enable_memory_bound_merging_of_aggregation_results = 1)
    SETTINGS make_distributed_plan = 0;
SELECT countIf(explain ILIKE '%MergingAggregated%') - 1 = countIf(explain ILIKE '%GatherExchange%'),
       countIf(explain ILIKE '%GatherExchange%') > 0
    FROM (EXPLAIN PLAN distributed = 1 SELECT k, sum(v) FROM remote('127.0.0.{2,3}', currentDatabase(), t_cascades_bucket_order) GROUP BY k
        SETTINGS make_distributed_plan = 1, enable_cascades_optimizer = 1, distributed_plan_workers_num = 2,
                 distributed_aggregation_memory_efficient = 1, enable_memory_bound_merging_of_aggregation_results = 1,
                 distributed_plan_force_shuffle_aggregation = 1)
    SETTINGS make_distributed_plan = 0;
-- The promise is made from either setting alone, so the split must follow whichever is on.
SELECT countIf(explain ILIKE '%MergingAggregated%') - 1 = countIf(explain ILIKE '%GatherExchange%'),
       countIf(explain ILIKE '%GatherExchange%') > 0
    FROM (EXPLAIN PLAN distributed = 1 SELECT k, sum(v) FROM remote('127.0.0.{2,3}', currentDatabase(), t_cascades_bucket_order) GROUP BY k
        SETTINGS make_distributed_plan = 1, enable_cascades_optimizer = 1, distributed_plan_workers_num = 2,
                 distributed_aggregation_memory_efficient = 0, enable_memory_bound_merging_of_aggregation_results = 1)
    SETTINGS make_distributed_plan = 0;
SELECT countIf(explain ILIKE '%MergingAggregated%') - 1 = countIf(explain ILIKE '%GatherExchange%'),
       countIf(explain ILIKE '%GatherExchange%') > 0
    FROM (EXPLAIN PLAN distributed = 1 SELECT k, sum(v) FROM remote('127.0.0.{2,3}', currentDatabase(), t_cascades_bucket_order) GROUP BY k
        SETTINGS make_distributed_plan = 1, enable_cascades_optimizer = 1, distributed_plan_workers_num = 2,
                 distributed_aggregation_memory_efficient = 1, enable_memory_bound_merging_of_aggregation_results = 0)
    SETTINGS make_distributed_plan = 0;
-- Without the promise the shard gathers its partial aggregation as before: the initiator's merge is
-- the only one, and both shards keep the multi-node partial.
SELECT countIf(explain ILIKE '%MergingAggregated%') = 1, countIf(explain ILIKE '%GatherExchange%') = 2
    FROM (EXPLAIN PLAN distributed = 1 SELECT k, sum(v) FROM remote('127.0.0.{2,3}', currentDatabase(), t_cascades_bucket_order) GROUP BY k
        SETTINGS make_distributed_plan = 1, enable_cascades_optimizer = 1, distributed_plan_workers_num = 2,
                 distributed_aggregation_memory_efficient = 0, enable_memory_bound_merging_of_aggregation_results = 0)
    SETTINGS make_distributed_plan = 0;

-- The aggregation must complete: on both push paths of the merge, under the force setting, and
-- with the shard plan shipped rather than sent as text.
SELECT k, sum(v) FROM remote('127.0.0.{2,3}', currentDatabase(), t_cascades_bucket_order) GROUP BY k FORMAT Null;
SELECT k FROM remote('127.0.0.{2,3}', currentDatabase(), t_cascades_bucket_order) GROUP BY ALL FORMAT Null;
SELECT k, sum(v) FROM remote('127.0.0.{2,3}', currentDatabase(), t_cascades_bucket_order) GROUP BY k FORMAT Null
    SETTINGS distributed_plan_force_shuffle_aggregation = 1;
SELECT k, sum(v) FROM remote('127.0.0.{2,3}', currentDatabase(), t_cascades_bucket_order) GROUP BY k FORMAT Null
    SETTINGS serialize_query_plan = 1;
-- With the promise made from the memory-bound merging alone, the initiator still merges in bucket order.
SELECT k, sum(v) FROM remote('127.0.0.{2,3}', currentDatabase(), t_cascades_bucket_order) GROUP BY k FORMAT Null
    SETTINGS distributed_aggregation_memory_efficient = 0, enable_memory_bound_merging_of_aggregation_results = 1;

-- The result must match the plain plan, not merely avoid the rejection: every group once, and the
-- sums complete. Both shards read the same table, hence twice the sum of the inserted values.
SELECT count(), sum(s) = 2 * (SELECT sum(number) FROM numbers(200000))
    FROM (SELECT k, sum(v) AS s FROM remote('127.0.0.{2,3}', currentDatabase(), t_cascades_bucket_order) GROUP BY k);
SELECT k, sum(v) FROM remote('127.0.0.{2,3}', currentDatabase(), t_cascades_bucket_order) GROUP BY k HAVING k = 7;
SELECT k, sum(v) FROM remote('127.0.0.{2,3}', currentDatabase(), t_cascades_bucket_order) GROUP BY k HAVING k = 7
    SETTINGS make_distributed_plan = 0, enable_cascades_optimizer = 0;

DROP TABLE t_cascades_bucket_order;
