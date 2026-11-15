-- Tags: no-old-analyzer

-- Under the Cascades optimizer a shard aggregation that promises bucket order to the initiator runs
-- on a single node when `max_rows_to_group_by` rules out the partial + merge split.

DROP TABLE IF EXISTS t_cascades_shard_group_by_limit;
SET enable_parallel_replicas = 0;
SET automatic_parallel_replicas_mode = 0;
SET explain_query_plan_default = 'legacy';
-- Both shards go over TCP, so each plans its own `WithMergeableState` query.
SET prefer_localhost_replica = 0;

DROP TABLE IF EXISTS t_cascades_shard_group_by_limit;
CREATE TABLE t_cascades_shard_group_by_limit (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY tuple()
    SETTINGS index_granularity = 256, auto_statistics_types = '';
INSERT INTO t_cascades_shard_group_by_limit SELECT number % 50000, number FROM numbers(200000);

SET make_distributed_plan = 1;
SET enable_cascades_optimizer = 1;
SET distributed_plan_execute_locally = 1;
SET distributed_plan_workers_num = 2;
SET use_statistics = 0;
SET enable_parallel_blocks_marshalling = 0;
SET group_by_two_level_threshold = 10000;
SET group_by_two_level_threshold_bytes = 1;
SET max_threads = 4;
SET distributed_aggregation_memory_efficient = 1;
SET enable_memory_bound_merging_of_aggregation_results = 1;
SET serialize_query_plan = 0;
SET max_rows_to_group_by = 10000000000;

-- Only the initiator's merge, and no gather in either shard plan.
SELECT countIf(explain ILIKE '%MergingAggregated%'), countIf(explain ILIKE '%GatherExchange%')
    FROM (EXPLAIN PLAN distributed = 1 SELECT k, sum(v) FROM remote('127.0.0.1,localhost', currentDatabase(), t_cascades_shard_group_by_limit) GROUP BY k
        SETTINGS make_distributed_plan = 1, enable_cascades_optimizer = 1, distributed_plan_workers_num = 2,
                 distributed_aggregation_memory_efficient = 1, enable_memory_bound_merging_of_aggregation_results = 1,
                 max_rows_to_group_by = 10000000000)
    SETTINGS make_distributed_plan = 0;
-- Without the promise each shard still gathers a multi-node partial aggregation.
SELECT countIf(explain ILIKE '%MergingAggregated%'), countIf(explain ILIKE '%GatherExchange%')
    FROM (EXPLAIN PLAN distributed = 1 SELECT k, sum(v) FROM remote('127.0.0.1,localhost', currentDatabase(), t_cascades_shard_group_by_limit) GROUP BY k
        SETTINGS make_distributed_plan = 1, enable_cascades_optimizer = 1, distributed_plan_workers_num = 2,
                 distributed_aggregation_memory_efficient = 0, enable_memory_bound_merging_of_aggregation_results = 0,
                 max_rows_to_group_by = 10000000000)
    SETTINGS make_distributed_plan = 0;

SELECT count(), sum(s) = 2 * (SELECT sum(number) FROM numbers(200000))
    FROM (SELECT k, sum(v) AS s FROM remote('127.0.0.1,localhost', currentDatabase(), t_cascades_shard_group_by_limit) GROUP BY k);
SELECT k FROM remote('127.0.0.1,localhost', currentDatabase(), t_cascades_shard_group_by_limit) GROUP BY ALL FORMAT Null;

DROP TABLE t_cascades_shard_group_by_limit;
