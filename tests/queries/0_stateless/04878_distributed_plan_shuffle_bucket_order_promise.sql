-- Tags: shard

SET enable_parallel_replicas = 0;
SET automatic_parallel_replicas_mode = 0;
SET explain_query_plan_default = 'legacy';
-- Distributed aggregation cannot enforce a global `max_rows_to_group_by`, so pin it to 0.
SET max_rows_to_group_by = 0;

DROP TABLE IF EXISTS t_shuffle_bucket_order;
-- The two `actions = 1` plans below print `ReadFromMergeTree`'s granule count. The runner randomizes
-- both of its inputs, so pin them here: adaptive sizing off, part format fixed at Wide.
CREATE TABLE t_shuffle_bucket_order (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY tuple()
    SETTINGS index_granularity = 256, index_granularity_bytes = 0, min_bytes_for_wide_part = 0, auto_statistics_types = '';
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
-- Pinned because the runner randomizes it and the promise below is set from either setting, so a plan
-- that leaves this one open does not say which of the two it exercised.
SET enable_memory_bound_merging_of_aggregation_results = 1;
-- `EXPLAIN PLAN distributed = 1` prints a shipped shard plan as it was shipped, and a shard plan is
-- shipped before the distributed rewrite runs on it, so the plans below read the rewrite only when the
-- shard receives the query as text.
SET serialize_query_plan = 0;

-- The arming and the guard are read off one plan, being claims about the same rewritten plan the guard
-- acts on: `distributed = 1` appends the per-shard plans, and the settings sit on the explained query so
-- the remaining ones keep coming from the `SET`s above. Three things must hold at once here. The
-- initiator merges bucket by bucket, which is the `MergingAggregated` carrying `Mode: memory-efficient`
-- and is printed only under `actions = 1`; it does so over more than one producer, which is the
-- `ReadFromMergeTree` in each of the two shard plans; and the shuffle scatter is absent, so the strategy
-- is partial aggregation plus merge. The first two are what makes a duplicated bucket observable at all,
-- the third is the guard. `compact = 1` walks past the `Expression` steps here and in the shard plans,
-- which drops their action dumps and keeps the mode line.
SELECT '-- armed and demoted: memory-efficient merge over two producers, no shuffle scatter';
EXPLAIN PLAN actions = 1, compact = 1, distributed = 1 SELECT k, sum(v) FROM remote('127.0.0.{2,3}', currentDatabase(), t_shuffle_bucket_order) GROUP BY k
    SETTINGS make_distributed_plan = 1, distributed_aggregation_memory_efficient = 1;

-- `distributed_plan_force_shuffle_aggregation` loses to the guard, as it loses to GROUPING SETS, and the
-- strategy the guard demotes to is the partial aggregation plus its merge, each named at the site that
-- builds it, so a plan left undistributed rather than demoted is a different plan here.
SELECT '-- the force setting loses to the guard';
EXPLAIN PLAN distributed = 1 SELECT k, sum(v) FROM remote('127.0.0.{2,3}', currentDatabase(), t_shuffle_bucket_order) GROUP BY k
    SETTINGS make_distributed_plan = 1, distributed_aggregation_memory_efficient = 1,
             distributed_plan_force_shuffle_aggregation = 1;

-- The aggregation must complete. The shuffle strategy keeps the promise to produce results in bucket
-- order while each of its instances orders only its own share, so the merge receives a bucket it has
-- already merged and rejects the whole query. Two shapes, because the merge reaches the duplicate
-- from both its ordered and its delayed-bucket push. The force setting pins the strategy, so neither
-- arm depends on the statistics-free default choice.
SELECT k, sum(v) FROM remote('127.0.0.{2,3}', currentDatabase(), t_shuffle_bucket_order) GROUP BY k FORMAT Null
    SETTINGS distributed_plan_force_shuffle_aggregation = 1;
SELECT k FROM remote('127.0.0.{2,3}', currentDatabase(), t_shuffle_bucket_order) GROUP BY ALL FORMAT Null
    SETTINGS distributed_plan_force_shuffle_aggregation = 1;
-- A shipped shard plan is rewritten by the shard that receives it rather than by the initiator. No
-- explain reaches a rewrite made there, so this row pins the outcome rather than the strategy: it does
-- not separate the demotion from an aggregation left unrewritten above the gather, which the plans above
-- separate whenever the plan is not shipped.
SELECT k, sum(v) FROM remote('127.0.0.{2,3}', currentDatabase(), t_shuffle_bucket_order) GROUP BY k FORMAT Null
    SETTINGS distributed_plan_force_shuffle_aggregation = 1, serialize_query_plan = 1;

-- The keys and the aggregate values must match the plain plan, not merely avoid the rejection. A
-- single group keeps the output deterministic without an ORDER BY, which this plan cannot distribute.
SELECT k, sum(v) FROM remote('127.0.0.{2,3}', currentDatabase(), t_shuffle_bucket_order) GROUP BY k HAVING k = 7;
SELECT k, sum(v) FROM remote('127.0.0.{2,3}', currentDatabase(), t_shuffle_bucket_order) GROUP BY k HAVING k = 7
    SETTINGS make_distributed_plan = 0;

-- The shuffle strategy stays available, including under its force setting, when nothing downstream
-- requires bucket order. This is the shard plan of the query the guard acts on, with both settings that
-- create the promise turned off: the same plan loses the shuffle above when they are on, so a guard
-- demoting every non-final aggregation shows a different plan here.
SELECT '-- nothing promises bucket order: the shuffle scatter stays';
EXPLAIN PLAN distributed = 1 SELECT k, sum(v) FROM remote('127.0.0.{2,3}', currentDatabase(), t_shuffle_bucket_order) GROUP BY k
    SETTINGS make_distributed_plan = 1,
             distributed_aggregation_memory_efficient = 0,
             enable_memory_bound_merging_of_aggregation_results = 0,
             distributed_plan_force_shuffle_aggregation = 1;

-- The next two plans keep the `Complete` stage, with both settings pinned on, so what keeps the shuffle
-- there is the stage alone: a guard reading those settings instead of the promise shows a different plan.
-- That is also the default configuration, since both settings default to 1.
SELECT '-- complete stage: the shuffle scatter stays';
EXPLAIN PLAN SELECT k, sum(v) FROM t_shuffle_bucket_order GROUP BY k
    SETTINGS make_distributed_plan = 1,
             distributed_aggregation_memory_efficient = 1,
             enable_memory_bound_merging_of_aggregation_results = 1;
SELECT '-- complete stage under the force setting: the shuffle scatter stays';
EXPLAIN PLAN SELECT k, sum(v) FROM t_shuffle_bucket_order GROUP BY k
    SETTINGS make_distributed_plan = 1,
             distributed_aggregation_memory_efficient = 1,
             enable_memory_bound_merging_of_aggregation_results = 1,
             distributed_plan_force_shuffle_aggregation = 1;

-- The promise is set from either setting, so each of the two demotes on its own. Both plans below pin
-- both settings, and each shows the whole shape at once: no shuffle, and the partial aggregation plus
-- its merge, so a plan left undistributed is a different plan here as well.
SELECT '-- promise from distributed_aggregation_memory_efficient alone';
EXPLAIN PLAN distributed = 1 SELECT k, sum(v) FROM remote('127.0.0.{2,3}', currentDatabase(), t_shuffle_bucket_order) GROUP BY k
    SETTINGS make_distributed_plan = 1,
             distributed_aggregation_memory_efficient = 1,
             enable_memory_bound_merging_of_aggregation_results = 0,
             distributed_plan_force_shuffle_aggregation = 1;
-- The boundary of the plan above: with only memory-bound merging on, the merge over the shard output is
-- not the memory-efficient one, so nothing there reads the bucket order the demotion preserves. This is
-- the second claim that needs `actions = 1`, because `Mode:` is where a memory-efficient merge names
-- itself, and it must appear nowhere here, neither in the initiator plan nor in a shard plan.
SELECT '-- promise from enable_memory_bound_merging_of_aggregation_results alone';
EXPLAIN PLAN actions = 1, compact = 1, distributed = 1 SELECT k, sum(v) FROM remote('127.0.0.{2,3}', currentDatabase(), t_shuffle_bucket_order) GROUP BY k
    SETTINGS make_distributed_plan = 1,
             distributed_aggregation_memory_efficient = 0,
             enable_memory_bound_merging_of_aggregation_results = 1,
             distributed_plan_force_shuffle_aggregation = 1;

DROP TABLE t_shuffle_bucket_order;
