-- Tags: no-darwin, no-old-analyzer
-- no-darwin: distributed execution uses the streaming exchange, which is implemented only on Linux.
-- no-old-analyzer: distributed Cascades planning requires the analyzer, like the other make_distributed_plan tests.

-- `distributed_plan_workers_num` sets the node count Cascades plans for under
-- `distributed_plan_execute_locally`: one worker stays single-node, four distribute
-- (`distributed_plan_force_shuffle_aggregation` pins the shape so the check does not depend on a
-- cost tie). Without it the count comes from the configured worker cluster, so one worker would
-- still build a multi-node plan.

SET explain_query_plan_default = 'legacy';
SET enable_analyzer = 1;
SET max_rows_to_group_by = 0;

DROP TABLE IF EXISTS t_worker_count;
CREATE TABLE t_worker_count (k UInt64, g UInt16) ENGINE = MergeTree ORDER BY k
    SETTINGS auto_statistics_types = '', index_granularity = 8192, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;
-- Enough granules that a parallel read can split across nodes.
INSERT INTO t_worker_count SELECT number, number % 200 FROM numbers(1000000);

SELECT '-- one worker: local plan, no exchange';
EXPLAIN PLAN SELECT g, count() FROM t_worker_count GROUP BY g
SETTINGS enable_cascades_optimizer = 1, make_distributed_plan = 1, distributed_plan_execute_locally = 1,
    enable_parallel_replicas = 0, automatic_parallel_replicas_mode = 0, distributed_plan_force_shuffle_aggregation = 1,
    distributed_plan_workers_num = 1;

SELECT '-- four workers: distributed plan with shuffle and gather exchanges';
EXPLAIN PLAN SELECT g, count() FROM t_worker_count GROUP BY g
SETTINGS enable_cascades_optimizer = 1, make_distributed_plan = 1, distributed_plan_execute_locally = 1,
    enable_parallel_replicas = 0, automatic_parallel_replicas_mode = 0, distributed_plan_force_shuffle_aggregation = 1,
    distributed_plan_workers_num = 4;

DROP TABLE t_worker_count;
