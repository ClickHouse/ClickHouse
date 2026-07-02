-- Tags: no-old-analyzer

-- Regression test: make_distributed_plan fails early and explicitly when a stage fragment contains a
-- step that cannot be serialized for remote execution (here a window function), instead of failing
-- late mid-execution with a generic "Method serialize is not implemented" error.

DROP TABLE IF EXISTS t_unserializable;

CREATE TABLE t_unserializable (a UInt32, v UInt32) ENGINE = MergeTree ORDER BY a;
INSERT INTO t_unserializable SELECT number % 10, number FROM numbers(100000);

SET distributed_plan_default_shuffle_join_bucket_count = 3, distributed_plan_default_reader_bucket_count = 3;

SELECT a, sum(v) OVER (PARTITION BY a) AS w FROM t_unserializable
SETTINGS make_distributed_plan = 1, enable_parallel_replicas = 0, distributed_plan_execute_locally = 1,
    distributed_plan_max_rows_to_broadcast = 0, distributed_plan_default_reader_bucket_count = 3,
    enable_join_runtime_filters = 0, max_rows_to_group_by = 0; -- { serverError SUPPORT_IS_DISABLED }

DROP TABLE t_unserializable;

-- Same fail-early behavior when a Cascades plan gets a distributed exchange over a source that cannot
-- run on a worker (here the right join side reads from `numbers`). Running it on one node would turn
-- the exchange into a no-op and drop the redistribution, so conversion must reject it instead.
DROP TABLE IF EXISTS t_local_exchange;

CREATE TABLE t_local_exchange (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_local_exchange SELECT number % 1000, number FROM numbers(10000);

SET enable_cascades_optimizer = 1;
SET param__internal_cascades_cluster_node_count = 4;
-- Cheap network and expensive local work steer Cascades toward a broadcast exchange over the numbers side.
SET param__internal_cascades_cost_config = '{"work_weight":1,"network_weight":0.01,"sequential_weight":100000,"exchange_fixed_overhead":1}';
SET param__internal_join_table_stat_hints = '{"t_local_exchange":{"cardinality":100000000,"avg_row_bytes":16,"distinct_keys":{"k":1000000}}}';

SELECT count() FROM t_local_exchange
INNER JOIN (SELECT number AS k FROM numbers(10)) AS n USING (k)
SETTINGS make_distributed_plan = 1, enable_parallel_replicas = 0, distributed_plan_execute_locally = 1,
    enable_join_runtime_filters = 0, max_rows_to_group_by = 0; -- { serverError SUPPORT_IS_DISABLED }

DROP TABLE t_local_exchange;
