-- Tags: no-old-analyzer

-- Regression test: make_distributed_plan fails early and explicitly when a stage fragment contains a
-- step that cannot be serialized for remote execution (here a window function), instead of failing
-- late mid-execution with a generic "Method serialize is not implemented" error.

DROP TABLE IF EXISTS t_unserializable;

CREATE TABLE t_unserializable (a UInt32, v UInt32) ENGINE = MergeTree ORDER BY a;
INSERT INTO t_unserializable SELECT number % 10, number FROM numbers(100000);

SELECT a, sum(v) OVER (PARTITION BY a) AS w FROM t_unserializable
SETTINGS make_distributed_plan = 1, enable_parallel_replicas = 0, distributed_plan_execute_locally = 1,
    distributed_plan_max_rows_to_broadcast = 0, distributed_plan_default_reader_bucket_count = 4,
    enable_join_runtime_filters = 0, max_rows_to_group_by = 0; -- { serverError SUPPORT_IS_DISABLED }

DROP TABLE t_unserializable;
