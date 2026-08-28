-- Tags: no-old-analyzer
-- no-old-analyzer: make_distributed_plan requires the analyzer.

-- Regression test: make_distributed_plan rejects an aggregation it cannot distribute correctly,
-- rather than silently running it single-node. A global GROUP BY limit (max_rows_to_group_by) is
-- such a case: each bucket aggregates only its own share of the data, so no worker can tell when
-- the global number of groups exceeds the limit.

DROP TABLE IF EXISTS t_agg_guard;

CREATE TABLE t_agg_guard (a UInt32, b UInt32, v UInt32) ENGINE = MergeTree ORDER BY (a, b);
INSERT INTO t_agg_guard SELECT number % 10, number % 7, number FROM numbers(100000);

SET distributed_plan_default_shuffle_join_bucket_count = 3, distributed_plan_default_reader_bucket_count = 3;

SET make_distributed_plan = 1, enable_parallel_replicas = 0, distributed_plan_execute_locally = 1,
    distributed_plan_max_rows_to_broadcast = 1000000000, enable_join_runtime_filters = 0;


SELECT '-- max_rows_to_group_by rejected';
SELECT a, sum(v) FROM t_agg_guard GROUP BY a
SETTINGS max_rows_to_group_by = 5, distributed_plan_fallback_to_local_execution=0; -- { serverError SUPPORT_IS_DISABLED }

DROP TABLE t_agg_guard;
