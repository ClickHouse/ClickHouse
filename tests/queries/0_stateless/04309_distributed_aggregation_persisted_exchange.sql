-- Tags: no-old-analyzer

-- Regression test: distributed partial aggregation delivered through a persisted exchange must not
-- deadlock. The result reader drains final_result after the driver (the executor) has finished, so
-- the query's in-memory exchanges must outlive the executor rather than be removed on its completion.
-- A high distributed_plan_max_rows_to_broadcast keeps the aggregation on the partial+merge path.

DROP TABLE IF EXISTS t_agg_persisted;

CREATE TABLE t_agg_persisted (k UInt32, v UInt32) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_agg_persisted SELECT number % 500, number FROM numbers(50000);

SELECT k, count() AS c FROM t_agg_persisted GROUP BY k ORDER BY k LIMIT 5
SETTINGS make_distributed_plan = 1, enable_parallel_replicas = 0, distributed_plan_execute_locally = 1,
    distributed_plan_max_rows_to_broadcast = 1000000000, distributed_plan_force_exchange_kind = 'Persisted',
    enable_join_runtime_filters = 0;

DROP TABLE t_agg_persisted;
