-- Tags: no-fasttest, no-old-analyzer
-- no-fasttest: the remote distributed plan needs the stateless worker configuration.
-- no-old-analyzer: `make_distributed_plan` requires the analyzer.
-- A satisfied LIMIT must stop the upstream stages of a distributed plan. The LIMIT is inside a
-- subquery, so its stage is in the middle of the plan, not at the root. The query runs twice:
-- with local in-memory exchanges and over the real streaming exchange transport. The remote run
-- also depends on the streaming exchange sink flushing small chunks while its input is idle;
-- without that the first rows never reach the LIMIT within the timeout.

CREATE TABLE t_dp_limit_stop (x UInt64) ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 1000;
INSERT INTO t_dp_limit_stop SELECT number FROM numbers(300000);
CREATE TABLE t_dp_limit_stop_dim (x UInt64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_dp_limit_stop_dim SELECT number FROM numbers(1000);

-- The probe scan sleeps 1s per 1000-row block. The first block satisfies the LIMIT; without the
-- backward stop the scan runs on for 50+ seconds and `max_execution_time` aborts the query.
-- Pinned: `max_block_size` and `index_granularity` keep `sleepEachRow` under its 3s per-block cap,
-- `max_threads` keeps the full scan slower than the timeout, `join_algorithm` because a sorting
-- join returns no rows until it reads all input, `min_joined_block_size_*` because squashing
-- before the join would hold the first rows back until enough blocks accumulate,
-- `max_rows_to_group_by` because the CI profile sets it and `make_distributed_plan` rejects
-- an aggregation with a row limit, and the join order because a swap makes the probe table
-- the build side, which also reads all input before the first row.
SELECT count() FROM
(
    SELECT l.x FROM t_dp_limit_stop AS l
    INNER JOIN t_dp_limit_stop_dim AS r ON l.x % 1000 = r.x
    WHERE NOT sleepEachRow(0.001)
    LIMIT 1
)
SETTINGS make_distributed_plan = 1, enable_parallel_replicas = 0, distributed_plan_execute_locally = 1,
    distributed_plan_default_shuffle_join_bucket_count = 3, distributed_plan_default_reader_bucket_count = 3,
    distributed_plan_max_rows_to_broadcast = 0, distributed_plan_force_exchange_kind = 'Streaming',
    max_block_size = 1000, max_threads = 2, join_algorithm = 'hash',
    query_plan_optimize_join_order_randomize = 0, query_plan_join_swap_table = 'false',
    min_joined_block_size_rows = 0, min_joined_block_size_bytes = 0, max_rows_to_group_by = 0, max_execution_time = 25;

-- The same query on remote worker tasks: `StreamingExchangeSink` must stop over the socket too.
SELECT count() FROM
(
    SELECT l.x FROM t_dp_limit_stop AS l
    INNER JOIN t_dp_limit_stop_dim AS r ON l.x % 1000 = r.x
    WHERE NOT sleepEachRow(0.001)
    LIMIT 1
)
SETTINGS make_distributed_plan = 1, enable_parallel_replicas = 0, distributed_plan_execute_locally = 0,
    distributed_plan_default_shuffle_join_bucket_count = 3, distributed_plan_default_reader_bucket_count = 3,
    distributed_plan_max_rows_to_broadcast = 0, distributed_plan_force_exchange_kind = 'Streaming',
    max_block_size = 1000, max_threads = 2, join_algorithm = 'hash',
    query_plan_optimize_join_order_randomize = 0, query_plan_join_swap_table = 'false',
    min_joined_block_size_rows = 0, min_joined_block_size_bytes = 0, max_rows_to_group_by = 0, max_execution_time = 25;

DROP TABLE t_dp_limit_stop;
DROP TABLE t_dp_limit_stop_dim;
