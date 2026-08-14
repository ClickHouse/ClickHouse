-- Tags: no-fasttest, no-old-analyzer
-- no-fasttest: the remote distributed plan needs the stateless worker configuration.
-- no-old-analyzer: `make_distributed_plan` requires the analyzer.
-- A satisfied LIMIT must stop the upstream stages of a distributed plan. The LIMIT is inside a
-- subquery, so its stage is in the middle of the plan, not at the root. The query runs twice:
-- with local in-memory exchanges and over the real streaming exchange transport.

-- The incompressible `pad` column makes each 1000-row block larger than the streaming exchange
-- sink's 128 KiB flush threshold, so every block crosses the socket at once. The outer query
-- aggregates `pad`, so column pruning cannot drop it from the exchanged streams.
CREATE TABLE t_dp_limit_stop (x UInt64, pad String) ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 1000;
INSERT INTO t_dp_limit_stop SELECT number, randomPrintableASCII(400) FROM numbers(300000);
CREATE TABLE t_dp_limit_stop_dim (x UInt64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_dp_limit_stop_dim SELECT number FROM numbers(1000);

-- The probe scan sleeps 1s per 1000-row block. The first block satisfies the LIMIT; without the
-- backward stop the scan runs on for 50+ seconds and `max_execution_time` aborts the query.
-- Pinned: `max_block_size` and `index_granularity` keep `sleepEachRow` under its 3s per-block cap,
-- `max_threads` keeps the full scan slower than the timeout, `join_algorithm` because a sorting
-- join returns no rows until it reads all input, and `min_joined_block_size_*` because squashing
-- before the join would hold the first rows back until enough blocks accumulate.
SELECT count(), sum(length(pad)) FROM
(
    SELECT l.x, l.pad FROM t_dp_limit_stop AS l
    INNER JOIN t_dp_limit_stop_dim AS r ON l.x % 1000 = r.x
    WHERE NOT sleepEachRow(0.001)
    LIMIT 1
)
SETTINGS make_distributed_plan = 1, enable_parallel_replicas = 0, distributed_plan_execute_locally = 1,
    distributed_plan_default_shuffle_join_bucket_count = 3, distributed_plan_default_reader_bucket_count = 3,
    distributed_plan_max_rows_to_broadcast = 0, distributed_plan_force_exchange_kind = 'Streaming',
    max_block_size = 1000, max_threads = 2, join_algorithm = 'hash',
    min_joined_block_size_rows = 0, min_joined_block_size_bytes = 0, max_execution_time = 25;

-- The same query on remote worker tasks: `StreamingExchangeSink` must stop over the socket too.
SELECT count(), sum(length(pad)) FROM
(
    SELECT l.x, l.pad FROM t_dp_limit_stop AS l
    INNER JOIN t_dp_limit_stop_dim AS r ON l.x % 1000 = r.x
    WHERE NOT sleepEachRow(0.001)
    LIMIT 1
)
SETTINGS make_distributed_plan = 1, enable_parallel_replicas = 0, distributed_plan_execute_locally = 0,
    distributed_plan_default_shuffle_join_bucket_count = 3, distributed_plan_default_reader_bucket_count = 3,
    distributed_plan_max_rows_to_broadcast = 0, distributed_plan_force_exchange_kind = 'Streaming',
    max_block_size = 1000, max_threads = 2, join_algorithm = 'hash',
    min_joined_block_size_rows = 0, min_joined_block_size_bytes = 0, max_execution_time = 25;

DROP TABLE t_dp_limit_stop;
DROP TABLE t_dp_limit_stop_dim;
