-- Tags: no-fasttest, no-old-analyzer
-- no-fasttest: the remote distributed plan needs the stateless worker configuration.
-- no-old-analyzer: `make_distributed_plan` requires the analyzer.
-- A satisfied `LIMIT` must stop the upstream stages even when nothing flows through them
-- anymore. Probe rows match the first join only in the first block, so after that block every
-- stage upstream of the `LIMIT` goes silent. The backward stop must then cross idle exchanges:
-- an idle `StreamingExchangeSink` must hear the `NoMoreDataNeeded` packet on its socket instead
-- of with the next output chunk (which never comes), and an idle `StreamingExchangeSource` must
-- notice that its output port was closed even though its peer sends no data, and forward
-- `NoMoreDataNeeded` one hop upstream. If either half is missing, the sleeping scan runs on for
-- 100+ seconds and `max_execution_time` aborts the query.

CREATE TABLE t_dp_idle_sink (x UInt64) ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 1000;
-- The matching rows must stay in the first block of the part; a parallel insert (randomized
-- `max_insert_threads`/`max_threads` in CI) would scatter them to an arbitrary depth and the
-- first probe block would no longer satisfy the `LIMIT`.
INSERT INTO t_dp_idle_sink SELECT if(number < 1000, number, number + 10000000) FROM numbers(300000) SETTINGS max_threads = 1, max_insert_threads = 1;
CREATE TABLE t_dp_idle_sink_dim (x UInt64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_dp_idle_sink_dim SELECT number FROM numbers(1000);

-- The probe scan sleeps 1s per 1000-row block. The first block satisfies the `LIMIT` through both
-- joins; rows of later blocks are shifted by 10000000, so the first join matches nothing and its
-- stage never touches its sink again. The two joins use different keys, so an exchange separates
-- their stages and the stop signal has to cross it backward.
-- Pinned: `max_block_size` and `index_granularity` keep `sleepEachRow` under its 3s per-block cap,
-- `max_threads` keeps the full scan slower than the timeout, `join_algorithm` because a sorting
-- join returns no rows until it reads all input, `min_joined_block_size_*` because squashing
-- before the join would hold the first rows back until enough blocks accumulate,
-- `max_rows_to_group_by` because the CI profile sets it and `make_distributed_plan` rejects
-- an aggregation with a row limit, and the join order because a swap makes the probe table
-- the build side, which also reads all input before the first row.
SELECT count() FROM
(
    SELECT s.x FROM
    (
        SELECT l.x FROM t_dp_idle_sink AS l
        INNER JOIN t_dp_idle_sink_dim AS r ON l.x = r.x
        WHERE NOT sleepEachRow(0.001)
    ) AS s
    INNER JOIN t_dp_idle_sink_dim AS r2 ON s.x % 1000 = r2.x
    LIMIT 1
)
SETTINGS make_distributed_plan = 1, enable_parallel_replicas = 0, distributed_plan_execute_locally = 0,
    distributed_plan_default_shuffle_join_bucket_count = 3, distributed_plan_default_reader_bucket_count = 3,
    distributed_plan_max_rows_to_broadcast = 0, distributed_plan_force_exchange_kind = 'Streaming',
    max_block_size = 1000, max_threads = 2, join_algorithm = 'hash',
    query_plan_optimize_join_order_randomize = 0, query_plan_join_swap_table = 'false',
    query_plan_optimize_join_order_algorithm = 'greedy',
    min_joined_block_size_rows = 0, min_joined_block_size_bytes = 0, max_rows_to_group_by = 0, max_execution_time = 25;

DROP TABLE t_dp_idle_sink;
DROP TABLE t_dp_idle_sink_dim;
