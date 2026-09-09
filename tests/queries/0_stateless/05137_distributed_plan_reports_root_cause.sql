-- Tags: no-fasttest, no-old-analyzer, no-parallel
-- no-fasttest: a remote distributed plan needs the stateless worker configuration.
-- no-old-analyzer: make_distributed_plan requires the analyzer.
-- no-parallel: enables a global failpoint that would delay other distributed-plan queries.

-- A failing task makes every task connected to it fail too (their exchange sockets close), and the
-- initiator's result reader sees the main task's socket close. The client must get the error that
-- started it, whichever report arrives first. Streaming only: a persisted producer finishes first.

DROP TABLE IF EXISTS t_root_cause;
CREATE TABLE t_root_cause (x UInt64) ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 1000;
INSERT INTO t_root_cause SELECT number FROM numbers(100000);

SET make_distributed_plan = 1, enable_parallel_replicas = 0, distributed_plan_execute_locally = 0,
    distributed_plan_force_exchange_kind = 'Streaming',
    distributed_plan_default_shuffle_join_bucket_count = 3, distributed_plan_default_reader_bucket_count = 3,
    distributed_plan_max_rows_to_broadcast = 0, max_rows_to_group_by = 0;

-- The main task fails: rowNumberInAllBlocks() keeps the expression above the gather. The initiator's
-- result reader sees the closed socket before any task status arrives.
SELECT throwIf(rowNumberInAllBlocks() > 10, 'main task fails') FROM t_root_cause FORMAT Null; -- { serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO }

-- The reading tasks fail: the aggregating tasks and the main task see their producers go away.
SELECT x, count() FROM t_root_cause WHERE NOT throwIf(x = 777, 'reading task fails') GROUP BY x FORMAT Null; -- { serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO }

-- The worker reports the failing task late, so every follow-on failure reaches the initiator first.
SYSTEM ENABLE FAILPOINT distributed_plan_delay_root_cause_report;

SELECT throwIf(rowNumberInAllBlocks() > 10, 'main task fails') FROM t_root_cause FORMAT Null; -- { serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO }
SELECT x, count() FROM t_root_cause WHERE NOT throwIf(x = 777, 'reading task fails') GROUP BY x FORMAT Null; -- { serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO }

SYSTEM DISABLE FAILPOINT distributed_plan_delay_root_cause_report;

-- Without a failure the same queries complete.
SELECT count() FROM (SELECT throwIf(rowNumberInAllBlocks() > 1000000, 'never') FROM t_root_cause);
SELECT count() FROM (SELECT x, count() FROM t_root_cause WHERE NOT throwIf(x = 7777777, 'never') GROUP BY x);

DROP TABLE t_root_cause;
