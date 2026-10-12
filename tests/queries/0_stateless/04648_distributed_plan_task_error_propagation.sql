-- Tags: no-fasttest, no-old-analyzer, no-parallel
-- no-fasttest: a remote distributed plan needs the stateless worker configuration.
-- no-old-analyzer: make_distributed_plan requires the analyzer.
-- no-parallel: enables a global failpoint that would disrupt other distributed-plan queries.

-- A failing worker task records its exception and cancels the query. The failure can land while the
-- initiator is still dispatching the rest of the stage, so the dispatch loop's cancellation check
-- must report that recorded failure; otherwise the client would get a bare `QUERY_WAS_CANCELLED`
-- and the real reason would stay in the server log. The failpoint records a failure at dispatch time, which is
-- the ordering a fast-failing worker task produces.

DROP TABLE IF EXISTS t_task_error_propagation;

CREATE TABLE t_task_error_propagation (x UInt64) ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 1000;
INSERT INTO t_task_error_propagation SELECT number FROM numbers(100000);

SYSTEM ENABLE FAILPOINT distributed_plan_record_failure_while_starting_tasks;

SELECT x, count() FROM t_task_error_propagation GROUP BY x FORMAT Null
SETTINGS make_distributed_plan = 1, enable_parallel_replicas = 0, distributed_plan_execute_locally = 0,
    distributed_plan_default_shuffle_join_bucket_count = 3, distributed_plan_default_reader_bucket_count = 3,
    distributed_plan_max_rows_to_broadcast = 0, max_rows_to_group_by = 0; -- { serverError CANNOT_SCHEDULE_TASK }

SYSTEM DISABLE FAILPOINT distributed_plan_record_failure_while_starting_tasks;

-- Without the injected failure the same query still reports every group.
SELECT count() FROM (SELECT x, count() FROM t_task_error_propagation GROUP BY x)
SETTINGS make_distributed_plan = 1, enable_parallel_replicas = 0, distributed_plan_execute_locally = 0,
    distributed_plan_default_shuffle_join_bucket_count = 3, distributed_plan_default_reader_bucket_count = 3,
    distributed_plan_max_rows_to_broadcast = 0, max_rows_to_group_by = 0;

DROP TABLE t_task_error_propagation;
