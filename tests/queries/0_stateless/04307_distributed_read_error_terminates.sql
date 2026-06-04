-- Regression test: when a distributed read errors mid-flight, the query must terminate with that
-- error rather than hang. The local in-memory exchanges are cancelled on teardown so tasks waiting
-- for input do not block forever. max_rows_to_read trips during the distributed read while it is
-- still feeding the downstream aggregation.

DROP TABLE IF EXISTS t_distr_read_error;

CREATE TABLE t_distr_read_error (a UInt32, v UInt32) ENGINE = MergeTree ORDER BY a;
INSERT INTO t_distr_read_error SELECT number % 10, number FROM numbers(100000);

SELECT a, sum(v) FROM t_distr_read_error GROUP BY a
SETTINGS make_distributed_plan = 1, enable_parallel_replicas = 0, distributed_plan_execute_locally = 1,
    distributed_plan_max_rows_to_broadcast = 0, distributed_plan_default_reader_bucket_count = 4,
    enable_join_runtime_filters = 0, max_rows_to_group_by = 0,
    max_rows_to_read = 10, read_overflow_mode = 'throw'; -- { serverError TOO_MANY_ROWS }

DROP TABLE t_distr_read_error;
