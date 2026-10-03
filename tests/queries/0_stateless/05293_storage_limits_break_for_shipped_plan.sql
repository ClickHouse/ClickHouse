-- With `make_distributed_plan`, a read that exceeds a read limit in 'break' mode returns a partial result.

DROP TABLE IF EXISTS t_shipped_limits_break SYNC;

-- Granules and blocks much smaller than a reader's range, so that the limit stops each reader early.
CREATE TABLE t_shipped_limits_break (a UInt64) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 8192;
INSERT INTO t_shipped_limits_break SELECT number FROM numbers(100000);

SET enable_analyzer = 1, max_block_size = 1000;
SET automatic_parallel_replicas_mode = 0, enable_parallel_replicas = 0;
SET make_distributed_plan = 1, distributed_plan_execute_locally = 1, distributed_plan_default_reader_bucket_count = 3, max_rows_to_group_by = 0;

SELECT sum(a) FROM t_shipped_limits_break;

SELECT count() > 0, sum(a) < 4999950000 FROM t_shipped_limits_break SETTINGS max_rows_to_read = 1000, read_overflow_mode = 'break';
SELECT count() > 0, sum(a) < 4999950000 FROM t_shipped_limits_break SETTINGS max_bytes_to_read = 1000, read_overflow_mode = 'break';
SELECT count() > 0, sum(a) < 4999950000 FROM t_shipped_limits_break SETTINGS max_rows_to_read_leaf = 1000, read_overflow_mode_leaf = 'break';
SELECT count() > 0, sum(a) < 4999950000 FROM t_shipped_limits_break SETTINGS max_bytes_to_read_leaf = 1000, read_overflow_mode_leaf = 'break';

DROP TABLE t_shipped_limits_break SYNC;
