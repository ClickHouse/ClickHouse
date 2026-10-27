-- Sizing a read to decide whether a distributed plan copies it to every node must not enforce
-- `max_rows_to_read`: the analysis counts whole granules, and later planning can make the executed
-- read smaller, here a read in key order with `LIMIT`. Without this the plan failed with
-- `TOO_MANY_ROWS` before any row was read. `EXPLAIN` without step descriptions builds the plan only:
-- describing the read would analyze it with the limit again.
SET enable_analyzer = 1;
SET enable_parallel_replicas = 0;

DROP TABLE IF EXISTS t_sizing_limit;
CREATE TABLE t_sizing_limit (i UInt64) ENGINE = MergeTree ORDER BY i SETTINGS index_granularity = 2;
INSERT INTO t_sizing_limit SELECT number FROM numbers(10);

SELECT count() > 0 FROM (
    EXPLAIN description = 0, actions = 0, indexes = 0
    SELECT i FROM t_sizing_limit ORDER BY i LIMIT 1
    SETTINGS max_rows_to_read = 2, read_overflow_mode = 'throw', make_distributed_plan = 1, enable_cascades_optimizer = 0
);

DROP TABLE t_sizing_limit;
