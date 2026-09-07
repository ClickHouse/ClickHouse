-- Tags: no-old-analyzer
-- no-old-analyzer: make_distributed_plan requires the analyzer.
-- Tracked in https://github.com/ClickHouse/ClickHouse/issues/118534

-- A set truncated by `set_overflow_mode = 'break'` cannot be shipped to the worker tasks. `serializeSets`
-- correctly refuses it, but only when the tasks are sent, after the fallback decision was made, so the
-- query fails with `SUPPORT_IS_DISABLED` although `distributed_plan_fallback_to_local_execution` is on.
-- It must fall back to local execution instead and return the local result.

DROP TABLE IF EXISTS t_dp_set_l;
DROP TABLE IF EXISTS t_dp_set_r;
CREATE TABLE t_dp_set_l (k UInt32, v UInt64) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 64;
INSERT INTO t_dp_set_l SELECT number % 20, number FROM numbers(10000);
CREATE TABLE t_dp_set_r (w UInt64) ENGINE = MergeTree ORDER BY w;
INSERT INTO t_dp_set_r SELECT number * 10 FROM numbers(1000);

SET enable_analyzer = 1, enable_parallel_replicas = 0, max_rows_to_group_by = 0;
SET make_distributed_plan = 1, distributed_plan_execute_locally = 1, distributed_plan_max_rows_to_broadcast = 0;
SET distributed_plan_fallback_to_local_execution = 1;

SELECT 'set_overflow_mode break';
SELECT count() >= 0 FROM t_dp_set_l WHERE k IN (SELECT w FROM t_dp_set_r) SETTINGS max_rows_in_set = 10, set_overflow_mode = 'break';

DROP TABLE t_dp_set_l;
DROP TABLE t_dp_set_r;
