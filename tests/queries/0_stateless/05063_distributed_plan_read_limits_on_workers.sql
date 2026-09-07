-- Tags: no-old-analyzer
-- no-old-analyzer: make_distributed_plan requires the analyzer.
-- Tracked in https://github.com/ClickHouse/ClickHouse/issues/118534

-- Read-time limits are attached to the read step as storage limits. A distributed-plan worker rebuilds
-- its `ReadFromMergeTree` from the serialized fragment without them (`ReadFromMergeTree::deserialize`
-- creates a `SelectQueryInfo` with no `storage_limits`), so the limits that are only checked while
-- reading (`max_bytes_to_read`, the execution speed limits) are never enforced. The queries below must
-- throw exactly like they do with `make_distributed_plan = 0`.

DROP TABLE IF EXISTS t_dp_read_limits;
CREATE TABLE t_dp_read_limits (k UInt32, v UInt64, s String) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 64;
INSERT INTO t_dp_read_limits SELECT number % 20, number, concat('s', toString(number % 7)) FROM numbers(10000);

SET enable_analyzer = 1, enable_parallel_replicas = 0, max_rows_to_group_by = 0, optimize_trivial_count_query = 0;
-- Bucket the read regardless of the table size, so the limits must be enforced on the worker fragments.
SET make_distributed_plan = 1, distributed_plan_execute_locally = 1, distributed_plan_max_rows_to_broadcast = 0;

SELECT 'max_bytes_to_read';
SELECT sum(v) FROM t_dp_read_limits SETTINGS max_bytes_to_read = 20000; -- { serverError TOO_MANY_BYTES }

SELECT 'min_execution_speed';
SELECT sum(v) FROM t_dp_read_limits SETTINGS min_execution_speed = 1000000000000, timeout_before_checking_execution_speed = 0; -- { serverError TOO_SLOW }

DROP TABLE t_dp_read_limits;
