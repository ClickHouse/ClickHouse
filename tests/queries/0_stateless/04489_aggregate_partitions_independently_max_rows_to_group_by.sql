-- Enabling `allow_aggregate_partitions_independently` by default must not break the global
-- `max_rows_to_group_by` limit. When the partition key is a function of the GROUP BY key, the
-- optimization aggregates each partition independently and skips the merge phase, which is where
-- the global limit is enforced. The optimization must therefore be disabled when
-- `max_rows_to_group_by` is set, matching the sharded-aggregation and distributed-plan paths.

DROP TABLE IF EXISTS t_apart_max_rows;

CREATE TABLE t_apart_max_rows (a UInt64)
ENGINE = MergeTree
ORDER BY a
PARTITION BY a % 16;

INSERT INTO t_apart_max_rows SELECT number FROM numbers(1000);

-- Force the optimization to be considered regardless of the runtime layout heuristics and the
-- number of available cores, so the test is deterministic.
SET allow_aggregate_partitions_independently = 1;
SET force_aggregate_partitions_independently = 1;

-- Sanity check: without a limit, the query returns all 1000 groups.
SELECT count() FROM (SELECT a FROM t_apart_max_rows GROUP BY a);

-- With a global limit below the total number of groups (1000), the query must throw. Each of the
-- 16 partitions holds ~63 distinct keys, so a per-partition limit of 100 would not trip and would
-- hide the violation - hence the global limit must be enforced and the optimization disabled.
SET max_rows_to_group_by = 100;
SET group_by_overflow_mode = 'throw';
SELECT a FROM t_apart_max_rows GROUP BY a FORMAT Null; -- { serverError TOO_MANY_ROWS }

DROP TABLE t_apart_max_rows;
