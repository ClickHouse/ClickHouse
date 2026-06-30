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
-- number of available cores.
SET allow_aggregate_partitions_independently = 1;
SET force_aggregate_partitions_independently = 1;

-- Parallel replicas distribute the aggregation across replicas and enforce `max_rows_to_group_by`
-- per replica rather than globally; that path is orthogonal to this optimization, so pin it off so
-- the test stays deterministic when the CI setting randomizer enables it.
SET enable_parallel_replicas = 0;

-- `optimize_aggregation_in_order` streams each group out as soon as it is complete, so the
-- aggregation hash table never grows past the limit. Pin it off so the global limit is exercised
-- via the merge phase that the optimization skips.
SET optimize_aggregation_in_order = 0;

-- The CI test profile (`tests/config/users.d/limits.yaml`) sets `max_rows_to_group_by = 10G` as a
-- high "won't limit anything" safety net. That value is non-zero, so the optimization's guard would
-- already disable independent aggregation here and make the first check below return 0 instead of 1.
-- Reset the limit to 0 so the no-limit case is actually exercised.
SET max_rows_to_group_by = 0;

-- Without a limit the optimization is applied: each partition is read through a separate port and
-- the cross-partition merge is skipped. This makes sure the setup actually triggers the
-- optimization, so the check below is not vacuous. Expected: 1.
SELECT count() FROM (EXPLAIN PLAN SELECT a FROM t_apart_max_rows GROUP BY a) WHERE explain LIKE '%separate port%';

SET max_rows_to_group_by = 100;
SET group_by_overflow_mode = 'throw';

-- With the limit set the optimization must be disabled: the plan no longer reads each partition
-- through a separate port, so the merge phase that enforces the global limit is present. This plan
-- check is deterministic regardless of `max_threads` and the other randomized settings. Expected: 0.
SELECT count() FROM (EXPLAIN PLAN SELECT a FROM t_apart_max_rows GROUP BY a) WHERE explain LIKE '%separate port%';

-- And the global limit is enforced at runtime: 1000 distinct keys exceed the limit of 100.
SELECT a FROM t_apart_max_rows GROUP BY a FORMAT Null; -- { serverError TOO_MANY_ROWS }

DROP TABLE t_apart_max_rows;
