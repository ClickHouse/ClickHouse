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
-- aggregation hash table never grows past the limit and the runtime check below would not trip.
-- Pin it off.
SET optimize_aggregation_in_order = 0;

-- `max_rows_to_group_by` is checked against each aggregation lane's own hash table, not against a
-- single global one. When the data is read through several lanes (e.g. the read-in-order spread
-- gives each partition its own lane), every lane can hold fewer than `max_rows_to_group_by` distinct
-- keys while the query as a whole returns far more, so the runtime check below would not trip even
-- with the optimization disabled. Pin `max_threads = 1` so a single lane sees all the keys and the
-- limit is exercised deterministically regardless of the partition layout and the randomized reading
-- settings. This does not affect the plan checks above, which are independent of `max_threads`.
SET max_threads = 1;

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
-- through a separate port, so the aggregation falls back to a normal single pipeline that enforces
-- the limit. This plan check is deterministic regardless of `max_threads` and the other randomized
-- settings. Expected: 0.
SELECT count() FROM (EXPLAIN PLAN SELECT a FROM t_apart_max_rows GROUP BY a) WHERE explain LIKE '%separate port%';

-- And the limit is enforced at runtime: 1000 distinct keys exceed the limit of 100.
SELECT a FROM t_apart_max_rows GROUP BY a FORMAT Null; -- { serverError TOO_MANY_ROWS }

DROP TABLE t_apart_max_rows;
