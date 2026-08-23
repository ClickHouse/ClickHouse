-- Tags: no-parallel-replicas
-- Assertions on reading do not hold with parallel replicas.

-- The `parallel_hash` counterpart of `04649_spilling_hash_join_keep_left_order_opt_out`: with
-- `query_plan_read_in_order_through_spilling_join = 0` nothing pins the join, so the
-- `ConcurrentHashJoin`-backed `SpillingHashJoin` really converts its slots to `GraceHashJoin` once
-- the threshold is crossed. Ratio-based auto-spill stays enabled: the effective threshold is the
-- minimum of the ratio-derived value and the absolute `max_bytes_before_external_join`, so the
-- absolute value keeps the test deterministic while the ratio branch is exercised too.
--
-- The threshold and the right table are sized as in `04649`: `GraceHashJoin` inherits the same
-- threshold and splits buckets until each one fits in half of it, so the right table is large
-- enough for a 4 MB threshold to be crossed by an order of magnitude, and to be satisfied again
-- after a couple of dozen splits - two orders of magnitude below the bucket limit.

DROP TABLE IF EXISTS t_no_keep_order_ph_events;
DROP TABLE IF EXISTS t_no_keep_order_ph_payloads;

CREATE TABLE t_no_keep_order_ph_events (Time DateTime, Id String) ENGINE = MergeTree ORDER BY Time
SETTINGS index_granularity = 8192;
INSERT INTO t_no_keep_order_ph_events
    SELECT toDateTime('2024-01-01 00:00:00') + INTERVAL number SECOND AS Time, toString(number) AS Id
    FROM numbers(500000);

CREATE TABLE t_no_keep_order_ph_payloads (Payload String, Id String) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_no_keep_order_ph_payloads
    SELECT concat('Payload ', toString(number)) AS Payload, toString(number) AS Id
    FROM numbers(1000000) WHERE number % 4 = 0;

SET explain_query_plan_default = 'legacy';
SET optimize_read_in_order = 1, query_plan_read_in_order = 1, query_plan_read_in_order_through_join = 1;
SET min_joined_block_size_rows = 0, min_joined_block_size_bytes = 0;
SET read_in_order_use_virtual_row = 1, query_plan_optimize_join_order_limit = 1;
SET query_plan_join_swap_table = 0, join_algorithm = 'parallel_hash', max_threads = 4;
SET max_bytes_ratio_before_external_join = 0.5, max_bytes_before_external_join = 4000000;
SET grace_hash_join_initial_buckets = 1, grace_hash_join_max_buckets = 1024;
SET query_plan_read_in_order_through_spilling_join = 0;

-- The left table is no longer read in order through the join.
SELECT groupArray(trim(explain)) FROM (
    EXPLAIN PLAN actions = 1
    SELECT t_no_keep_order_ph_events.Time, t_no_keep_order_ph_events.Id, t_no_keep_order_ph_payloads.Payload
    FROM t_no_keep_order_ph_events
    INNER JOIN t_no_keep_order_ph_payloads ON t_no_keep_order_ph_events.Id = t_no_keep_order_ph_payloads.Id
    ORDER BY t_no_keep_order_ph_events.Time
    LIMIT 3
) WHERE explain LIKE '%ReadType%';

-- The results are the same either way.
SELECT t_no_keep_order_ph_events.Time, t_no_keep_order_ph_events.Id, t_no_keep_order_ph_payloads.Payload
FROM t_no_keep_order_ph_events
INNER JOIN t_no_keep_order_ph_payloads ON t_no_keep_order_ph_events.Id = t_no_keep_order_ph_payloads.Id
ORDER BY t_no_keep_order_ph_events.Time
LIMIT 3
SETTINGS log_comment = '04662_no_keep_left_order_parallel_hash';

SYSTEM FLUSH LOGS query_log;

-- Nothing pinned the join, so the threshold made it spill.
SELECT ProfileEvents['JoinSpillingHashJoinSwitchedToGraceJoin'] > 0 AS spilled
FROM system.query_log
WHERE current_database = currentDatabase()
  AND type = 'QueryFinish'
  AND log_comment = '04662_no_keep_left_order_parallel_hash'
ORDER BY event_time_microseconds DESC
LIMIT 1;

DROP TABLE t_no_keep_order_ph_events;
DROP TABLE t_no_keep_order_ph_payloads;
