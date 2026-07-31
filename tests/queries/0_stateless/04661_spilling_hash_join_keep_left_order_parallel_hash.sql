-- Tags: no-parallel-replicas
-- Assertions on reading do not hold with parallel replicas.

-- The `parallel_hash` counterpart of `04648_spilling_hash_join_keep_left_order`: with
-- `join_algorithm = 'parallel_hash'` the spill-capable join is a `SpillingHashJoin` backed by
-- `ConcurrentHashJoin` rather than by a single-threaded `HashJoin`, which is a separate transition
-- path inside `SpillingHashJoin`. Ratio-based auto-spill stays enabled: the effective threshold is
-- the minimum of the ratio-derived value and the absolute `max_bytes_before_external_join`, so the
-- 1-byte absolute value keeps the test deterministic while the ratio branch is exercised too.

DROP TABLE IF EXISTS t_keep_order_ph_events;
DROP TABLE IF EXISTS t_keep_order_ph_payloads;

CREATE TABLE t_keep_order_ph_events (Time DateTime, Id String) ENGINE = MergeTree ORDER BY Time
SETTINGS index_granularity = 8192;
INSERT INTO t_keep_order_ph_events
    SELECT toDateTime('2024-01-01 00:00:00') + INTERVAL number SECOND AS Time, toString(number) AS Id
    FROM numbers(500000);

CREATE TABLE t_keep_order_ph_payloads (Payload String, Id String) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_keep_order_ph_payloads
    SELECT concat('Payload ', toString(number)) AS Payload, toString(number) AS Id
    FROM numbers(30) WHERE number % 4 = 0;

SET explain_query_plan_default = 'legacy';
SET optimize_read_in_order = 1, query_plan_read_in_order = 1, query_plan_read_in_order_through_join = 1;
SET min_joined_block_size_rows = 0, min_joined_block_size_bytes = 0;
SET read_in_order_use_virtual_row = 1, query_plan_optimize_join_order_limit = 1;
SET query_plan_join_swap_table = 0, join_algorithm = 'parallel_hash', max_threads = 4;
SET max_bytes_ratio_before_external_join = 0.5, max_bytes_before_external_join = 1;

-- The left table is still read in order through the join.
SELECT groupArray(trim(explain)) FROM (
    EXPLAIN PLAN actions = 1
    SELECT t_keep_order_ph_events.Time, t_keep_order_ph_events.Id, t_keep_order_ph_payloads.Payload
    FROM t_keep_order_ph_events
    INNER JOIN t_keep_order_ph_payloads ON t_keep_order_ph_events.Id = t_keep_order_ph_payloads.Id
    ORDER BY t_keep_order_ph_events.Time
    LIMIT 3
) WHERE explain LIKE '%ReadType%';

SELECT t_keep_order_ph_events.Time, t_keep_order_ph_events.Id, t_keep_order_ph_payloads.Payload
FROM t_keep_order_ph_events
INNER JOIN t_keep_order_ph_payloads ON t_keep_order_ph_events.Id = t_keep_order_ph_payloads.Id
ORDER BY t_keep_order_ph_events.Time
LIMIT 3
SETTINGS log_comment = '04661_keep_left_order_parallel_hash';

SYSTEM FLUSH LOGS query_log;

-- Pinned to the in-memory algorithm, so it never spilled despite the 1-byte threshold.
SELECT ProfileEvents['JoinSpillingHashJoinSwitchedToGraceJoin'] AS spills
FROM system.query_log
WHERE current_database = currentDatabase()
  AND type = 'QueryFinish'
  AND log_comment = '04661_keep_left_order_parallel_hash'
ORDER BY event_time_microseconds DESC
LIMIT 1;

DROP TABLE t_keep_order_ph_events;
DROP TABLE t_keep_order_ph_payloads;
